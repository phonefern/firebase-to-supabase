import firebase_admin
from firebase_admin import credentials, firestore
import psycopg2
import os

import time
from datetime import datetime, timedelta
import pytz
import threading
from flask import Flask, render_template_string, request, redirect, url_for, jsonify
from functools import wraps
import json
import logging
import schedule

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)
BANGKOK_TZ = pytz.timezone('Asia/Bangkok')

# --- Firebase Initialization ---
def initialize_firebase():
    """Initializes Firebase from environment variables."""
    try:
        firebase_creds_json = os.environ.get('FIREBASE_CREDS')
        if not firebase_creds_json:
            raise ValueError("FIREBASE_CREDS environment variable not set. Please set it as a JSON string.")

        # Parse the JSON string for Firebase credentials
        firebase_creds_dict = json.loads(firebase_creds_json)
        
        # Important: Fix newlines in private_key if they were escaped during environment variable setup
        if 'private_key' in firebase_creds_dict and isinstance(firebase_creds_dict['private_key'], str):
            firebase_creds_dict['private_key'] = firebase_creds_dict['private_key'].replace('\\n', '\n')
            
        cred = credentials.Certificate(firebase_creds_dict)
        firebase_admin.initialize_app(cred)
        logger.info("Firebase initialized successfully.")
        return firebase_admin.firestore.client()
    except Exception as e:
        logger.critical(f"Firebase initialization failed: {str(e)}. Exiting.")
        # In a real app, you might want to gracefully exit or prevent startup
        raise

# Initialize Firebase (this will raise an error if env var is missing/malformed)
fs_db = initialize_firebase()

# --- Supabase (PostgreSQL) Connection ---
def get_db_connection():
    """Establishes a connection to the Supabase PostgreSQL database."""
    conn = None
    try:
        conn = psycopg2.connect(
            host=os.getenv("SUPABASE_HOST", "aws-0-ap-southeast-1.pooler.supabase.com"),
            port=os.getenv("SUPABASE_PORT", "6543"),
            database=os.getenv("SUPABASE_DB", "postgres"),
            user=os.getenv("SUPABASE_USER", "postgres.mieiwzfhohifeprjtnek"),
            password=os.getenv("SUPABASE_PASSWORD", "root"),
            connect_timeout=10 # Timeout for connection attempt
        )
        logger.info("Connected to Supabase successfully.")
        return conn
    except Exception as e:
        logger.error(f"Supabase connection failed: {str(e)}")
        # Propagate the exception so the caller knows the connection failed
        raise

# --- Global State Variables ---
# last_execution will now be managed persistently in Supabase, but stored here for immediate UI update.
last_execution = None 
is_running = False # Flag to prevent concurrent migration runs
execution_history = [] # In-memory history for recent runs, will reset on app restart

# --- Constants ---
FIELDS_TO_COUNT = [
    "balance", "dualTap", "dualTapRight", "gaitWalk",
    "pinchToSize", "pinchToSizeRight", "questionnaire",
    "tremorPostural", "tremorResting", "voiceAhh"
]

# --- Helper Functions for Time and Parsing ---
def now_thai():
    """Returns the current datetime in the Bangkok timezone."""
    return datetime.now(BANGKOK_TZ)

def format_dt(dt):
    """Formats a datetime object to a string in Bangkok timezone, or 'ยังไม่เคย' if None."""
    if not dt:
        return "ยังไม่เคย"
    # Ensure datetime is in Bangkok timezone before formatting
    return dt.astimezone(BANGKOK_TZ).strftime("%Y-%m-%d %H:%M:%S")

def parse_ts(val):
    """Parses various timestamp formats (datetime, int, str) into a Bangkok-aware datetime object."""
    try:
        if isinstance(val, datetime):
            # If naive, assume UTC and localize, then convert to Bangkok. If aware, just convert.
            if val.tzinfo is None:
                return pytz.utc.localize(val).astimezone(BANGKOK_TZ)
            return val.astimezone(BANGKOK_TZ)
        if isinstance(val, int):
            # Assume Unix timestamp in milliseconds
            return datetime.fromtimestamp(val / 1000, BANGKOK_TZ)
        if isinstance(val, str):
            # Parse ISO 8601 string, handling 'Z' (UTC) and converting to Bangkok
            return datetime.fromisoformat(val.replace("Z", "+00:00")).astimezone(BANGKOK_TZ)
        return None
    except Exception as e:
        logger.warning(f"Failed to parse timestamp value '{val}' (type: {type(val)}): {str(e)}")
        return None

# --- Persistent State Management in Supabase ---
def get_last_execution_from_db():
    """Retrieves the last successful execution time from the Supabase `migration_status` table."""
    conn = None
    cur = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT last_execution_time FROM migration_status WHERE id = 'current_status'")
        result = cur.fetchone()
        if result and result[0]:
            # Convert fetched UTC timestamp to Bangkok timezone
            return result[0].astimezone(BANGKOK_TZ)
        logger.info("No last execution time found in DB, assuming first run.")
        return None
    except Exception as e:
        logger.error(f"Error fetching last_execution_time from DB: {str(e)}")
        return None
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

def save_last_execution_to_db(timestamp):
    """Saves the given timestamp as the last successful execution time to Supabase."""
    conn = None
    cur = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        # Always store in UTC for database consistency
        cur.execute("""
            INSERT INTO migration_status (id, last_execution_time)
            VALUES ('current_status', %s)
            ON CONFLICT (id) DO UPDATE SET last_execution_time = EXCLUDED.last_execution_time, updated_at = NOW();
        """, (timestamp.astimezone(pytz.UTC),))
        conn.commit()
        logger.info(f"Saved last_execution_time to DB: {format_dt(timestamp)}")
    except Exception as e:
        logger.error(f"Error saving last_execution_time to DB: {str(e)}")
        if conn:
            conn.rollback() # Rollback on error to ensure data consistency
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

# --- Decorator for Background Tasks ---
def background_task(fn):
    """Decorator to manage task running status and logging."""
    @wraps(fn)
    def wrapper(*args, **kwargs):
        global is_running
        if is_running:
            logger.warning(f"Attempted to start {fn.__name__} but another task is already running.")
            return {"status": "error", "message": "Task already running"}
        
        is_running = True
        result = {"status": "error", "message": "Unknown error"} # Default error
        try:
            logger.info(f"Starting background task: {fn.__name__}")
            result = fn(*args, **kwargs)
            logger.info(f"Completed background task: {fn.__name__} (Status: {result.get('status')})")
        except Exception as e:
            logger.error(f"Critical error in background task {fn.__name__}: {str(e)}", exc_info=True)
            result = {"status": "error", "message": f"Critical task error: {str(e)}"}
        finally:
            is_running = False
        return result
    return wrapper

# --- Main Migration Logic ---
@background_task
def migrate_all(full_sync=False):
    """
    Orchestrates the full or incremental data migration.
    Fetches `since` from DB and saves `last_execution` to DB after completion.
    """
    global last_execution # Reference global for immediate UI update

    current_start_time = now_thai()
    
    # Determine the 'since' timestamp for incremental sync
    # If full_sync is True, or if no previous execution time is found in DB,
    # 'since' will be None, triggering a full sync in child functions.
    since_timestamp = None
    if not full_sync:
        since_timestamp = get_last_execution_from_db() # Get from persistent storage
        if since_timestamp:
            logger.info(f"Performing incremental sync since: {format_dt(since_timestamp)}")
        else:
            logger.info("No previous execution time found, performing full sync.")
    else:
        logger.info("Performing full sync as requested.")
    
    user_count = 0
    summary_count = 0
    migration_status = "success"
    error_message = ""

    try:
        # Migrate users
        user_count = migrate_users(since=since_timestamp)
        logger.info(f"User migration completed. Processed {user_count} users.")

        # Migrate summaries
        # For summaries, we need to re-evaluate users whose records might have changed.
        # If migrate_users already filtered users by lastUpdate,
        # then migrate_summaries should re-process summaries for those users
        # by looking at ALL their records, not just records updated since 'since_timestamp'.
        # However, to be efficient, we only process summaries for users that were *just* migrated
        # or all users if it's a full sync.
        summary_count = migrate_summaries(since=since_timestamp) # Keep 'since' for consistency in how users are filtered
        logger.info(f"Summary migration completed. Processed {summary_count} summaries.")

    except Exception as e:
        migration_status = "failed"
        error_message = str(e)
        logger.error(f"Migration process failed: {str(e)}", exc_info=True)
    finally:
        # Always save the current time as last_execution, regardless of success or failure.
        # This prevents repeated full syncs if a partial failure occurs.
        save_last_execution_to_db(now_thai())
        last_execution = now_thai() # Update global for immediate UI refresh

        execution_history.append({
            "type": "full" if full_sync else "incremental",
            "start_time": current_start_time,
            "end_time": last_execution,
            "user_count": user_count,
            "summary_count": summary_count,
            "status": migration_status if migration_status == "success" else f"failed: {error_message}"
        })
    
    return {
        "status": migration_status,
        "user_count": user_count,
        "summary_count": summary_count,
        "message": error_message
    }

def migrate_users(since=None):
    """
    Migrates user data from Firestore to Supabase.
    Performs an incremental sync if 'since' timestamp is provided.
    """
    count = 0
    conn = None
    cur = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        users_ref = fs_db.collection("users")
        if since:
            # Firestore requires UTC for timestamp queries
            users_query = users_ref.where("lastUpdate", ">=", since.astimezone(pytz.UTC))
            logger.info(f"Querying Firestore for users updated since {since.astimezone(pytz.UTC)}")
        else:
            users_query = users_ref
            logger.info("Querying all users from Firestore (full sync).")
        
        users_stream = users_query.stream()
        
        for doc in users_stream:
            user_id = doc.id
            data = doc.to_dict()
            try:
                # Upsert user data with all fields.
                # Ensure all fields are handled, even if None.
                cur.execute("""
                    INSERT INTO users (
                        id, age, bod, educationStatus, email, emorument, ethnicity,
                        firstName, gender, idCardAddress, irb, isStaff, lastName,
                        lastUpdate, liveAddress, maritalStatus, occupation, pdpa,
                        perfixName, phoneNumber, remind, thaiId, timestamp
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s
                    )
                    ON CONFLICT (id) DO UPDATE SET
                        age = EXCLUDED.age,
                        bod = EXCLUDED.bod,
                        educationStatus = EXCLUDED.educationStatus,
                        email = EXCLUDED.email,
                        emorument = EXCLUDED.emorument,
                        ethnicity = EXCLUDED.ethnicity,
                        firstName = EXCLUDED.firstName,
                        gender = EXCLUDED.gender,
                        idCardAddress = EXCLUDED.idCardAddress,
                        irb = EXCLUDED.irb,
                        isStaff = EXCLUDED.isStaff,
                        lastName = EXCLUDED.lastName,
                        lastUpdate = EXCLUDED.lastUpdate,
                        liveAddress = EXCLUDED.liveAddress,
                        maritalStatus = EXCLUDED.maritalStatus,
                        occupation = EXCLUDED.occupation,
                        pdpa = EXCLUDED.pdpa,
                        perfixName = EXCLUDED.perfixName,
                        phoneNumber = EXCLUDED.phoneNumber,
                        remind = EXCLUDED.remind,
                        thaiId = EXCLUDED.thaiId,
                        timestamp = EXCLUDED.timestamp;
                """, (
                    user_id,
                    data.get("age"),
                    parse_ts(data.get("bod")),
                    data.get("educationStatus"),
                    data.get("email"),
                    data.get("emorument"),
                    data.get("ethnicity"),
                    data.get("firstName"),
                    data.get("gender"),
                    data.get("idCardAddress"),
                    data.get("irb"),
                    data.get("isStaff"),
                    data.get("lastName"),
                    parse_ts(data.get("lastUpdate")),
                    data.get("liveAddress"),
                    data.get("maritalStatus"),
                    data.get("occupation"),
                    data.get("pdpa"),
                    data.get("perfixName"),
                    data.get("phoneNumber"),
                    parse_ts(data.get("remind")),
                    data.get("thaiId"),
                    parse_ts(data.get("timestamp")),
                ))
                count += 1
                if count % 100 == 0:
                    logger.info(f"User migration: Processed {count} users.")
            except Exception as e:
                logger.error(f"Failed to upsert user {user_id}: {str(e)}", exc_info=True)
                # Continue processing other users even if one fails
        
        conn.commit()
        logger.info(f"User migration completed. Successfully processed {count} users.")
        return count
            
    except Exception as e:
        logger.error(f"Overall user migration failed: {str(e)}", exc_info=True)
        if conn:
            conn.rollback() # Rollback all changes if a critical error occurs
        raise # Re-raise to signal failure to the caller
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

def migrate_summaries(since=None):
    """
    Migrates user record summaries from Firestore to Supabase.
    It will iterate through users. If 'since' is provided, it only processes
    users whose 'lastUpdate' is recent. For each selected user, it fetches ALL
    their records to compute a comprehensive summary.
    """
    count = 0 # Renamed from processed_count to count for consistency
    conn = None
    cur = None # Renamed from cursor to cur for consistency
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        users_ref = fs_db.collection("users")
        if since:
            users_query = users_ref.where("lastUpdate", ">=", since.astimezone(pytz.UTC))
            logger.info(f"Querying Firestore for users (for summaries) updated since {since.astimezone(pytz.UTC)}")
        else:
            users_query = users_ref
            logger.info("Querying all users from Firestore for summaries (full sync).")

        users_stream = users_query.stream()
        
        for user_doc in users_stream:
            user_id = user_doc.id
            
            records_ref = fs_db.collection("users").document(user_id).collection("records")
            all_user_records = list(records_ref.stream())
            
            if not all_user_records:
                logger.debug(f"User {user_id} has no records, skipping summary.")
                continue
            
            # Group by recorder (as provided in snippet)
            grouped = {}
            for rec in all_user_records: # Renamed from records to all_user_records
                data = rec.to_dict()
                recorder = data.get("recorder") or "unknown"
                last_update = parse_ts(data.get("lastUpdate")) or datetime.min
                if recorder not in grouped:
                    grouped[recorder] = []
                grouped[recorder].append((last_update, rec.id, data))
            
            # Process each recorder's records
            for recorder, rec_list in grouped.items():
                try:
                    rec_list.sort(key=lambda x: x[0], reverse=True)
                    latest_update_ts, latest_rec_id, latest_data = rec_list[0]
                    
                    version = latest_data.get("version")
                    prediction = latest_data.get("prediction")
                    
                    # Handling prediction_risk: Send as TEXT/VARCHAR to Supabase
                    # This assumes you have changed the `prediction_risk` column in Supabase to TEXT.
                    risk_val = None
                    if isinstance(prediction, dict):
                        # Get the raw value, regardless of type
                        risk_val = prediction.get("risk")
                    
                    # Convert to string if it's not None, otherwise keep None for SQL NULL
                    prediction_risk_for_db = str(risk_val) if risk_val is not None else None
                    
                    # Count fields with data across ALL records for this recorder
                    counts = {field: 0 for field in FIELDS_TO_COUNT}
                    for _, _, data in rec_list:
                        for field in FIELDS_TO_COUNT:
                            if data.get(field) is not None:
                                counts[field] += 1
                    
                    # Upsert summary into Supabase
                    cur.execute(f"""
                        INSERT INTO user_record_summary (
                            user_id, recorder, record_id, version, last_update,
                            prediction_risk, record_count, {', '.join(FIELDS_TO_COUNT)}
                        ) VALUES (
                            %s, %s, %s, %s, %s,
                            %s, %s, {', '.join(['%s'] * len(FIELDS_TO_COUNT))}
                        )
                        ON CONFLICT (user_id, recorder) DO UPDATE SET
                            record_id = EXCLUDED.record_id,
                            version = EXCLUDED.version,
                            last_update = EXCLUDED.last_update,
                            prediction_risk = EXCLUDED.prediction_risk,
                            record_count = EXCLUDED.record_count,
                            {', '.join([f"{f} = EXCLUDED.{f}" for f in FIELDS_TO_COUNT])},
                            updated_at = NOW();
                    """, [
                        user_id,
                        recorder,
                        latest_rec_id,
                        version,
                        latest_update_ts, # Use the parsed datetime object
                        prediction_risk_for_db, # Use the processed string value
                        len(rec_list) # Total number of records for this user/recorder
                    ] + [counts[f] for f in FIELDS_TO_COUNT])
                    
                    count += 1
                    if count % 50 == 0:
                        logger.info(f"Summary migration: Processed {count} summaries.")
                    
                except Exception as e:
                    # Log error, then rollback the *current transaction* to clear the aborted state
                    logger.error(f"Failed to process summary for user {user_id}, recorder {recorder}: {str(e)}", exc_info=True)
                    # This rollback is crucial if an error occurs mid-transaction to allow subsequent
                    # user summaries to be processed within the same overall function call.
                    # Note: psycopg2's default isolation level (READ COMMITTED) and how it handles
                    # statement-level errors might mean it already auto-rolls back the sub-transaction.
                    # However, an explicit rollback ensures a clean state.
                    if conn:
                        conn.rollback() # Rollback to clear aborted state
                    # Re-establish cursor if needed, or simply continue.
                    # In this loop, if an error happens, we just log and move to next recorder.
            
        conn.commit() # Final commit of all successful operations
        logger.info(f"Summary migration completed. Successfully processed {count} summaries.")
        return count
            
    except Exception as e:
        logger.error(f"Overall summary migration failed: {str(e)}", exc_info=True)
        if conn:
            conn.rollback() # Rollback all changes if a critical error occurs
        raise # Re-raise to signal failure to the caller to `migrate_all`
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

# --- Flask Routes ---
@app.route("/")
def index():
    """Renders the main migration console dashboard."""
    # `last_execution` global is updated by migrate_all, which gets its true value from DB.
    # `schedule.next_run()` depends on the in-process scheduler, which might not be active on Render.
    next_scheduled_run = schedule.next_run() # This will be None if scheduler thread is not running
    
    return render_template_string("""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Firebase-Supabase Migration Console (UTC+7)</title>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <style>
                body { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; max-width: 800px; margin: 0 auto; padding: 20px; background-color: #f8f8f8; color: #333; line-height: 1.6;}
                .card { background: #ffffff; padding: 25px; margin: 20px 0; border-radius: 8px; box-shadow: 0 4px 8px rgba(0,0,0,0.1); }
                h1, h2 { color: #2c3e50; margin-top: 0;}
                .status-info p { margin: 5px 0; }
                .status-info strong { color: #007bff; }
                .thai-time { font-weight: bold; color: #0066cc; }
                .button-group { margin-top: 20px; text-align: center; }
                .button { 
                    background: #28a745; /* Success green */
                    color: white; 
                    border: none; 
                    padding: 12px 20px; 
                    margin: 8px; 
                    border-radius: 6px; 
                    cursor: pointer; 
                    text-decoration: none; 
                    display: inline-block;
                    font-size: 1rem;
                    transition: background-color 0.3s ease, transform 0.2s ease;
                }
                .button:hover { 
                    background-color: #218838; 
                    transform: translateY(-2px);
                }
                .button.full { 
                    background: #dc3545; /* Danger red */
                }
                .button.full:hover { 
                    background-color: #c82333;
                }
                .history-section { margin-top: 30px; }
                .history-item { 
                    background: #f0f0f0; 
                    padding: 15px; 
                    margin-bottom: 10px; 
                    border-radius: 5px; 
                    border-left: 5px solid;
                }
                .history-item.success { border-color: #28a745; }
                .history-item.failed { border-color: #dc3545; }
                .history-item p { margin: 3px 0; }
                .history-item strong { color: #495057; }
                .success-text { color: #28a745; font-weight: bold; }
                .failed-text { color: #dc3545; font-weight: bold; }
            </style>
        </head>
        <body>
            <div class="card">
                <h1>Firebase to Supabase Migration (ไทยเวลา UTC+7)</h1>
                <div class="status-info">
                    <p>สถานะ: <strong>{{ 'กำลังทำงาน' if is_running else 'พร้อมทำงาน' }}</strong></p>
                    <p>เวลาปัจจุบัน: <span class="thai-time">{{ now }}</span></p>
                    <p>การดำเนินการล่าสุด: <span class="thai-time">{{ last }}</span></p>
                    <p>การดำเนินการถัดไป (ในแอป): <span class="thai-time">{{ next }}</span></p>
                    <p><em>(การตั้งเวลาอัตโนมัติบน Render ต้องใช้ Cron Jobs ภายนอก)</em></p>
                </div>
                
                <form method="post" action="/run" class="button-group">
                    <button class="button" type="submit" name="type" value="incremental">ซิงค์เฉพาะข้อมูลใหม่</button>
                    <button class="button full" type="submit" name="type" value="full">ซิงค์ข้อมูลทั้งหมด</button>
                </form>
            </div>
            
            <div class="card history-section">
                <h2>ประวัติการทำงานล่าสุด</h2>
                {% if history %}
                    {% for entry in history %}
                    <div class="history-item {{ 'success' if 'success' in entry.status else 'failed' }}">
                        <p><strong>{{ 'ซิงค์ทั้งหมด' if entry.type == 'full' else 'ซิงค์เฉพาะใหม่' }}</strong></p>
                        <p>เริ่มต้น: <span class="thai-time">{{ entry.start_time }}</span></p>
                        <p>สิ้นสุด: <span class="thai-time">{{ entry.end_time }}</span></p>
                        <p>สถานะ: <span class="{{ 'success-text' if 'success' in entry.status else 'failed-text' }}">
                            {{ entry.status }}
                        </span></p>
                        <p>ผู้ใช้ที่ประมวลผล: {{ entry.user_count }}, สรุปข้อมูลที่ประมวลผล: {{ entry.summary_count }}</p>
                    </div>
                    {% endfor %}
                {% else %}
                    <p>ยังไม่มีประวัติการทำงาน</p>
                {% endif %}
            </div>
        </body>
        </html>
    """, 
    now=format_dt(now_thai()),
    last=format_dt(last_execution),
    next=format_dt(next_scheduled_run) if next_scheduled_run else "ไม่ได้กำหนด/ปิดใช้งาน",
    is_running=is_running,
    history=execution_history[::-1] # Show most recent first
    )

@app.route("/run", methods=["POST"])
def trigger_migration():
    """Endpoint to manually trigger a migration."""
    migration_type = request.form.get("type", "incremental")
    full_sync = (migration_type == "full")
    logger.info(f"Manual trigger for {migration_type} sync received.")
    
    # Start the migration in a separate thread to avoid blocking the Flask request
    thread = threading.Thread(target=lambda: migrate_all(full_sync=full_sync))
    thread.daemon = True # Allow the main program to exit even if this thread is running
    thread.start()
    
    return redirect(url_for("index"))

@app.route("/api/status")
def api_status():
    """API endpoint to get the current status of the migration."""
    next_scheduled_run = schedule.next_run()
    return jsonify({
        "status": "running" if is_running else "idle",
        "timezone": "Asia/Bangkok (UTC+7)",
        "current_time": format_dt(now_thai()),
        "last_execution": format_dt(last_execution),
        "next_scheduled_in_app": format_dt(next_scheduled_run) if next_scheduled_run else None,
        "execution_history": [
            {
                "type": h["type"],
                "start_time": format_dt(h["start_time"]),
                "end_time": format_dt(h["end_time"]),
                "user_count": h["user_count"],
                "summary_count": h["summary_count"],
                "status": h["status"]
            } for h in execution_history[::-1] # Most recent first for API
        ]
    })

# --- Scheduler Setup ---
def schedule_jobs():
    """Configures and runs the in-app scheduler."""
    # Weekly full sync: Sunday 00:00 Bangkok time
    schedule.every().sunday.at("00:00", BANGKOK_TZ).do(lambda: migrate_all(full_sync=True)).tag("weekly")
    
    # Daily incremental sync: Every day at 03:00 Bangkok time
    schedule.every().day.at("03:00", BANGKOK_TZ).do(lambda: migrate_all(full_sync=False)).tag("daily")
    
    logger.info("In-app scheduler started. Checking for pending jobs every 30 seconds.")
    while True:
        schedule.run_pending()
        time.sleep(30) # Check every 30 seconds

# --- Application Entry Point ---
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 7860))
    
    # Initialize last_execution global variable by loading from DB at startup
    last_execution = get_last_execution_from_db()

    # Determine if running in a Render environment
    # Render sets a 'RENDER' environment variable for deployed services.
    if os.getenv('RENDER'):
        logger.info("Detected RENDER environment. Running Flask app without in-app scheduler thread.")
        # On Render, rely on external Cron Jobs for scheduling.
        app.run(host="0.0.0.0", port=port)
    else:
        logger.info("Running in local development environment. Starting Flask app with in-app scheduler.")
        # Start the scheduler in a separate daemon thread for local development
        threading.Thread(target=schedule_jobs, daemon=True).start()
        app.run(host="0.0.0.0", port=port)
