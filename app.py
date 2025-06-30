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
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
BANGKOK_TZ = pytz.timezone('Asia/Bangkok')

def initialize_firebase():
    try:
        firebase_creds_json = os.environ.get('FIREBASE_CREDS')
        if not firebase_creds_json:
            raise ValueError("FIREBASE_CREDS environment variable not set")

        # Parse the JSON with proper handling of escaped characters
        firebase_creds_dict = json.loads(firebase_creds_json)
        
        # Fix newlines in private key if they were escaped
        if '\\n' in firebase_creds_dict.get('private_key', ''):
            firebase_creds_dict['private_key'] = firebase_creds_dict['private_key'].replace('\\n', '\n')
        
        cred = credentials.Certificate(firebase_creds_dict)
        firebase_admin.initialize_app(cred)
        print("Firebase initialized successfully")
        return firebase_admin.firestore.client()
    except Exception as e:
        print(f"Firebase initialization failed: {str(e)}")
        raise

# Initialize Firebase
fs_db = initialize_firebase()

# Supabase Connection
def get_db_connection():
    try:
        conn = psycopg2.connect(
            host=os.getenv("SUPABASE_HOST", "aws-0-ap-southeast-1.pooler.supabase.com"),
            port=os.getenv("SUPABASE_PORT", "6543"),
            database=os.getenv("SUPABASE_DB", "postgres"),
            user=os.getenv("SUPABASE_USER", "postgres.mieiwzfhohifeprjtnek"),
            password=os.getenv("SUPABASE_PASSWORD", "root"),
            connect_timeout=10
        )
        logger.info("Connected to Supabase successfully")
        return conn
    except Exception as e:
        logger.error(f"Supabase connection failed: {str(e)}")
        raise

# Globals
last_execution = None
is_running = False
execution_history = []

FIELDS_TO_COUNT = [
    "balance", "dualTap", "dualTapRight", "gaitWalk",
    "pinchToSize", "pinchToSizeRight", "questionnaire",
    "tremorPostural", "tremorResting", "voiceAhh"
]

def now_thai():
    return datetime.now(BANGKOK_TZ)

def format_dt(dt):
    if not dt:
        return "ยังไม่เคย"
    return dt.astimezone(BANGKOK_TZ).strftime("%Y-%m-%d %H:%M:%S")

def parse_ts(val):
    try:
        if isinstance(val, datetime):
            return val.astimezone(BANGKOK_TZ)
        if isinstance(val, int):
            return datetime.fromtimestamp(val / 1000, BANGKOK_TZ)
        if isinstance(val, str):
            return datetime.fromisoformat(val.replace("Z", "+00:00")).astimezone(BANGKOK_TZ)
        return None
    except Exception as e:
        logger.warning(f"Failed to parse timestamp {val}: {str(e)}")
        return None

def background_task(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        global is_running
        if is_running:
            logger.warning("Task already running, skipping")
            return {"status": "error", "message": "Task already running"}
        
        is_running = True
        try:
            logger.info(f"Starting {fn.__name__} task")
            result = fn(*args, **kwargs)
            logger.info(f"Completed {fn.__name__} task successfully")
            return result
        except Exception as e:
            logger.error(f"Error in {fn.__name__} task: {str(e)}")
            return {"status": "error", "message": str(e)}
        finally:
            is_running = False
    return wrapper

@background_task
def migrate_all(full_sync=False):
    global last_execution
    start_time = now_thai()
    since = None if full_sync else last_execution
    user_count = 0
    summary_count = 0

    try:
        logger.info(f"Starting {'full' if full_sync else 'incremental'} migration")
        user_count = migrate_users(since)
        summary_count = migrate_summaries(since)
        status = "success"
        logger.info(f"Migration completed: {user_count} users, {summary_count} summaries")
    except Exception as e:
        status = f"failed: {e}"
        logger.error(f"Migration failed: {str(e)}")

    last_execution = now_thai()
    execution_history.append({
        "type": "full" if full_sync else "incremental",
        "start_time": start_time,
        "end_time": last_execution,
        "user_count": user_count,
        "summary_count": summary_count,
        "status": status
    })
    
    return {
        "status": "success",
        "user_count": user_count,
        "summary_count": summary_count
    }

@background_task
def migrate_users(since=None):
    print(f"\nStarting user migration from Firestore → Supabase (since: {since})")
    conn = get_db_connection()
    cursor = conn.cursor()
    
    users_ref = db.collection("users")
    users_query = users_ref
    
    # Only get users updated since last migration if specified
    if since:
        # Convert Thai time to UTC for Firestore query
        since_utc = since.astimezone(pytz.UTC)
        users_query = users_ref.where("lastUpdate", ">=", since_utc)
    
    users = users_query.stream()
    migrated_count = 0
    
    for doc in users:
        data = doc.to_dict()
        user_id = doc.id

        try:
            cursor.execute("""
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
            migrated_count += 1
        except Exception as e:
            print(f"❌ Error updating user {user_id}: {e}")

    conn.commit()
    cursor.close()
    conn.close()
    print(f"✅ User migration complete. {migrated_count} users processed.")
    return migrated_count

@background_task
def migrate_summaries(since=None):
    print(f"\nProcessing user summaries (since: {since})")
    conn = get_db_connection()
    cursor = conn.cursor()
    users = db.collection("users").stream()
    processed_count = 0

    for user_doc in users:
        user_id = user_doc.id
        records_ref = db.collection("users").document(user_id).collection("records")
        
        # Only get records updated since last migration if specified
        if since:
            since_utc = since.astimezone(pytz.UTC)
            records_query = records_ref.where("lastUpdate", ">=", since_utc)
        else:
            records_query = records_ref
            
        records = list(records_query.stream())

        # Group by recorder
        grouped = {}
        for rec in records:
            data = rec.to_dict()
            recorder = data.get("recorder") or "unknown"
            last_update = parse_ts(data.get("lastUpdate")) or datetime.min

            if recorder not in grouped:
                grouped[recorder] = []
            grouped[recorder].append((last_update, rec.id, data))

        for recorder, rec_list in grouped.items():
            rec_list.sort(key=lambda x: x[0], reverse=True)
            latest_update, latest_rec_id, latest_data = rec_list[0]
            prediction_risk = None
            version = latest_data.get("version")

            prediction = latest_data.get("prediction")
            if isinstance(prediction, dict):
                risk_val = prediction.get("risk")
                if isinstance(risk_val, bool):
                    prediction_risk = risk_val

            counts = {field: 0 for field in FIELDS_TO_COUNT}
            for _, _, data in rec_list:
                for field in FIELDS_TO_COUNT:
                    if data.get(field) is not None:
                        counts[field] += 1

            try:
                cursor.execute(f"""
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
                        {', '.join([f"{f} = EXCLUDED.{f}" for f in FIELDS_TO_COUNT])};
                """, [
                    user_id,
                    recorder,
                    latest_rec_id,
                    version,
                    latest_update,
                    prediction_risk,
                    len(rec_list)
                ] + [counts[f] for f in FIELDS_TO_COUNT])
                processed_count += 1
            except Exception as e:
                print(f"❌ Error for user {user_id}, recorder {recorder}: {e}")

    conn.commit()
    cursor.close()
    conn.close()
    print(f"✅ User summaries complete. {processed_count} records processed.")
    return processed_count

@app.route("/")
def index():
    next_run = schedule.next_run()
    return render_template_string("""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Migration Console</title>
            <style>
                body { font-family: Arial, sans-serif; max-width: 800px; margin: 0 auto; padding: 20px; }
                h1 { color: #333; }
                .status { padding: 10px; background: #f5f5f5; border-radius: 5px; }
                .button { 
                    background: #4CAF50; color: white; border: none; 
                    padding: 10px 15px; margin: 5px; border-radius: 5px; 
                    cursor: pointer; text-decoration: none; display: inline-block;
                }
                .button.full { background: #f44336; }
                .history { margin-top: 20px; border-top: 1px solid #ddd; }
                .history-item { padding: 8px; border-bottom: 1px solid #eee; }
            </style>
        </head>
        <body>
            <h1>Migration Console</h1>
            <div class="status">
                <p>สถานะ: <strong>{{ 'กำลังทำงาน' if is_running else 'พร้อมทำงาน' }}</strong></p>
                <p>เวลาปัจจุบัน: {{ now }}</p>
                <p>ล่าสุด: {{ last }}</p>
                <p>รอบถัดไป: {{ next }}</p>
            </div>
            
            <form method="post" action="/run">
                <button class="button" name="type" value="incremental">Sync เฉพาะใหม่</button>
                <button class="button full" name="type" value="full">Sync ทั้งหมด</button>
            </form>
            
            <div class="history">
                <h2>ประวัติ</h2>
                {% for e in history %}
                <div class="history-item">
                    <strong>{{ e.start_time.strftime('%Y-%m-%d %H:%M:%S') }}</strong><br>
                    ประเภท: {{ 'ทั้งหมด' if e.type == 'full' else 'เฉพาะใหม่' }}<br>
                    สถานะ: {{ e.status }}<br>
                    ผู้ใช้: {{ e.user_count }}, สรุป: {{ e.summary_count }}
                </div>
                {% endfor %}
            </div>
        </body>
        </html>
    """, 
    now=format_dt(now_thai()),
    last=format_dt(last_execution),
    next=format_dt(next_run) if next_run else "ไม่ได้ตั้งเวลา",
    is_running=is_running,
    history=execution_history[-5:][::-1]  # Show most recent first
    )

@app.route("/run", methods=["POST"])
def run_now():
    full_sync = request.form.get("type") == "full"
    thread = threading.Thread(target=lambda: migrate_all(full_sync=full_sync))
    thread.start()
    return redirect(url_for("index"))

@app.route("/api/status")
def api_status():
    return jsonify({
        "running": is_running,
        "last_execution": format_dt(last_execution),
        "next_run": format_dt(schedule.next_run()) if schedule.next_run() else None,
        "history": [{
            "type": e["type"],
            "start_time": format_dt(e["start_time"]),
            "end_time": format_dt(e["end_time"]),
            "user_count": e["user_count"],
            "summary_count": e["summary_count"],
            "status": e["status"]
        } for e in execution_history[-5:]]
    })

def schedule_jobs():
    # Weekly full sync
    schedule.every().sunday.at("00:00").do(lambda: migrate_all(full_sync=True)).tag("weekly")
    
    # Daily incremental sync at 3am
    schedule.every().day.at("03:00").do(lambda: migrate_all(full_sync=False)).tag("daily")
    
    logger.info("Scheduler started")
    while True:
        schedule.run_pending()
        time.sleep(30)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 7860))
    
    if os.getenv('RENDER'):
        # In Render environment
        logger.info("Starting in Render mode (without scheduler thread)")
        app.run(host="0.0.0.0", port=port)
    else:
        # Local development with scheduler
        logger.info("Starting in local mode with scheduler")
        threading.Thread(target=schedule_jobs, daemon=True).start()
        app.run(host="0.0.0.0", port=port)
