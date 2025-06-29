import firebase_admin
from firebase_admin import credentials, firestore
import psycopg2
import os
import schedule
import time
from datetime import datetime, timedelta
import pytz
import threading
from flask import Flask, render_template_string, request, redirect, url_for, jsonify
from functools import wraps
import json
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
BANGKOK_TZ = pytz.timezone('Asia/Bangkok')

# Firebase Initialization
try:
    firebase_creds_json = os.environ.get('FIREBASE_CREDS')
    if not firebase_creds_json:
        raise ValueError("FIREBASE_CREDS environment variable not set")
    
    # Handle escaped newlines if present
    firebase_creds_json = firebase_creds_json.replace('\\n', '\n')
    firebase_creds_dict = json.loads(firebase_creds_json)
    
    cred = credentials.Certificate(firebase_creds_dict)
    firebase_admin.initialize_app(cred)
    fs_db = firestore.client()
    logger.info("Firebase initialized successfully")
except Exception as e:
    logger.error(f"Firebase initialization failed: {str(e)}")
    raise

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

def migrate_users(since=None):
    count = 0
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Query Firebase users
        users_ref = fs_db.collection("users")
        if since:
            users_ref = users_ref.where("lastUpdate", ">=", since.astimezone(pytz.UTC))
        
        users = users_ref.stream()
        
        for doc in users:
            user_id = doc.id
            data = doc.to_dict()
            try:
                # Upsert user data with all fields
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
                    logger.info(f"Processed {count} users...")
            except Exception as e:
                logger.error(f"Failed user {user_id}: {str(e)}")
        
        conn.commit()
        logger.info(f"Successfully migrated {count} users")
        return count
        
    except Exception as e:
        logger.error(f"User migration failed: {str(e)}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            cur.close()
            conn.close()

def migrate_summaries(since=None):
    count = 0
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Get all users from Firebase
        users = fs_db.collection("users").stream()
        
        for user_doc in users:
            user_id = user_doc.id
            records_ref = fs_db.collection("users").document(user_id).collection("records")
            
            if since:
                records_ref = records_ref.where("lastUpdate", ">=", since.astimezone(pytz.UTC))
            
            records = list(records_ref.stream())
            
            if not records:
                continue
            
            # Organize records by recorder
            recs = {}
            for rec in records:
                data = rec.to_dict()
                recorder = data.get("recorder") or "unknown"
                if recorder not in recs:
                    recs[recorder] = []
                recs[recorder].append(data)
            
            # Process each recorder's records
            for recorder, items in recs.items():
                try:
                    # Get latest prediction risk
                    latest = max(items, key=lambda x: parse_ts(x.get("lastUpdate")) or datetime.min)
                    prediction = latest.get("prediction", {})
                    risk = prediction.get("risk") if isinstance(prediction, dict) else None
                    
                    # Count fields with data
                    counts = {f: 0 for f in FIELDS_TO_COUNT}
                    for item in items:
                        for f in FIELDS_TO_COUNT:
                            if item.get(f) is not None:
                                counts[f] += 1
                    
                    # Upsert summary
                    cur.execute(f"""
                        INSERT INTO user_record_summary (
                            user_id, recorder, prediction_risk, record_count, {','.join(FIELDS_TO_COUNT)}
                        ) VALUES (
                            %s, %s, %s, %s, {','.join(['%s'] * len(FIELDS_TO_COUNT))}
                        )
                        ON CONFLICT (user_id, recorder) DO UPDATE SET
                            prediction_risk = EXCLUDED.prediction_risk,
                            record_count = EXCLUDED.record_count,
                            {','.join([f"{f} = EXCLUDED.{f}" for f in FIELDS_TO_COUNT])};
                    """, [user_id, recorder, risk, len(items)] + [counts[f] for f in FIELDS_TO_COUNT])
                    
                    count += 1
                    if count % 50 == 0:
                        logger.info(f"Processed {count} summaries...")
                
                except Exception as e:
                    logger.error(f"Failed summary for {user_id}/{recorder}: {str(e)}")
        
        conn.commit()
        logger.info(f"Successfully migrated {count} summaries")
        return count
        
    except Exception as e:
        logger.error(f"Summary migration failed: {str(e)}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            cur.close()
            conn.close()

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