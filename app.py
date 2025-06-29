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


app = Flask(__name__)
BANGKOK_TZ = pytz.timezone('Asia/Bangkok')

# Firebase Initialization
firebase_creds_json = os.environ.get('FIREBASE_CREDS')
firebase_creds_dict = json.loads(firebase_creds_json)
cred = credentials.Certificate(firebase_creds_dict)
firebase_admin.initialize_app(cred)
fs_db = firestore.client()

# Supabase Connection
def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("SUPABASE_HOST"),
        port=os.getenv("SUPABASE_PORT"),
        database=os.getenv("SUPABASE_DB"),
        user=os.getenv("SUPABASE_USER"),
        password=os.getenv("SUPABASE_PASSWORD"),
    )

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
        return datetime.fromisoformat(val.replace("Z", "+00:00")).astimezone(BANGKOK_TZ)
    except:
        return None

def background_task(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        global is_running
        if is_running:
            return
        is_running = True
        try:
            return fn(*args, **kwargs)
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
        user_count = migrate_users(since)
        summary_count = migrate_summaries(since)
        status = "success"
    except Exception as e:
        status = f"failed: {e}"

    last_execution = now_thai()
    execution_history.append({
        "type": "full" if full_sync else "incremental",
        "start_time": start_time,
        "end_time": last_execution,
        "user_count": user_count,
        "summary_count": summary_count,
        "status": status
    })

def migrate_users(since=None):
    conn = get_db_connection()
    cur = conn.cursor()
    users = fs_db.collection("users")
    if since:
        users = users.where("lastUpdate", ">=", since.astimezone(pytz.UTC))
    users = users.stream()

    count = 0
    for doc in users:
        u = doc.to_dict()
        try:
            cur.execute("""
                INSERT INTO users (
                    id, email, firstName, lastName, phoneNumber,
                    gender, age, thaiId, lastUpdate
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (id) DO UPDATE SET
                    email=EXCLUDED.email,
                    firstName=EXCLUDED.firstName,
                    lastName=EXCLUDED.lastName,
                    phoneNumber=EXCLUDED.phoneNumber,
                    gender=EXCLUDED.gender,
                    age=EXCLUDED.age,
                    thaiId=EXCLUDED.thaiId,
                    lastUpdate=EXCLUDED.lastUpdate;
            """, (
                doc.id,
                u.get("email"),
                u.get("firstName"),
                u.get("lastName"),
                u.get("phoneNumber"),
                u.get("gender"),
                u.get("age"),
                u.get("thaiId"),
                parse_ts(u.get("lastUpdate")),
            ))
            count += 1
        except Exception as e:
            print(f"❌ Failed user {doc.id}: {e}")

    conn.commit()
    cur.close()
    conn.close()
    return count

def migrate_summaries(since=None):
    conn = get_db_connection()
    cur = conn.cursor()
    users = fs_db.collection("users").stream()
    count = 0

    for user_doc in users:
        user_id = user_doc.id
        records = fs_db.collection("users").document(user_id).collection("records")
        if since:
            records = records.where("lastUpdate", ">=", since.astimezone(pytz.UTC))
        records = list(records.stream())

        if not records:
            continue

        recs = {}
        for rec in records:
            data = rec.to_dict()
            recorder = data.get("recorder") or "unknown"
            if recorder not in recs:
                recs[recorder] = []
            recs[recorder].append(data)

        for recorder, items in recs.items():
            latest = sorted(items, key=lambda x: parse_ts(x.get("lastUpdate")) or datetime.min, reverse=True)[0]
            prediction = latest.get("prediction", {})
            risk = prediction.get("risk") if isinstance(prediction, dict) else None
            counts = {f: 0 for f in FIELDS_TO_COUNT}
            for item in items:
                for f in FIELDS_TO_COUNT:
                    if item.get(f) is not None:
                        counts[f] += 1

            try:
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
            except Exception as e:
                print(f"❌ Failed summary for {user_id}/{recorder}: {e}")

    conn.commit()
    cur.close()
    conn.close()
    return count

@app.route("/")
def index():
    next_run = schedule.next_run()
    return render_template_string("""
        <h1>Migration Console</h1>
        <p>สถานะ: <strong>{{ 'กำลังทำงาน' if is_running else 'พร้อมทำงาน' }}</strong></p>
        <p>เวลาปัจจุบัน: {{ now }}</p>
        <p>ล่าสุด: {{ last }}</p>
        <p>รอบถัดไป: {{ next }}</p>
        <form method="post" action="/run">
            <button name="type" value="incremental">Sync เฉพาะใหม่</button>
            <button name="type" value="full">Sync ทั้งหมด</button>
        </form>
        <h2>ประวัติ</h2>
        {% for e in history %}
        <p>{{ e.start_time.strftime('%Y-%m-%d %H:%M:%S') }} - {{ e.status }} (Users: {{ e.user_count }}, Summary: {{ e.summary_count }})</p>
        {% endfor %}
    """, now=format_dt(now_thai()),
       last=format_dt(last_execution),
       next=format_dt(next_run) if next_run else "ไม่ได้ตั้งเวลา",
       is_running=is_running,
       history=execution_history[-5:]
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
        "history": execution_history[-5:]
    })

def schedule_jobs():
    schedule.every().sunday.at("00:00").do(lambda: migrate_all(full_sync=True)).tag("weekly")
    while True:
        schedule.run_pending()
        time.sleep(30)

if __name__ == "__main__":
    if os.getenv('RENDER'):
        # In Render environment, don't start the scheduler thread
        app.run(host="0.0.0.0", port=7860)
    else:
        # Local development with scheduler
        threading.Thread(target=schedule_jobs, daemon=True).start()
        app.run(host="0.0.0.0", port=7860)