import os
import json
import pytz
import firebase_admin
from flask import Flask, request, jsonify, render_template_string
from firebase_admin import credentials, firestore
import psycopg2
from datetime import datetime, timedelta

# Flask app
app = Flask(__name__)

# Load Firebase credentials from environment variable (JSON string)
firebase_creds_json = os.environ.get('FIREBASE_CREDS')
firebase_creds_dict = json.loads(firebase_creds_json)
cred = credentials.Certificate(firebase_creds_dict)
firebase_admin.initialize_app(cred)

# Firestore
db = firestore.client()

# Supabase credentials from environment variables
SUPABASE_HOST = os.environ.get('SUPABASE_HOST')
SUPABASE_USER = os.environ.get('SUPABASE_USER')
SUPABASE_PASSWORD = os.environ.get('SUPABASE_PASSWORD')
SUPABASE_DATABASE = os.environ.get('SUPABASE_DATABASE')

# Connect to Supabase (PostgreSQL)
def connect_to_supabase():
    return psycopg2.connect(
        host=SUPABASE_HOST,
        user=SUPABASE_USER,
        password=SUPABASE_PASSWORD,
        database=SUPABASE_DATABASE
    )

# Full migration
def full_migration():
    conn = connect_to_supabase()
    cur = conn.cursor()
    docs = db.collection("checking_data").stream()

    for doc in docs:
        data = doc.to_dict()
        checking_id = doc.id

        cur.execute("SELECT 1 FROM checking_data WHERE checking_id = %s", (checking_id,))
        if cur.fetchone():
            cur.execute("""
                UPDATE checking_data SET
                    user_id = %s,
                    start = %s,
                    stop = %s,
                    total_time = %s,
                    date = %s
                WHERE checking_id = %s
            """, (
                data.get("user_id"),
                data.get("start"),
                data.get("stop"),
                data.get("total_time"),
                data.get("date"),
                checking_id
            ))
        else:
            cur.execute("""
                INSERT INTO checking_data (
                    checking_id, user_id, start, stop, total_time, date
                ) VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                checking_id,
                data.get("user_id"),
                data.get("start"),
                data.get("stop"),
                data.get("total_time"),
                data.get("date")
            ))

    conn.commit()
    cur.close()
    conn.close()
    return f"Full migration complete. Total documents: {len(list(docs))}"

# Incremental migration
def incremental_migration():
    conn = connect_to_supabase()
    cur = conn.cursor()
    now = datetime.now(pytz.timezone("Asia/Bangkok"))
    one_hour_ago = now - timedelta(hours=1)

    query = db.collection("checking_data").where("timestamp", ">=", one_hour_ago)
    docs = query.stream()

    count = 0
    for doc in docs:
        data = doc.to_dict()
        checking_id = doc.id

        cur.execute("SELECT 1 FROM checking_data WHERE checking_id = %s", (checking_id,))
        if cur.fetchone():
            cur.execute("""
                UPDATE checking_data SET
                    user_id = %s,
                    start = %s,
                    stop = %s,
                    total_time = %s,
                    date = %s
                WHERE checking_id = %s
            """, (
                data.get("user_id"),
                data.get("start"),
                data.get("stop"),
                data.get("total_time"),
                data.get("date"),
                checking_id
            ))
        else:
            cur.execute("""
                INSERT INTO checking_data (
                    checking_id, user_id, start, stop, total_time, date
                ) VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                checking_id,
                data.get("user_id"),
                data.get("start"),
                data.get("stop"),
                data.get("total_time"),
                data.get("date")
            ))
        count += 1

    conn.commit()
    cur.close()
    conn.close()
    return f"Incremental migration complete. New/Updated documents: {count}"

# Home page (manual trigger UI)
@app.route("/")
def index():
    return render_template_string("""
        <h2>Firebase → Supabase Sync</h2>
        <form action="/run" method="post">
            <select name="type">
                <option value="full">Full Migration</option>
                <option value="incremental">Incremental (last 1 hour)</option>
            </select>
            <button type="submit">Run Now</button>
        </form>
    """)

# API endpoint for cronjob
@app.route("/run", methods=["POST"])
def run():
    migration_type = request.form.get("type") or request.json.get("type")
    if migration_type == "full":
        result = full_migration()
    elif migration_type == "incremental":
        result = incremental_migration()
    else:
        return jsonify({"error": "Invalid type. Use 'full' or 'incremental'."}), 400

    return jsonify({"message": result})

# Gunicorn compatibility
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
