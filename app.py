import os, re, csv, sqlite3, io
from datetime import datetime
from flask import Flask, request, jsonify, render_template_string, send_file
from minio import Minio
from minio.error import S3Error

# --- Flask App ---
app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 5 * 1024 * 1024  # 5 MB

# --- SQLite Config ---
DB_PATH = os.environ.get("DB_PATH", "./data/app.db")
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

# --- MinIO Config ---
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "miniokey")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "miniosecret")

# --- Email Regex ---
EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

# --- HTML Template ---
HTML = """
<!doctype html>
<title>CSV Import (Serverless-style)</title>
<link rel="stylesheet" href="https://unpkg.com/milligram@1.4.1/dist/milligram.min.css">
<div class="container" style="max-width: 900px; margin-top: 30px">
  <h2>CSV Import Demo</h2>
  <p>Upload a CSV with columns: <code>name,email,age</code> (UTF-8).</p>
  <form action="/upload" method="post" enctype="multipart/form-data">
    <input type="file" name="file" accept=".csv">
    <button type="submit" class="button-primary">Upload & Import</button>
    <a class="button" href="/sample">Download Sample CSV</a>
    <a class="button" href="/export">Export DB</a>
  </form>

  {% if summary %}
  <hr>
  <h4>Result</h4>
  <p>Inserted: <b>{{summary.ok}}</b> &nbsp; | &nbsp; Errors: <b>{{summary.err}}</b></p>
  {% if errors %}
  <details open><summary>View errors ({{summary.err}})</summary>
    <ul>
      {% for e in errors %}<li>Row {{e.row}}: {{e.msg}}</li>{% endfor %}
    </ul>
  </details>
  {% endif %}
  {% endif %}

  <hr>
  <h4>Latest records (top 50)</h4>
  <table>
    <thead><tr><th>ID</th><th>Name</th><th>Email</th><th>Age</th><th>Created</th></tr></thead>
    <tbody>
      {% for r in rows %}
      <tr>
        <td>{{r[0]}}</td><td>{{r[1]}}</td><td>{{r[2]}}</td><td>{{r[3]}}</td><td>{{r[4]}}</td>
      </tr>
      {% endfor %}
    </tbody>
  </table>
</div>
"""

# --- DB Helper ---
def db():
    con = sqlite3.connect(DB_PATH)
    con.execute("""CREATE TABLE IF NOT EXISTS customers (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        email TEXT NOT NULL UNIQUE,
        age INTEGER NOT NULL,
        created_at TEXT NOT NULL
    )""")
    return con

# --- CSV Import Function ---
def import_csv_stream(text_stream):
    reader = csv.DictReader(text_stream)
    required = {"name", "email", "age"}
    if not required.issubset(set([c.strip().lower() for c in reader.fieldnames or []])):
        raise ValueError("Header must include: name,email,age")

    errors = []
    ok = 0
    with db() as con:
        for i, row in enumerate(reader, start=2):
            name = (row.get("name") or "").strip()
            email = (row.get("email") or "").strip().lower()
            age_s = (row.get("age") or "").strip()

            if len(name) < 2:
                errors.append({"row": i, "msg": "name must be at least 2 characters"})
                continue
            if not EMAIL_RE.match(email):
                errors.append({"row": i, "msg": "invalid email"})
                continue
            try:
                age = int(age_s)
                if age < 1 or age > 120:
                    raise ValueError
            except Exception:
                errors.append({"row": i, "msg": "age must be an integer 1–120"})
                continue

            try:
                con.execute("INSERT INTO customers(name,email,age,created_at) VALUES(?,?,?,?)",
                            (name, email, age, datetime.utcnow().isoformat(timespec="seconds")+"Z"))
                ok += 1
            except sqlite3.IntegrityError:
                errors.append({"row": i, "msg": "duplicate email (already imported)"})
        con.commit()
    return ok, errors

# --- Validate MinIO Connection ---
def validate_minio_connection():
    endpoint = MINIO_ENDPOINT.replace("http://", "").replace("https://", "")
    try:
        mc = Minio(endpoint,
                   access_key=MINIO_ACCESS_KEY,
                   secret_key=MINIO_SECRET_KEY,
                   secure=False)
        print(f"[DEBUG] Attempting to connect to MinIO at {MINIO_ENDPOINT}...")
        buckets = list(mc.list_buckets())
        print(f"[SUCCESS] MinIO connection established. Buckets: {[b.name for b in buckets]}")
        return mc
    except S3Error as e:
        print(f"[ERROR] MinIO S3 error: {e}")
    except Exception as e:
        print(f"[ERROR] Could not connect to MinIO: {e}")
    return None

# --- Web Routes ---
@app.get("/")
def home():
    with db() as con:
        rows = con.execute("SELECT id,name,email,age,created_at FROM customers ORDER BY id DESC LIMIT 50").fetchall()
    return render_template_string(HTML, rows=rows, summary=None, errors=None)

@app.get("/sample")
def sample():
    csv_bytes = b"name,email,age\nAlice,alice@example.com,30\nBob,bob@example.org,25\n"
    return send_file(io.BytesIO(csv_bytes), mimetype="text/csv", as_attachment=True, download_name="sample_customers.csv")

@app.get("/export")
def export():
    with db() as con:
        rows = con.execute("SELECT name,email,age,created_at FROM customers ORDER BY id").fetchall()
    sio = io.StringIO()
    w = csv.writer(sio)
    w.writerow(["name","email","age","created_at"])
    for r in rows:
        w.writerow(r)
    return send_file(io.BytesIO(sio.getvalue().encode("utf-8")), mimetype="text/csv",
                     as_attachment=True, download_name="customers_export.csv")

@app.post("/upload")
def upload():
    if "file" not in request.files:
        return "No file part", 400
    f = request.files["file"]
    if not f.filename.lower().endswith(".csv"):
        return "Please upload a .csv file", 400

    data = f.read()
    wrapper = io.TextIOWrapper(io.BytesIO(data), encoding="utf-8", newline="")
    try:
        ok, errors = import_csv_stream(wrapper)
    except ValueError as e:
        return str(e), 400

    # Upload to MinIO
    try:
        bucket = "uploads"
        if not minio_client.bucket_exists(bucket):
            print(f"[DEBUG] Bucket '{bucket}' does not exist. Creating...")
            minio_client.make_bucket(bucket)
        ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        obj_name = f"{ts}_{os.path.basename(f.filename)}"
        print(f"[DEBUG] Uploading '{obj_name}' ({len(data)} bytes) to bucket '{bucket}'")
        minio_client.put_object(bucket, obj_name, io.BytesIO(data), length=len(data), content_type="text/csv")
        print(f"[SUCCESS] Uploaded file to MinIO: {obj_name}")
    except Exception as e:
        print(f"[ERROR] Failed to upload to MinIO: {e}")

    with db() as con:
        rows = con.execute("SELECT id,name,email,age,created_at FROM customers ORDER BY id DESC LIMIT 50").fetchall()
    return render_template_string(HTML, rows=rows, summary={"ok": ok, "err": len(errors)}, errors=errors)

@app.get("/health")
def health():
    return "OK", 200

# --- MinIO Webhook Endpoint ---
@app.post("/obs-event")
def obs_event():
    payload = request.get_json(force=True, silent=True) or {}
    records = payload.get("Records") or []
    total_ok, total_err = 0, 0
    details = []

    for rec in records:
        bucket = (((rec.get("s3") or {}).get("bucket") or {}).get("name")) or ""
        obj = (((rec.get("s3") or {}).get("object") or {}).get("key")) or ""
        if not bucket or not obj:
            continue
        if not obj.lower().endswith(".csv"):
            details.append({"bucket": bucket, "object": obj, "skipped": "not a .csv"})
            continue

        try:
            print(f"[OBS EVENT] Downloading '{obj}' from bucket '{bucket}'")
            resp = minio_client.get_object(bucket, obj)
            data = resp.read(); resp.close(); resp.release_conn()
            wrapper = io.TextIOWrapper(io.BytesIO(data), encoding="utf-8", newline="")
            ok, errors = import_csv_stream(wrapper)
            total_ok += ok
            total_err += len(errors)
            details.append({"bucket": bucket, "object": obj, "inserted": ok, "errors": len(errors)})
            print(f"[OBS EVENT] Imported {ok} rows, errors {len(errors)} from {bucket}/{obj}")
        except Exception as e:
            details.append({"bucket": bucket, "object": obj, "error": str(e)})
            print(f"[OBS EVENT] Error for {bucket}/{obj}: {e}")

    return jsonify({"ok": total_ok, "errors": total_err, "items": details}), 200

# --- Main ---
if __name__ == "__main__":
    minio_client = validate_minio_connection()
    if not minio_client:
        print("[FATAL] Cannot proceed. Check MinIO endpoint, keys, and network.")
        exit(1)
    app.run(host="0.0.0.0", port=8080)
