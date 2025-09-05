# CSV Import + MinIO Demo (Flask + SQLite)

This project is a small **Flask web app** that demonstrates:

- Uploading CSV files (with `name,email,age` columns).  
- Importing CSV rows into a **SQLite database**.  
- Exporting database content as CSV.  
- Storing uploaded files in **MinIO (S3-compatible object storage)**.  
- Handling **MinIO webhook events** (`obs-event`) to automatically import uploaded CSVs.

---

## Features

- Web UI for uploading `.csv` files.  
- Validation rules:
  - `name`: at least 2 characters.  
  - `email`: must be valid format, unique.  
  - `age`: integer between 1 and 120.  
- DB export (`/export`) and sample CSV download (`/sample`).  
- Health check at `/health`.  
- MinIO webhook ingestion at `/obs-event`.

---

## Requirements

- **Python 3.11+**  
- **MinIO server** (running locally or remote).  
- Docker (optional, for containerized run).

---

## Installation (Local)

1. Clone the repo and install dependencies:

   pip install -r requirements.txt
2. Set environment variables (optional, defaults provided):

export MINIO_ENDPOINT=http://localhost:9000
export MINIO_ACCESS_KEY=miniokey
export MINIO_SECRET_KEY=miniosecret
export DB_PATH=./data/app.db


3. Run the app:

python app.py


4. Visit http://localhost:8080
 in your browser.
###  Running with Docker

1. Build the image:

docker build -t csv-import-app .


2. Run the container with a persistent volume for SQLite DB:

docker run -it --rm \
  -p 8080:8080 \
  -e MINIO_ENDPOINT=http://minio:9000 \
  -e MINIO_ACCESS_KEY=miniokey \
  -e MINIO_SECRET_KEY=miniosecret \
  -v $(pwd)/data:/data \
  csv-import-app


The DB will be stored in ./data/app.db.
### Endpoints

1. / → Web UI (upload form + latest records).

2. /upload → Upload CSV and import.

3. /sample → Download sample CSV.

4. /export → Export DB as CSV.

5. /health → Health check.

6. /obs-event → MinIO webhook listener.
### ☁️ MinIO Setup
1. Start MinIO (example):

docker run -it --rm \
  -p 9000:9000 -p 9001:9001 \
  -e MINIO_ROOT_USER=miniokey \
  -e MINIO_ROOT_PASSWORD=miniosecret \
  -v $(pwd)/minio-data:/data \
  minio/minio server /data --console-address ":9001"


2. Create a bucket named uploads (or let the app auto-create it).

3. Configure a MinIO event notification (webhook) pointing http://<app-host>:8080/obs-event
### Example Workflow

1. Upload sample_customers.csv from the UI.

2. Data is validated and inserted into SQLite.

3. File is uploaded to MinIO bucket uploads.

4. If MinIO is configured with webhook → app auto-imports future .csv files uploaded directly to MinIO.


