FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app.py .
# DB will live at /data/app.db (we'll mount a volume to /data)
ENV DB_PATH=/data/app.db
EXPOSE 8080
CMD ["python","app.py"]


