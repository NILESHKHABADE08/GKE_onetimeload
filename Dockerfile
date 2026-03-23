FROM python:3.10-slim

WORKDIR /app

# 👇 FIX: copy from app folder
COPY app/requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# 👇 copy full app
COPY app/ .

CMD ["python", "main.py"]
