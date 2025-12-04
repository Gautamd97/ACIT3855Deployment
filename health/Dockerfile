FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py openapi.yml ./
COPY config ./config

VOLUME ["/data"]

EXPOSE 8120

CMD ["python", "app.py"]
