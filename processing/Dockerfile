FROM python:3.11-slim

LABEL maintainer="gautam@bcit.local"
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . /app

EXPOSE 8100

CMD ["python", "app.py"]