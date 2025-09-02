FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN mkdir -p /app/external_db

VOLUME /app/external_db

EXPOSE 6379 5672

CMD ["python", "main.py"]
