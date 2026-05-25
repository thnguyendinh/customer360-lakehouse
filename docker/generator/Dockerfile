FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir boto3==1.34.0 faker==22.0.0 pandas==2.1.0

COPY src/scripts/ /app/scripts/

CMD ["python", "scripts/generate_data.py", "--backfill", "--interval", "300"]
