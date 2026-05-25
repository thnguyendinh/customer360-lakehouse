FROM apache/superset:3.1.0
USER root
RUN apt-get update && apt-get install -y gcc libpq-dev && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir trino==0.327.0 sqlalchemy-trino==0.5.0 psycopg2-binary
ENV LANG C.UTF-8
ENV LC_ALL C.UTF-8
USER superset
