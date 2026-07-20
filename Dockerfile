FROM python:3.11-slim

WORKDIR /app

# gcc/libpq-dev are needed to build psycopg2-binary's transitive deps on
# some platforms; curl is used by the container healthcheck.
RUN apt-get update \
    && apt-get install -y --no-install-recommends gcc libpq-dev curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
