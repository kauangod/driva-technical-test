from pyspark.sql import SparkSession
from fastapi import Depends, FastAPI, Header, HTTPException, Query
import os
import random
import psycopg
import math
from datetime import timezone

API_KEY = os.environ.get("API_KEY", "").strip()

def get_connection():
    return psycopg.connect(
        host=os.environ.get("DB_HOST", "postgres"),
        port=os.environ.get("DB_PORT", 5432),
        dbname=os.environ.get("DB_NAME", "driva-dw"),
        user=os.environ.get("DB_USR", "driva"),
        password=os.environ.get("DB_PSW", "driva"),
    )

app = FastAPI(title="Driva API", description="API for the Driva project")


def require_api_key(authorization: str | None = Header(default=None)) -> None:
    if not authorization:
        raise HTTPException(status_code=401, detail="Missing Authorization header")
    expected = f"Bearer {API_KEY}"
    if authorization.strip() != expected:
        raise HTTPException(status_code=403, detail="Invalid API key")


def format_dt(value):
    if value is None:
        return None
    if hasattr(value, "tzinfo") and value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    else:
        value = value.astimezone(timezone.utc)
    return value.replace(microsecond=0).isoformat().replace("+00:00", "Z")


@app.get("/people/v1/enrichments", dependencies=[Depends(require_api_key)])
def enrichments(
    page: int = Query(default=1, ge=1),
    limit: int = Query(default=50, ge=1, le=100),
):

    if (random.random() < 0.05):
        raise HTTPException(status_code=429, detail="Too many requests")
    else:
        try:
            with get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT COUNT(*) FROM api_enrichments_seed.enriquecimentos")
                    count_row = cursor.fetchone()

                    if count_row is None:
                        total_records = 0
                    else:
                        total_records = count_row[0]

                    cursor.execute(
                        "SELECT COUNT(*) FROM (SELECT * FROM api_enrichments_seed.enriquecimentos LIMIT %s OFFSET %s)",
                        (limit, (page - 1) * limit),
                    )
                    count_row = cursor.fetchone()

                    if count_row is None:
                        items_count = 0
                    else:
                        items_count = count_row[0]

                    metadata = {
                        "current_page": page,
                        "items_per_page": items_count,
                        "total_items": total_records,
                        "total_pages": math.ceil(total_records / limit),
                    }
                    cursor.execute(
                        "SELECT * FROM api_enrichments_seed.enriquecimentos ORDER BY id LIMIT %s OFFSET %s", # Evita fazer a ingestão dos mesmos dados (ORDER BY)
                        (limit, (page - 1) * limit),
                    )
                    records = cursor.fetchall()
        except psycopg.Error:
            raise HTTPException(status_code=500, detail="Database error")

        data = [
            {
                "id": record[0],
                "id_workspace": record[1],
                "workspace_name": record[2],
                "total_contacts": record[3],
                "contact_type": record[4],
                "status": record[5],
                "created_at": format_dt(record[6]),
                "updated_at": format_dt(record[7]),
            } for record in records
        ]

        response = {
            "meta": metadata,
            "data": data,
        }

        return response

@app.get("/analytics/overview", dependencies=[Depends(require_api_key)])
def overview():
    if (random.random() < 0.05):
        raise HTTPException(status_code=429, detail="Too many requests")
    else:
        try:
            return {"status": "ok"}
        except psycopg.Error:
            raise HTTPException(status_code=500, detail="Database error")

@app.get("/analytics/enrichments/status", dependencies=[Depends(require_api_key)])
def enrichments_status():
    if (random.random() < 0.05):
        raise HTTPException(status_code=429, detail="Too many requests")
    else:
        try:
            return {"status": "ok"}
        except psycopg.Error:
            raise HTTPException(status_code=500, detail="Database error")

@app.get("/analytics/workspaces/top", dependencies=[Depends(require_api_key)])
def workspaces_top():
    if (random.random() < 0.05):
        raise HTTPException(status_code=429, detail="Too many requests")
    else:
        try:
            return {"status": "ok"}
        except psycopg.Error:
            raise HTTPException(status_code=500, detail="Database error")