from pyspark.sql import SparkSession
from fastapi import Depends, FastAPI, Header, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
import os
import random
import psycopg
import math
from datetime import datetime, timezone
import uuid
import time
from contextlib import contextmanager

API_KEY = os.environ.get("API_KEY", "").strip()

origins = [
    "http://localhost:4200",
]

def connect_with_retry():
    retries = int(os.environ.get("DB_CONNECT_RETRIES", "1"))
    retry_delay = float(os.environ.get("DB_CONNECT_RETRY_DELAY", "0.2"))
    last_error: psycopg.OperationalError | None = None
    for attempt in range(retries + 1):
        try:
            return psycopg.connect(
                host=os.environ.get("DB_HOST", "postgres"),
                port=os.environ.get("DB_PORT", 5432),
                dbname=os.environ.get("DB_NAME", "driva-dw"),
                user=os.environ.get("DB_USR", "driva"),
                password=os.environ.get("DB_PSW", "driva"),
            )
        except psycopg.OperationalError as exc:
            last_error = exc
            if attempt < retries:
                time.sleep(retry_delay)
                continue
            raise
    if last_error is not None:
        raise last_error
    raise psycopg.OperationalError("Failed to connect to the database")


@contextmanager
def get_connection():
    conn = connect_with_retry()
    try:
        yield conn
    finally:
        conn.close()

app = FastAPI(title="Driva API", description="API for the Driva project")

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


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
            ''' Retorna KPIs para o dashboard (ex.: total de jobs, % sucesso, tempo médio,
distribuição por categoria) '''
            with get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT COUNT(*) FROM gold.enriquecimentos")
                    total_jobs = cursor.fetchone()
                    if total_jobs is not None:
                        total_jobs = total_jobs[0]
                    else:
                        total_jobs = 0

                    cursor.execute("SELECT COUNT(*) FROM gold.enriquecimentos WHERE status_processamento = 'CONCLUIDO'")
                    success_rate = cursor.fetchone()
                    if success_rate is not None:
                        success_rate = success_rate[0]
                    else:
                        success_rate = 0
                    cursor.execute("SELECT COUNT(*) FROM gold.enriquecimentos WHERE categoria_tamanho_job = 'PEQUENO'")
                    small_jobs = cursor.fetchone()
                    if small_jobs is not None:
                        small_jobs = small_jobs[0]
                    else:
                        small_jobs = 0

                    cursor.execute("SELECT COUNT(*) FROM gold.enriquecimentos WHERE categoria_tamanho_job = 'MEDIO'")
                    medium_jobs = cursor.fetchone()
                    if medium_jobs is not None:
                        medium_jobs = medium_jobs[0]
                    else:
                        medium_jobs = 0

                    cursor.execute("SELECT COUNT(*) FROM gold.enriquecimentos WHERE categoria_tamanho_job = 'GRANDE'")
                    large_jobs = cursor.fetchone()
                    if large_jobs is not None:
                        large_jobs = large_jobs[0]
                    else:
                        large_jobs = 0

                    cursor.execute("SELECT COUNT(*) FROM gold.enriquecimentos WHERE categoria_tamanho_job = 'MUITO_GRANDE'")
                    very_large_jobs = cursor.fetchone()
                    if very_large_jobs is not None:
                        very_large_jobs = very_large_jobs[0]
                    else:
                        very_large_jobs = 0

                    response = {
                        "total_jobs": total_jobs,
                        #"success_rate": success_rate,
                        "small_jobs": small_jobs,
                        "medium_jobs": medium_jobs,
                        "large_jobs": large_jobs,
                        "very_large_jobs": very_large_jobs,
                    }
                    return response
        except psycopg.Error:
            raise HTTPException(status_code=500, detail="Database error")

@app.get("/analytics/enrichments/status", dependencies=[Depends(require_api_key)])
def enrichments_status(start_date: str = Query(default=None), end_date: str = Query(default=None), processing_status: str = Query(default=None), workspace_id: str = Query(default=None)):

    """
    Exemplo de requisição:
        GET /analytics/enrichments/status?
            start_date=2024-01-01T00:00:00Z&
            end_date=2024-01-31T23:59:59Z&
            processing_status=COMPLETED&
            workspace_id=3fa85f64-5717-4562-b3fc-2c963f66afa6

    Parâmetros de query esperados:
        - start_date: str (ex: "2024-01-01T00:00:00Z")
        - end_date: str (ex: "2024-01-31T23:59:59Z")
        - processing_status: str (ex: "COMPLETED")
        - workspace_id: str (UUID, ex: "3fa85f64-5717-4562-b3fc-2c963f66afa6")
    """

    if (random.random() < 0.05):
        raise HTTPException(status_code=429, detail="Too many requests")
    else:
        try:
            ''' Lista paginada/filtrável (ex.: por id_workspace , status_processamento , período) '''
            with get_connection() as conn:
                with conn.cursor() as cursor:
                    query = "SELECT * FROM gold.enriquecimentos"
                    conditions = []
                    params = []
                    if start_date is not None and end_date is not None:
                        conditions.append("data_criacao BETWEEN %s AND %s")
                        params.append(
                            datetime.fromisoformat(start_date)
                            .replace(tzinfo=timezone.utc)
                            .isoformat()
                            .replace("+00:00", "Z")
                        )
                        params.append(
                            datetime.fromisoformat(end_date)
                            .replace(tzinfo=timezone.utc)
                            .isoformat()
                            .replace("+00:00", "Z")
                        )
                    if processing_status is not None:
                        conditions.append("status_processamento = %s")
                        params.append(processing_status.upper())
                    if workspace_id is not None:
                        conditions.append("id_workspace = %s")
                        params.append(str(uuid.UUID(workspace_id)))
                    if conditions:
                        query += " WHERE " + " AND ".join(conditions)

                    cursor.execute(query, params)
                    records = cursor.fetchall()
                    data = [
                        {
                            "id_enriquecimento": record[0],
                            "id_workspace": record[1],
                            "nome_workspace": record[2],
                            "total_contatos": record[3],
                            "tipo_contato": record[4],
                            "status_processamento": record[5],
                            "duracao_processamento_minutos": record[6],
                            "tempo_por_contato_minutos": record[7],
                            "processamento_sucesso": record[8],
                            "categoria_tamanho_job": record[9],
                            "necessita_reprocessamento": record[10],
                            "data_criacao": format_dt(record[11]),
                            "data_atualizacao": format_dt(record[12]),
                            "data_atualizacao_dw": format_dt(record[13]),
                        } for record in records
                    ]
                    response = {
                        "data": data,
                    }
                    return response
        except psycopg.Error:
            raise HTTPException(status_code=500, detail="Database error")

@app.get("/analytics/workspaces/top", dependencies=[Depends(require_api_key)])
def workspaces_top():
    if (random.random() < 0.05):
        raise HTTPException(status_code=429, detail="Too many requests")
    else:
        try:
            with get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT id_workspace, nome_workspace, COUNT(*) as total_jobs FROM gold.enriquecimentos GROUP BY id_workspace, nome_workspace ORDER BY total_jobs DESC")
                    records = cursor.fetchall()
                    data = [
                        {
                            "id_workspace": record[0],
                            "nome_workspace": record[1],
                            "total_jobs": record[2],
                        } for record in records
                    ]
                    response = {
                        "data": data,
                    }
                    return response
        except psycopg.Error:
            raise HTTPException(status_code=500, detail="Database error")