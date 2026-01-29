from fastapi import FastAPI, Query
from contextlib import asynccontextmanager
from sqlalchemy import text
from .db import engine, init_db
from .models import JobOut

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup (before yield)
    init_db()
    # TODO: open shared resources if needed
    yield

    # Shutdown (after yield)

app = FastAPI(title="Real-Time Job Market Trends API", lifespan=lifespan)

@app.get("/health")
def health():
    return {"status": "ok"}

# Pull jobs from PostgreSQL
@app.get("/jobs", response_model=list[JobOut])
def list_jobs(limit: int = Query(20, ge=1, le=200), offset: int = Query(0, ge=0)):
    # Query() sets query parameters (? in url)
    sql = text("""
        SELECT event_id, ingested_at, company, title, location, seniority, role_category,
               description, url, techs
        FROM jobs
        ORDER BY ingested_at DESC
        LIMIT :limit OFFSET :offset
    """)
    with engine.begin() as conn:
        rows = conn.execute(sql, {"limit": limit, "offset": offset}).mappings().all()
    return [dict(r) for r in rows]
