from fastapi import FastAPI, Query
from typing import Optional
from contextlib import asynccontextmanager
from sqlalchemy import text
from .db import engine, init_db
from .models import JobOut
from fastapi.middleware.cors import CORSMiddleware

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup (before yield)
    init_db()
    yield

    # Shutdown (after yield)

app = FastAPI(title="Real-Time Job Market Trends API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/health")
def health():
    return {"status": "ok"}

# Pull jobs from PostgreSQL given input parameters by building SQL query
@app.get("/jobs")
def get_jobs(
    limit: int = Query(20, ge=1, le=200), # limit & offset for pagination
    offset: int = Query(0, ge=0),
    company: Optional[str] = None,
    location: Optional[str] = None,
    role_category: Optional[str] = None,
    seniority: Optional[str] = None,
):
    # Query() sets query parameters (? in url)

    # Base query
    sql = """
        SELECT
            event_id,
            ingested_at,
            company,
            title,
            location,
            seniority,
            role_category,
            description,
            url,
            techs
        FROM jobs
        WHERE 1=1
    """
    # NOTE: WHERE 1=1 is dummy for building query

    # SQL Query parameters
    params = {
        "limit": limit,
        "offset": offset,
    }

    # Build query
    if company:
        sql += " AND company ILIKE :company"
        params["company"] = f"%{company}%"

    if location:
        sql += " AND location ILIKE :location"
        params["location"] = f"%{location}%"

    if role_category:
        sql += " AND role_category = :role_category"
        params["role_category"] = role_category

    if seniority:
        sql += " AND seniority = :seniority"
        params["seniority"] = seniority

    # Limit + offset
    sql += " ORDER BY ingested_at DESC LIMIT :limit OFFSET :offset"

    with engine.begin() as conn:
        rows = conn.execute(text(sql), params).mappings().all()

    return [dict(r) for r in rows]

# GET top techs from most recent aggregate window
@app.get("/trends/tech/top")
def get_top_tech_trends(
    window_size_sec: int = Query(300, ge=1), # 300 -> 5 minutes
    limit: int = Query(10, ge=1, le=100),
):
    # Find latest window
    # Join with trend_tech table
    # Sort -> Order by count
    sql = """
        WITH latest_window AS (
            SELECT MAX(window_start) AS max_window_start
            FROM trend_tech
            WHERE window_size_sec = :window_size_sec
        )
        SELECT
            t.window_start,
            t.window_end,
            t.window_size_sec,
            t.tech,
            t.count
        FROM trend_tech t
        JOIN latest_window lw
          ON t.window_start = lw.max_window_start
        WHERE t.window_size_sec = :window_size_sec
        ORDER BY t.count DESC
        LIMIT :limit
    """

    with engine.begin() as conn:
        rows = conn.execute(
            text(sql),
            {"window_size_sec": window_size_sec, "limit": limit},
        ).mappings().all()

    return [dict(r) for r in rows]

# GET timeseries of techs over time from aggregated windows
@app.get("/trends/tech/timeseries")
def get_tech_timeseries(
    tech: str,
    window_size_sec: int = Query(300, ge=1),
    limit: int = Query(100, ge=1, le=1000),
):
    sql = """
        SELECT
            window_start,
            window_end,
            window_size_sec,
            tech,
            count
        FROM trend_tech
        WHERE tech = :tech
          AND window_size_sec = :window_size_sec
        ORDER BY window_start ASC
        LIMIT :limit
    """

    with engine.begin() as conn:
        rows = conn.execute(
            text(sql),
            {
                "tech": tech,
                "window_size_sec": window_size_sec,
                "limit": limit,
            },
        ).mappings().all()

    return [dict(r) for r in rows]


@app.get("/trends/roles/top")
def get_top_role_trends(
    window_size_sec: int = Query(300, ge=1),
    limit: int = Query(10, ge=1, le=100),
):
    sql = """
        WITH latest_window AS (
            SELECT MAX(window_start) AS max_window_start
            FROM trend_role
            WHERE window_size_sec = :window_size_sec
        )
        SELECT
            t.window_start,
            t.window_end,
            t.window_size_sec,
            t.role_category,
            t.count
        FROM trend_role t
        JOIN latest_window lw
          ON t.window_start = lw.max_window_start
        WHERE t.window_size_sec = :window_size_sec
        ORDER BY t.count DESC
        LIMIT :limit
    """

    with engine.begin() as conn:
        rows = conn.execute(
            text(sql),
            {"window_size_sec": window_size_sec, "limit": limit},
        ).mappings().all()

    return [dict(r) for r in rows]


@app.get("/trends/roles/timeseries")
def get_role_timeseries(
    role_category: str,
    window_size_sec: int = Query(300, ge=1),
    limit: int = Query(100, ge=1, le=1000),
):
    sql = """
        SELECT
            window_start,
            window_end,
            window_size_sec,
            role_category,
            count
        FROM trend_role
        WHERE role_category = :role_category
          AND window_size_sec = :window_size_sec
        ORDER BY window_start ASC
        LIMIT :limit
    """

    with engine.begin() as conn:
        rows = conn.execute(
            text(sql),
            {
                "role_category": role_category,
                "window_size_sec": window_size_sec,
                "limit": limit,
            },
        ).mappings().all()

    return [dict(r) for r in rows]