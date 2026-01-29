import os
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# Create url from .env or defaults
def get_db_url() -> str:
    user = os.getenv("POSTGRES_USER", "jobtrends")
    pwd = os.getenv("POSTGRES_PASSWORD", "jobtrends")
    host = os.getenv("POSTGRES_HOST", "postgres")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "jobtrends")

    return f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}"

engine: Engine = create_engine(get_db_url(), pool_pre_ping=True)

def init_db() -> None:
    schema_path = os.path.join(os.path.dirname(__file__), "schema.sql")
    
    # Read schema
    with open(schema_path, "r", encoding="utf-8") as f:
        ddl = f.read()
    
    # Open db with schema
    with engine.begin() as conn:
        conn.execute(text(ddl))