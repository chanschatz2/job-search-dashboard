-- Main job postings table
CREATE TABLE IF NOT EXISTS jobs (
  event_id TEXT PRIMARY KEY,
  ingested_at TIMESTAMPTZ NOT NULL,
  company TEXT,
  title TEXT,
  location TEXT,
  seniority TEXT,
  role_category TEXT,
  description TEXT,
  url TEXT,
  techs TEXT[] DEFAULT ARRAY[]::TEXT[] -- Changed from JSONB to TEXT[] to support Spark raw out
);

-- Create indexes for retrieval
CREATE INDEX IF NOT EXISTS idx_jobs_ingested_at ON jobs (ingested_at DESC);
CREATE INDEX IF NOT EXISTS idx_jobs_company ON jobs (company);
CREATE INDEX IF NOT EXISTS idx_jobs_location ON jobs (location);
CREATE INDEX IF NOT EXISTS idx_jobs_role_category ON jobs (role_category);
CREATE INDEX IF NOT EXISTS idx_jobs_seniority ON jobs (seniority);
CREATE INDEX IF NOT EXISTS idx_jobs_techs_gin ON jobs USING GIN (techs);

-- Aggregate for a specific technology keyword over a window
CREATE TABLE IF NOT EXISTS trend_tech (
  window_start TIMESTAMPTZ NOT NULL,
  window_end   TIMESTAMPTZ NOT NULL,
  window_size_sec INT NOT NULL,
  tech TEXT NOT NULL,
  count BIGINT NOT NULL,
  PRIMARY KEY (window_start, window_size_sec, tech)
);

CREATE INDEX IF NOT EXISTS idx_trend_tech_window ON trend_tech (window_size_sec, window_start DESC);

-- Aggregate for a specific role keyword over a window
CREATE TABLE IF NOT EXISTS trend_role (
  window_start TIMESTAMPTZ NOT NULL,
  window_end   TIMESTAMPTZ NOT NULL,
  window_size_sec INT NOT NULL,
  role_category TEXT NOT NULL,
  count BIGINT NOT NULL,
  PRIMARY KEY (window_start, window_size_sec, role_category)
);

CREATE INDEX IF NOT EXISTS idx_trend_role_window ON trend_role (window_size_sec, window_start DESC);