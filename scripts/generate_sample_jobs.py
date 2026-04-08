import json
import random
from pathlib import Path

OUT_PATH = Path("data/sample_jobs.jsonl")

companies = [
    "Acme", "Globex", "Initech", "Stark Labs", "Wayfinder", "Blue Peak",
    "Northstar", "Pioneer Data", "Nimbus", "Vertex", "Cedar Systems",
    "Redwood Analytics", "Summit Tech", "Atlas AI", "Ironclad"
]

titles = [
    "Backend Engineer",
    "Frontend Engineer",
    "Full Stack Engineer",
    "Data Engineer",
    "Software Engineer",
    "Data Scientist",
    "Analytics Engineer",
    "Platform Engineer",
    "Machine Learning Engineer",
    "DevOps Engineer",
]

locations = [
    "Remote",
    "Chicago, IL",
    "New York, NY",
    "Austin, TX",
    "Seattle, WA",
    "San Francisco, CA",
    "Boston, MA",
    "Denver, CO",
    "Atlanta, GA",
    "Madison, WI",
]

tech_groups = [
    ["python", "sql", "postgres", "docker"],
    ["java", "spring", "aws", "kafka"],
    ["typescript", "react", "node", "docker"],
    ["python", "spark", "kafka", "aws"],
    ["python", "sql", "dbt", "airflow"],
    ["go", "kubernetes", "docker", "aws"],
    ["scala", "spark", "kafka", "postgres"],
    ["python", "react", "postgres", "docker"],
]

descriptions = [
    "Build and maintain distributed data services using {techs}.",
    "Develop internal analytics tooling and production services with {techs}.",
    "Own ETL and event-driven workflows using {techs}.",
    "Work on customer-facing features and backend APIs built with {techs}.",
    "Improve platform reliability, observability, and deployments with {techs}.",
]

def make_url(company: str, title: str, idx: int) -> str:
    company = company.lower().replace(" ", "-")
    title = title.lower().replace(" ", "-")
    return f"https://jobs.example.com/{company}/{title}-{idx}"

rows = []
for i in range(250):
    company = random.choice(companies)
    title = random.choice(titles)
    location = random.choice(locations)
    techs = random.choice(tech_groups)
    desc_template = random.choice(descriptions)

    row = {
        "company": company,
        "title": title,
        "location": location,
        "description": desc_template.format(techs=", ".join(techs)),
        "url": make_url(company, title, i),
    }
    rows.append(row)


with OUT_PATH.open("w", encoding="utf-8") as f:
    for row in rows:
        f.write(json.dumps(row) + "\n")

print(f"Wrote {len(rows)} rows to {OUT_PATH}")