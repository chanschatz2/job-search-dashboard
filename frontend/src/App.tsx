import { useEffect, useState } from "react";

type Job = {
  event_id: string;
  ingested_at: string;
  company?: string;
  title?: string;
  location?: string;
  seniority?: string;
  role_category?: string;
  techs?: any[];
};

const API_BASE = import.meta.env.VITE_API_BASE || "http://localhost:8000";

export default function App() {
  const [jobs, setJobs] = useState<Job[]>([]);
  const [error, setError] = useState<string | null>(null);

  const job_limit: number = 20;
  const job_offset: number = 0;

  useEffect(() => {
    fetch(`${API_BASE}/jobs?limit=${job_limit}&offset=${job_offset}`) // use route url query
      .then((r) => r.json())
      .then(setJobs)
      .catch((e) => setError(String(e)));
  }, []);

  return (
    <div style={{ padding: 16, fontFamily: "system-ui, sans-serif" }}>
      <h1>Job Market Trends Dashboard (MVP)</h1>
      <p>API: {API_BASE}</p>

      {error && <pre style={{ color: "crimson" }}>{error}</pre>}

      <h2>Recent Jobs</h2>
      <table cellPadding={8} style={{ borderCollapse: "collapse", width: "100%" }}>
        <thead>
          <tr>
            <th align="left">Time</th>
            <th align="left">Company</th>
            <th align="left">Title</th>
            <th align="left">Location</th>
          </tr>
        </thead>
        <tbody>
          {jobs.map((j) => (
            <tr key={j.event_id} style={{ borderTop: "1px solid #ddd" }}>
              <td>{new Date(j.ingested_at).toLocaleString()}</td>
              <td>{j.company ?? "-"}</td>
              <td>{j.title ?? "-"}</td>
              <td>{j.location ?? "-"}</td>
            </tr>
          ))}
        </tbody>
      </table>

      {jobs.length === 0 && <p>No jobs yet — ingestion/streaming comes Day 2.</p>}
    </div>
  );
}