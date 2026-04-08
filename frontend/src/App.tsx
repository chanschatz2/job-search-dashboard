import { useEffect, useMemo, useState } from "react";
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  Tooltip,
  CartesianGrid,
  ResponsiveContainer,
  LineChart,
  Line,
  Legend,
} from "recharts";

type Job = {
  event_id: string;
  ingested_at: string;
  company?: string;
  title?: string;
  location?: string;
  seniority?: string;
  role_category?: string;
  description?: string;
  url?: string;
  techs?: string[];
};

type TechTopRow = {
  window_start: string;
  window_end: string;
  window_size_sec: number;
  tech: string;
  count: number;
};

type TechTimeseriesRow = {
  window_start: string;
  window_end: string;
  window_size_sec: number;
  tech: string;
  count: number;
};

type RoleTopRow = {
  window_start: string;
  window_end: string;
  window_size_sec: number;
  role_category: string;
  count: number;
};

const API_BASE = import.meta.env.VITE_API_BASE || "http://localhost:8000";

export default function App() {
  const [jobs, setJobs] = useState<Job[]>([]);
  const [topTechs, setTopTechs] = useState<TechTopRow[]>([]);
  const [techTimeseries, setTechTimeseries] = useState<TechTimeseriesRow[]>([]);
  const [topRoles, setTopRoles] = useState<RoleTopRow[]>([]);

  const [selectedTech, setSelectedTech] = useState<string>("python");
  const [jobLocation, setJobLocation] = useState<string>("");
  const [jobRole, setJobRole] = useState<string>("");

  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<string | null>(null);

  const jobLimit = 20;
  const windowSizeSec = 300;

  async function fetchJson<T>(url: string): Promise<T> {
    const res = await fetch(url);
    if (!res.ok) {
      throw new Error(`Request failed: ${res.status} ${res.statusText}`);
    }
    return res.json();
  }

  async function loadDashboardData() {
    setLoading(true);
    setError(null);

    try {
      const jobsUrl = new URL(`${API_BASE}/jobs`);
      jobsUrl.searchParams.set("limit", String(jobLimit));
      jobsUrl.searchParams.set("offset", "0");
      if (jobLocation.trim()) {
        jobsUrl.searchParams.set("location", jobLocation.trim());
      }
      if (jobRole.trim()) {
        jobsUrl.searchParams.set("role_category", jobRole.trim());
      }

      const topTechUrl = new URL(`${API_BASE}/trends/tech/top`);
      topTechUrl.searchParams.set("window_size_sec", String(windowSizeSec));
      topTechUrl.searchParams.set("limit", "10");

      const topRoleUrl = new URL(`${API_BASE}/trends/roles/top`);
      topRoleUrl.searchParams.set("window_size_sec", String(windowSizeSec));
      topRoleUrl.searchParams.set("limit", "10");

      const [jobsData, topTechData, topRoleData] = await Promise.all([
        fetchJson<Job[]>(jobsUrl.toString()),
        fetchJson<TechTopRow[]>(topTechUrl.toString()),
        fetchJson<RoleTopRow[]>(topRoleUrl.toString()),
      ]);

      setJobs(jobsData);
      setTopTechs(topTechData);
      setTopRoles(topRoleData);

      const nextSelectedTech =
        selectedTech ||
        topTechData[0]?.tech ||
        "python";

      setSelectedTech(nextSelectedTech);

      const techSeriesUrl = new URL(`${API_BASE}/trends/tech/timeseries`);
      techSeriesUrl.searchParams.set("tech", nextSelectedTech);
      techSeriesUrl.searchParams.set("window_size_sec", String(windowSizeSec));
      techSeriesUrl.searchParams.set("limit", "100");

      const techSeriesData = await fetchJson<TechTimeseriesRow[]>(
        techSeriesUrl.toString()
      );
      setTechTimeseries(techSeriesData);
    } catch (e) {
      setError(String(e));
    } finally {
      setLoading(false);
    }
  }

  async function loadTechSeries(tech: string) {
    setError(null);
    try {
      const techSeriesUrl = new URL(`${API_BASE}/trends/tech/timeseries`);
      techSeriesUrl.searchParams.set("tech", tech);
      techSeriesUrl.searchParams.set("window_size_sec", String(windowSizeSec));
      techSeriesUrl.searchParams.set("limit", "100");

      const data = await fetchJson<TechTimeseriesRow[]>(techSeriesUrl.toString());
      setSelectedTech(tech);
      setTechTimeseries(data);
    } catch (e) {
      setError(String(e));
    }
  }

  useEffect(() => {
    loadDashboardData();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const techChartData = useMemo(
    () =>
      topTechs.map((row) => ({
        tech: row.tech,
        count: row.count,
      })),
    [topTechs]
  );

  const roleChartData = useMemo(
    () =>
      topRoles.map((row) => ({
        role_category: row.role_category,
        count: row.count,
      })),
    [topRoles]
  );

  const techSeriesChartData = useMemo(
    () =>
      techTimeseries.map((row) => ({
        window_start: new Date(row.window_start).toLocaleTimeString(),
        count: row.count,
      })),
    [techTimeseries]
  );

  return (
    <div style={{ padding: 24, fontFamily: "system-ui, sans-serif" }}>
      <h1>MarketPulse Dashboard</h1>
      <p>Streaming job market trends from Kafka → Spark → Postgres</p>

      <div style={{ marginBottom: 16 }}>
        <button onClick={loadDashboardData}>Refresh Dashboard</button>
      </div>

      <div
        style={{
          display: "flex",
          gap: 12,
          marginBottom: 20,
          flexWrap: "wrap",
        }}
      >
        <div>
          <label>
            Location:{" "}
            <input
              value={jobLocation}
              onChange={(e) => setJobLocation(e.target.value)}
              placeholder="e.g. Chicago"
            />
          </label>
        </div>

        <div>
          <label>
            Role Category:{" "}
            <input
              value={jobRole}
              onChange={(e) => setJobRole(e.target.value)}
              placeholder="e.g. Backend"
            />
          </label>
        </div>

        <div>
          <button onClick={loadDashboardData}>Apply Filters</button>
        </div>
      </div>

      {loading && <p>Loading dashboard...</p>}
      {error && <pre style={{ color: "crimson", whiteSpace: "pre-wrap" }}>{error}</pre>}

      <div
        style={{
          display: "grid",
          gridTemplateColumns: "1fr 1fr",
          gap: 24,
          marginBottom: 32,
        }}
      >
        <div style={{ minHeight: 320 }}>
          <h2>Top Technologies</h2>
          <ResponsiveContainer width="100%" height={300}>
            <BarChart data={techChartData}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="tech" />
              <YAxis />
              <Tooltip />
              <Bar dataKey="count" />
            </BarChart>
          </ResponsiveContainer>

          <div style={{ marginTop: 12 }}>
            <label>
              Selected Tech:{" "}
              <select
                value={selectedTech}
                onChange={(e) => loadTechSeries(e.target.value)}
              >
                {topTechs.map((row) => (
                  <option key={row.tech} value={row.tech}>
                    {row.tech}
                  </option>
                ))}
              </select>
            </label>
          </div>
        </div>

        <div style={{ minHeight: 320 }}>
          <h2>Top Role Categories</h2>
          <ResponsiveContainer width="100%" height={300}>
            <BarChart data={roleChartData}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="role_category" />
              <YAxis />
              <Tooltip />
              <Bar dataKey="count" />
            </BarChart>
          </ResponsiveContainer>
        </div>
      </div>

      <div style={{ marginBottom: 32 }}>
        <h2>{selectedTech} Trend Over Time</h2>
        <ResponsiveContainer width="100%" height={320}>
          <LineChart data={techSeriesChartData}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="window_start" />
            <YAxis />
            <Tooltip />
            <Legend />
            <Line type="monotone" dataKey="count" name={selectedTech} />
          </LineChart>
        </ResponsiveContainer>
      </div>

      <div>
        <h2>Recent Jobs</h2>
        <table
          cellPadding={8}
          style={{ borderCollapse: "collapse", width: "100%" }}
        >
          <thead>
            <tr>
              <th align="left">Time</th>
              <th align="left">Company</th>
              <th align="left">Title</th>
              <th align="left">Location</th>
              <th align="left">Role</th>
            </tr>
          </thead>
          <tbody>
            {jobs.map((j) => (
              <tr key={j.event_id} style={{ borderTop: "1px solid #ddd" }}>
                <td>{new Date(j.ingested_at).toLocaleString()}</td>
                <td>{j.company ?? "-"}</td>
                <td>{j.title ?? "-"}</td>
                <td>{j.location ?? "-"}</td>
                <td>{j.role_category ?? "-"}</td>
              </tr>
            ))}
          </tbody>
        </table>

        {jobs.length === 0 && !loading && <p>No jobs found.</p>}
      </div>
    </div>
  );
}