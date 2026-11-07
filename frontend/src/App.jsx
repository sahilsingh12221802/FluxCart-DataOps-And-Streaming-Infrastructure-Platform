import { useCallback, useEffect, useMemo, useState } from "react";
import {
  Activity,
  AlertTriangle,
  ArrowDownRight,
  ArrowUpRight,
  Boxes,
  Check,
  ChevronRight,
  CircleHelp,
  Clock3,
  Database,
  Download,
  ExternalLink,
  FileCheck2,
  FileText,
  Gauge,
  Layers3,
  LayoutDashboard,
  RefreshCw,
  Search,
  Server,
  ShieldCheck,
  ShoppingCart,
  Workflow,
  Zap,
} from "lucide-react";
import {
  Area,
  AreaChart,
  Bar,
  BarChart,
  CartesianGrid,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import "./App.css";

const API = "http://localhost:8000";

const NAV = [
  {
    id: "overview",
    label: "Overview",
    icon: LayoutDashboard,
    group: "WORKSPACE",
  },
  {
    id: "pipeline",
    label: "Streaming pipeline",
    icon: Workflow,
    group: "WORKSPACE",
  },
  { id: "lake", label: "Data lake", icon: Database, group: "DATA MANAGEMENT" },
  {
    id: "quality",
    label: "Data quality",
    icon: ShieldCheck,
    group: "DATA MANAGEMENT",
  },
];

const hasEventTimestamp = (event) =>
  Boolean(event.timestamp || event.event_timestamp);

const money = (n) =>
  new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "INR",
    maximumFractionDigits: 2,
  }).format(n || 0);

const number = (n) => new Intl.NumberFormat("en-US").format(n || 0);

function Status({ ok, children }) {
  return (
    <span className={`status ${ok ? "status-ok" : "status-warn"}`}>
      <span className="status-dot" />
      {children}
    </span>
  );
}

function Metric({ icon: Icon, label, value, note, tone = "blue" }) {
  return (
    <article className="metric-card">
      <div className="metric-top">
        <span className={`metric-icon ${tone}`}>
          <Icon size={19} />
        </span>
        <span className="metric-label">{label}</span>
      </div>
      <div className="metric-value">{value}</div>
      <div className="metric-note">{note}</div>
    </article>
  );
}

function Panel({ title, subtitle, action, children, className = "" }) {
  return (
    <section className={`panel ${className}`}>
      <div className="panel-head">
        <div>
          <h3>{title}</h3>
          {subtitle && <p>{subtitle}</p>}
        </div>
        {action}
      </div>
      {children}
    </section>
  );
}

function Empty({ icon: Icon = Database, title, detail }) {
  return (
    <div className="empty-state">
      <span>
        <Icon size={23} />
      </span>
      <strong>{title}</strong>
      <p>{detail}</p>
    </div>
  );
}

function timeLabel(value) {
  if (!value) return "Not provided";
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? String(value) : date.toLocaleString();
}

export default function App() {
  const [page, setPage] = useState("overview");
  const [health, setHealth] = useState(null);
  const [overview, setOverview] = useState(null);
  const [types, setTypes] = useState([]);
  const [events, setEvents] = useState([]);
  const [quarantine, setQuarantine] = useState({
    total_files: 0,
    total_records: 0,
    reason_counts: [],
    recent_records: [],
    status: "loading",
  });
  const [quality, setQuality] = useState(null);
  const [analytics, setAnalytics] = useState(null);
  const [streaming, setStreaming] = useState(null);
  const [streamingHistory, setStreamingHistory] = useState([]);
  const [kafka, setKafka] = useState(null);
  const [query, setQuery] = useState("");
  const [loading, setLoading] = useState(true);
  const [lastUpdated, setLastUpdated] = useState(null);
  const [error, setError] = useState("");

  const refresh = useCallback(async (initial = false) => {
    if (initial) setLoading(true);
    try {
      const paths = [
        "/api/health",
        "/api/overview",
        "/api/event-types",
        "/api/events?limit=500",
        "/api/quarantine?limit=10",
        "/api/quality",
        "/api/analytics",
        "/api/streaming",
        "/api/streaming/history?limit=500",
        "/api/kafka",
      ];
      const responses = await Promise.all(
        paths.map((path) => fetch(`${API}${path}`)),
      );
      if (responses.some((response) => !response.ok))
        throw new Error("One or more API requests failed.");
      const [
        h,
        o,
        t,
        e,
        q,
        qualityData,
        analyticsData,
        streamingData,
        historyData,
        kafkaData,
      ] = await Promise.all(responses.map((response) => response.json()));

      setHealth(h);
      setOverview(o);
      setTypes(t.event_types || []);
      setEvents(e.events || []);
      setQuarantine(q);
      setQuality(qualityData);
      setAnalytics(analyticsData);
      setStreaming(streamingData);
      setStreamingHistory(historyData.records || []);
      setKafka(kafkaData);

      setError(
        [
          o.error,
          t.error,
          e.error,
          q.error,
          qualityData.error,
          analyticsData.error,
          streamingData.error,
          historyData.error,
          kafkaData.error,
        ]
          .filter(Boolean)
          .join(" · "),
      );
      setLastUpdated(new Date());
    } catch {
      setError(
        "Could not connect to FluxCart API. Confirm the backend is running on port 8000.",
      );
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    refresh(true);
    const timer = setInterval(() => refresh(), 10000);
    return () => clearInterval(timer);
  }, [refresh]);

  const filteredEvents = useMemo(() => {
    const term = query.trim().toLowerCase();
    if (!term) return events;
    return events.filter((event) =>
      [event.event_id, event.event_type, event.timestamp].some((value) =>
        String(value ?? "")
          .toLowerCase()
          .includes(term),
      ),
    );
  }, [events, query]);

  const totalSampled = types.reduce((sum, item) => sum + item.count, 0);
  const purchases = analytics?.purchase_count ?? 0;
  const revenue = analytics?.revenue ?? 0;
  const revenueEvents = purchases;

  const purchaseRate = analytics?.purchase_share ?? 0;

  const chartData = (analytics?.event_distribution || []).map((item) => ({
    name: item.name.replaceAll("_", " "),
    events: item.count,
  }));

  const historicalThroughput = useMemo(() => {
    const grouped = new Map();

    streamingHistory.forEach((record) => {
      const timestamp = new Date(record.recorded_at).getTime();

      if (!Number.isFinite(timestamp)) return;

      // Align measurements collected within the same 10-second window.
      const bucket = Math.round(timestamp / 10000) * 10000;

      if (!grouped.has(bucket)) {
        grouped.set(bucket, {
          time: new Date(bucket).toLocaleTimeString([], {
            hour: "2-digit",
            minute: "2-digit",
            second: "2-digit",
          }),
          timestamp: bucket,
        });
      }

      const point = grouped.get(bucket);

      if (record.query_name === "fluxcart_raw_events") {
        point.raw = record.processed_rate;
      }

      if (record.query_name === "fluxcart_quarantined_events") {
        point.quarantine = record.processed_rate;
      }
    });

    return Array.from(grouped.values()).sort(
      (a, b) => a.timestamp - b.timestamp,
    );
  }, [streamingHistory]);

  const revenueTrendData = (analytics?.revenue_trend || []).map((item) => ({
    hour: new Date(item.hour).toLocaleString([], {
      month: "short",
      day: "numeric",
      hour: "numeric",
    }),
    revenue: item.revenue,
  }));

  const trendData = (analytics?.event_trend || []).map((item) => ({
    hour: new Date(item.hour).toLocaleString([], {
      month: "short",
      day: "numeric",
      hour: "numeric",
    }),
    events: item.events,
  }));

  const storageHealthy = health?.minio === "healthy";
  const apiHealthy = health?.api === "healthy";

  const currentPage = NAV.find((item) => item.id === page);
  const rawFiles = overview?.parquet_files ?? 0;
  const quarantineFiles = overview?.quarantine_files ?? 0;
  const rawStream = streaming?.queries?.find(
    (q) => q.name === "fluxcart_raw_events",
  );

  const quarantineStream = streaming?.queries?.find(
    (q) => q.name === "fluxcart_quarantined_events",
  );

  const streamingHealthy =
    streaming?.status === "connected" &&
    streaming.queries?.length === 2 &&
    streaming.queries.every((q) => {
      const age = q.last_batch_timestamp
        ? Date.now() - new Date(q.last_batch_timestamp).getTime()
        : Infinity;

      return q.is_active && age >= 0 && age < 30000;
    });

  const kafkaHealthy =
    kafka?.status === "connected" && kafka?.broker_status === "healthy";

  const platformHealthy =
    apiHealthy && storageHealthy && kafkaHealthy && streamingHealthy;

  const streamingChartData = [
    {
      name: "Raw events",
      input: rawStream?.input_rows_per_second ?? 0,
      processed: rawStream?.processed_rows_per_second ?? 0,
    },
    {
      name: "Quarantine",
      input: quarantineStream?.input_rows_per_second ?? 0,
      processed: quarantineStream?.processed_rows_per_second ?? 0,
    },
  ];

  return (
    <div className="app-shell">
      <aside className="sidebar">
        <button className="brand" onClick={() => setPage("overview")}>
          <span className="brand-symbol">
            <Activity size={22} />
          </span>
          <span className="brand-copy">
            <strong>FluxCart</strong>
            <small>DATA PLATFORM</small>
          </span>
        </button>

        <div className="workspace-switch">
          <span className="workspace-icon">F</span>
          <span>
            <strong>FluxCart workspace</strong>
            <small>Development environment</small>
          </span>
          <ChevronRight size={15} />
        </div>

        {["WORKSPACE", "DATA MANAGEMENT"].map((group) => (
          <div className="nav-group" key={group}>
            <div className="nav-heading">{group}</div>
            {NAV.filter((item) => item.group === group).map(
              ({ id, label, icon: Icon }) => (
                <button
                  key={id}
                  onClick={() => setPage(id)}
                  className={`nav-link ${page === id ? "selected" : ""}`}
                >
                  <Icon size={18} />
                  <span>{label}</span>
                  {id === "quality" && quarantineFiles > 0 && (
                    <span className="nav-count">{quarantineFiles}</span>
                  )}
                </button>
              ),
            )}
          </div>
        ))}

        <div className="sidebar-spacer" />
        <div className="sidebar-health">
          <div className="health-heading">
            <span className="live-pulse" /> ENVIRONMENT STATUS
          </div>
          <div className="health-line">
            <span>API service</span>
            <Status ok={apiHealthy}>{apiHealthy ? "Online" : "Offline"}</Status>
          </div>
          <div className="health-line">
            <span>Object storage</span>
            <Status ok={storageHealthy}>
              {storageHealthy ? "Online" : "Check"}
            </Status>
          </div>
        </div>
        <div className="sidebar-footer">
          <span className="avatar">FC</span>
          <span>
            <strong>FluxCart</strong>
            <small>Streaming analytics</small>
          </span>
        </div>
      </aside>

      <main className="main-area">
        <header className="topbar">
          <div className="breadcrumbs">
            <span>FluxCart</span>
            <ChevronRight size={14} />
            <strong>{currentPage?.label || "Overview"}</strong>
          </div>
          <div className="top-actions">
            <span className="refresh-time">
              <span className="live-pulse" /> Auto-refresh · 10s
            </span>
            <button
              className="icon-button"
              title="Refresh data"
              onClick={() => refresh()}
            >
              <RefreshCw size={17} />
            </button>
            <a
              className="docs-button"
              href={`${API}/docs`}
              target="_blank"
              rel="noreferrer"
            >
              API docs <ExternalLink size={13} />
            </a>
          </div>
        </header>

        <div className="page-content">
          <div className="page-title-row">
            <div>
              <div className="eyebrow">REAL-TIME E-COMMERCE INTELLIGENCE</div>
              <h1>{currentPage?.label || "Overview"}</h1>
              <p className="page-description">
                {page === "overview" &&
                  "Your streaming data, business activity, and infrastructure at a glance."}
                {page === "pipeline" &&
                  "Explore the data flow from event generation to object storage."}
                {page === "lake" &&
                  "Inspect the raw datasets persisted by your streaming pipeline."}
                {page === "quality" &&
                  "Monitor quarantined data and the quality signals currently available."}
              </p>
            </div>
            <button className="primary-button" onClick={() => refresh()}>
              <RefreshCw size={16} /> Refresh data
            </button>
          </div>

          {error && (
            <div className="notice">
              <AlertTriangle size={17} />
              <span>{error}</span>
            </div>
          )}

          {page === "overview" && (
            <div className="page-stack">
              <div className="connection-banner">
                <span className="connection-icon">
                  <Check size={21} />
                </span>
                <div className="connection-copy">
                  <strong>
                    {storageHealthy
                      ? "Object storage is connected"
                      : "Checking object storage"}
                  </strong>
                  <span>
                    {overview?.bucket
                      ? `Bucket · ${overview.bucket}`
                      : "Waiting for bucket details"}
                  </span>
                </div>
                <Status ok={storageHealthy}>
                  {storageHealthy ? "OPERATIONAL" : "CHECK REQUIRED"}
                </Status>
              </div>

              <div className="section-title">
                <h2>Platform snapshot</h2>
                <span>Based on records available through the API</span>
              </div>
              <div className="metric-grid">
                <Metric
                  icon={Activity}
                  label="Events analyzed"
                  value={number(totalSampled)}
                  note="Sampled from readable Parquet data"
                  tone="blue"
                />
                <Metric
                  icon={ShoppingCart}
                  label="Purchase events"
                  value={number(purchases)}
                  note="In the 2,000-record analytics sample"
                  tone="violet"
                />
                <Metric
                  icon={Gauge}
                  label="Purchase event share"
                  value={`${purchaseRate.toFixed(1)}%`}
                  note="Purchases ÷ sampled event counts"
                  tone="green"
                />
                <Metric
                  icon={ShoppingCart}
                  label="Purchase revenue"
                  value={money(revenue)}
                  note="Total revenue in sampled purchase events"
                  tone="green"
                />
                <Metric
                  icon={Gauge}
                  label="Average purchase value"
                  value={money(analytics?.average_purchase_value || 0)}
                  note="Revenue divided by purchase events"
                  tone="violet"
                />
                <Metric
                  icon={Database}
                  label="Raw data stored"
                  value={`${(Number(overview?.raw_bytes || 0) / 1048576).toFixed(1)} MB`}
                  note={`${number(rawFiles)} Parquet files`}
                  tone="orange"
                />
              </div>

              <div className="content-grid">
                <Panel
                  title="Event distribution"
                  subtitle="Frequency by event type in the sampled data"
                  action={
                    <span className="subtle-tag">
                      <Activity size={13} /> Event analytics
                    </span>
                  }
                >
                  {chartData.length ? (
                    <div className="chart-area">
                      <ResponsiveContainer width="100%" height="100%">
                        <BarChart
                          data={chartData}
                          margin={{ top: 10, right: 10, left: -15, bottom: 35 }}
                        >
                          <CartesianGrid
                            stroke="#edf0f6"
                            strokeDasharray="4 4"
                            vertical={false}
                          />
                          <XAxis
                            dataKey="name"
                            tick={{ fill: "#778198", fontSize: 10 }}
                            angle={-22}
                            textAnchor="end"
                            interval={0}
                            axisLine={false}
                            tickLine={false}
                          />
                          <YAxis
                            allowDecimals={false}
                            tick={{ fill: "#778198", fontSize: 11 }}
                            axisLine={false}
                            tickLine={false}
                          />
                          <Tooltip
                            cursor={{ fill: "#f3f5ff" }}
                            contentStyle={{
                              borderRadius: 10,
                              border: "1px solid #e5e9f2",
                              fontSize: 12,
                            }}
                          />
                          <Bar
                            dataKey="events"
                            name="Events"
                            fill="#6076e9"
                            radius={[5, 5, 0, 0]}
                            maxBarSize={42}
                          />
                        </BarChart>
                      </ResponsiveContainer>
                    </div>
                  ) : (
                    <Empty
                      title="No event data yet"
                      detail="The chart will populate when event records are available."
                    />
                  )}
                </Panel>

                <Panel
                  title="Infrastructure health"
                  subtitle="Live API and object storage checks"
                >
                  <div className="service-list">
                    <div className="service-row">
                      <span className="service-icon">
                        <Server size={17} />
                      </span>
                      <span className="service-copy">
                        <strong>FluxCart API</strong>
                        <small>FastAPI service</small>
                      </span>
                      <Status ok={apiHealthy}>
                        {apiHealthy ? "Healthy" : "Unavailable"}
                      </Status>
                    </div>
                    <div className="service-row">
                      <span className="service-icon">
                        <Database size={17} />
                      </span>
                      <span className="service-copy">
                        <strong>MinIO storage</strong>
                        <small>
                          {overview?.bucket || "Object storage bucket"}
                        </small>
                      </span>
                      <Status ok={storageHealthy}>
                        {storageHealthy ? "Healthy" : "Unavailable"}
                      </Status>
                    </div>
                    <div className="service-row">
                      <span className="service-icon">
                        <Layers3 size={17} />
                      </span>
                      <span className="service-copy">
                        <strong>Raw dataset</strong>
                        <small>Parquet output files</small>
                      </span>
                      <Status ok={overview?.status === "connected"}>
                        {overview?.status === "connected"
                          ? "Available"
                          : "Check"}
                      </Status>
                    </div>
                  </div>
                  <div className="storage-card">
                    <div>
                      <span>Raw data footprint</span>
                      <strong>
                        {(Number(overview?.raw_bytes || 0) / 1048576).toFixed(
                          2,
                        )}{" "}
                        MB
                      </strong>
                    </div>
                    <div className="storage-track">
                      <span
                        style={{ width: overview?.raw_bytes ? "100%" : "0%" }}
                      />
                    </div>
                    <small>
                      Reported object size · not capacity utilization
                    </small>
                  </div>
                </Panel>
              </div>

              <Panel
                title="Event ingestion trend"
                subtitle="Hourly event distribution from the sampled raw dataset"
                action={<span className="subtle-tag">Historical sample</span>}
              >
                {trendData.length ? (
                  <div className="chart-area">
                    <ResponsiveContainer width="100%" height="100%">
                      <AreaChart data={trendData}>
                        <defs>
                          <linearGradient
                            id="eventGradient"
                            x1="0"
                            y1="0"
                            x2="0"
                            y2="1"
                          >
                            <stop
                              offset="0%"
                              stopColor="#6076e9"
                              stopOpacity={0.3}
                            />
                            <stop
                              offset="95%"
                              stopColor="#6076e9"
                              stopOpacity={0.02}
                            />
                          </linearGradient>
                        </defs>
                        <CartesianGrid stroke="#edf0f6" strokeDasharray="4 4" />
                        <XAxis dataKey="hour" tick={{ fontSize: 11 }} />
                        <YAxis allowDecimals={false} />
                        <Tooltip />
                        <Area
                          type="monotone"
                          dataKey="events"
                          stroke="#6076e9"
                          fill="url(#eventGradient)"
                          strokeWidth={2}
                        />
                      </AreaChart>
                    </ResponsiveContainer>
                  </div>
                ) : (
                  <Empty
                    icon={Activity}
                    title="No trend data available"
                    detail="Event timestamps are required to calculate hourly trends."
                  />
                )}
              </Panel>

              <Panel
                title="Revenue trend"
                subtitle="Hourly purchase revenue from sampled records"
                action={
                  <span className="subtle-tag">Historical sample · INR</span>
                }
              >
                {revenueTrendData.length ? (
                  <div className="chart-area">
                    <ResponsiveContainer width="100%" height="100%">
                      <AreaChart data={revenueTrendData}>
                        <defs>
                          <linearGradient
                            id="revenueGradient"
                            x1="0"
                            y1="0"
                            x2="0"
                            y2="1"
                          >
                            <stop
                              offset="0%"
                              stopColor="#16a36a"
                              stopOpacity={0.3}
                            />
                            <stop
                              offset="95%"
                              stopColor="#16a36a"
                              stopOpacity={0.02}
                            />
                          </linearGradient>
                        </defs>
                        <CartesianGrid stroke="#edf0f6" strokeDasharray="4 4" />
                        <XAxis dataKey="hour" tick={{ fontSize: 11 }} />
                        <YAxis
                          tickFormatter={(value) =>
                            `₹${(value / 100000).toFixed(0)}L`
                          }
                        />
                        <Tooltip
                          formatter={(value) => [money(value), "Revenue"]}
                        />
                        <Area
                          type="monotone"
                          dataKey="revenue"
                          stroke="#16a36a"
                          fill="url(#revenueGradient)"
                          strokeWidth={2}
                        />
                      </AreaChart>
                    </ResponsiveContainer>
                  </div>
                ) : (
                  <Empty
                    icon={Activity}
                    title="No revenue trend available"
                    detail="Purchase timestamps are required for hourly revenue analytics."
                  />
                )}
              </Panel>

              <Panel
                title="Recent event records"
                subtitle={`${number(events.length)} records returned by the API`}
                action={
                  <button
                    className="text-button"
                    onClick={() => setPage("lake")}
                  >
                    Explore data lake <ChevronRight size={14} />
                  </button>
                }
              >
                <div className="table-tools">
                  <label className="search-box">
                    <Search size={15} />
                    <input
                      value={query}
                      onChange={(e) => setQuery(e.target.value)}
                      placeholder="Search event ID or type..."
                    />
                  </label>
                  <span className="muted-text">Showing up to 10 rows</span>
                </div>
                <EventTable events={filteredEvents.slice(0, 10)} />
              </Panel>
            </div>
          )}

          {page === "pipeline" && (
            <div className="page-stack">
              <div className="connection-banner">
                <span className="connection-icon">
                  <Workflow size={21} />
                </span>
                <div className="connection-copy">
                  <strong>Streaming architecture</strong>
                  <span>
                    Event ingestion → stream processing → object storage
                  </span>
                </div>
                <span className="subtle-tag">Architecture view</span>
              </div>
              <Panel
                title="Data processing flow"
                subtitle="FluxCart pipeline components"
              >
                <div className="pipeline-flow">
                  {[
                    {
                      n: "01",
                      title: "Event simulator",
                      sub: "Synthetic e-commerce events",
                      icon: Zap,
                      cls: "flow-purple",
                    },
                    {
                      n: "02",
                      title: "Apache Kafka",
                      sub: "Topic: ecommerce-events",
                      icon: Activity,
                      cls: "flow-blue",
                    },
                    {
                      n: "03",
                      title: "Apache Spark",
                      sub: "Structured Streaming",
                      icon: Workflow,
                      cls: "flow-orange",
                    },
                    {
                      n: "04",
                      title: "MinIO data lake",
                      sub: "Parquet objects on S3 API",
                      icon: Database,
                      cls: "flow-green",
                    },
                  ].map((node, i) => (
                    <div className="flow-unit" key={node.n}>
                      <div className={`flow-node ${node.cls}`}>
                        <node.icon size={24} />
                        <span>{node.n}</span>
                      </div>
                      <strong>{node.title}</strong>
                      <small>{node.sub}</small>
                      {i < 3 && (
                        <ChevronRight className="flow-arrow" size={21} />
                      )}
                    </div>
                  ))}
                </div>
                <div className="pipeline-note">
                  <CircleHelp size={16} />
                  <span>
                    Live monitoring is connected to Apache Kafka and Spark
                    Structured Streaming through the FluxCart API.
                  </span>
                </div>
              </Panel>

              <Panel
                title="Infrastructure health"
                subtitle="Unified status of all FluxCart platform services"
                action={
                  <Status ok={platformHealthy}>
                    {platformHealthy
                      ? "All systems operational"
                      : "Attention required"}
                  </Status>
                }
              >
                <div className="service-list">
                  {[
                    {
                      name: "FastAPI backend",
                      detail: "REST API · Port 8000",
                      ok: apiHealthy,
                    },
                    {
                      name: "Apache Kafka",
                      detail: `${kafka?.broker_count ?? 0} broker(s) · ${kafka?.topic || "ecommerce-events"}`,
                      ok: kafkaHealthy,
                    },
                    {
                      name: "Apache Spark",
                      detail: "Structured Streaming · 2 queries",
                      ok: streamingHealthy,
                    },
                    {
                      name: "MinIO object storage",
                      detail: `Bucket · ${overview?.bucket || "fluxcart-raw"}`,
                      ok: storageHealthy,
                    },
                  ].map((service) => (
                    <div className="service-row" key={service.name}>
                      <span className="service-icon">
                        <Server size={17} />
                      </span>
                      <span className="service-copy">
                        <strong>{service.name}</strong>
                        <small>{service.detail}</small>
                      </span>
                      <Status ok={service.ok}>
                        {service.ok ? "Healthy" : "Unavailable"}
                      </Status>
                    </div>
                  ))}
                </div>

                <div className="pipeline-note">
                  <Activity size={16} />
                  <span>
                    Platform health is evaluated using live API, Kafka, Spark
                    and MinIO checks.
                  </span>
                </div>
              </Panel>

              <Panel
                title="Apache Kafka monitor"
                subtitle={`Topic: ${kafka?.topic || "Connecting..."}`}
                action={
                  <Status ok={kafkaHealthy}>
                    {kafkaHealthy ? "Broker healthy" : "Unavailable"}
                  </Status>
                }
              >
                {!kafkaHealthy ? (
                  <Empty
                    icon={Activity}
                    title="Kafka metrics unavailable"
                    detail={
                      kafka?.error ||
                      "Waiting for Kafka broker and topic metadata."
                    }
                  />
                ) : (
                  <>
                    <div className="metric-grid">
                      <Metric
                        icon={Server}
                        label="Active brokers"
                        value={number(kafka.broker_count)}
                        note="Connected Kafka brokers"
                        tone="green"
                      />
                      <Metric
                        icon={Layers3}
                        label="Topic partitions"
                        value={number(kafka.partition_count)}
                        note={kafka.topic}
                        tone="blue"
                      />
                      <Metric
                        icon={Database}
                        label="Retained messages"
                        value={number(kafka.total_retained_messages)}
                        note="Messages currently retained in Kafka"
                        tone="violet"
                      />
                    </div>

                    <div className="section-title" style={{ marginTop: 24 }}>
                      <h2>Partition monitoring</h2>
                      <span>Live Kafka partition metadata</span>
                    </div>

                    <div className="table-wrap">
                      <table className="event-table">
                        <thead>
                          <tr>
                            <th>Partition</th>
                            <th>Leader</th>
                            <th>Replicas</th>
                            <th>In-sync replicas</th>
                            <th>Retained messages</th>
                          </tr>
                        </thead>
                        <tbody>
                          {(kafka.partitions || []).map((partition) => (
                            <tr key={partition.partition}>
                              <td>{partition.partition}</td>
                              <td>{partition.leader ?? "—"}</td>
                              <td>{partition.replicas?.join(", ") || "—"}</td>
                              <td>
                                {partition.in_sync_replicas?.join(", ") || "—"}
                              </td>
                              <td>{number(partition.retained_messages)}</td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>

                    <div className="pipeline-note">
                      <Clock3 size={16} />
                      <span>
                        Last Kafka metadata update:{" "}
                        {timeLabel(kafka.updated_at)}
                      </span>
                    </div>
                  </>
                )}
              </Panel>

              <div className="metric-grid">
                <Metric
                  icon={Activity}
                  label="Sampled events"
                  value={number(totalSampled)}
                  note="API event sample"
                />
                <Metric
                  icon={Database}
                  label="Raw output files"
                  value={number(rawFiles)}
                  note="Parquet objects in MinIO"
                  tone="green"
                />
                <Metric
                  icon={AlertTriangle}
                  label="Quarantine files"
                  value={number(quarantineFiles)}
                  note="Quarantine Parquet objects"
                  tone="orange"
                />
                <Metric
                  icon={Server}
                  label="Storage API"
                  value={storageHealthy ? "Healthy" : "Check"}
                  note="MinIO connectivity probe"
                  tone="violet"
                />
                <Metric
                  icon={ArrowDownRight}
                  label="Raw input rate"
                  value={`${(rawStream?.input_rows_per_second ?? 0).toFixed(2)}/s`}
                  note="Incoming Kafka records"
                  tone="blue"
                />

                <Metric
                  icon={ArrowDownRight}
                  label="Quarantine input rate"
                  value={`${(quarantineStream?.input_rows_per_second ?? 0).toFixed(2)}/s`}
                  note="Incoming Kafka records"
                  tone="orange"
                />
              </div>

              <Panel
                title="Spark streaming monitor"
                subtitle={`Application: ${streaming?.application || "Connecting..."}`}
                action={
                  <Status ok={streamingHealthy}>
                    {streamingHealthy
                      ? "All queries running"
                      : "Check connection"}
                  </Status>
                }
              >
                {!streaming?.queries?.length ? (
                  <Empty
                    icon={Activity}
                    title="Streaming metrics unavailable"
                    detail="Waiting for Spark monitoring data."
                  />
                ) : (
                  <>
                    <div className="metric-grid">
                      <Metric
                        icon={Activity}
                        label="Active queries"
                        value={`${streaming.queries.filter((q) => q.is_active).length}/2`}
                        note="Spark Structured Streaming"
                        tone="green"
                      />
                      <Metric
                        icon={Zap}
                        label="Raw processing rate"
                        value={`${(rawStream?.processed_rows_per_second ?? 0).toFixed(2)}/s`}
                        note="Rows processed per second"
                        tone="blue"
                      />
                      <Metric
                        icon={Database}
                        label="Quarantine processing"
                        value={`${(quarantineStream?.processed_rows_per_second ?? 0).toFixed(2)}/s`}
                        note="Rows processed per second"
                        tone="orange"
                      />
                      <Metric
                        icon={Clock3}
                        label="Latest batch duration"
                        value={`${rawStream?.batch_duration_ms ?? 0} ms`}
                        note="Raw stream latest batch"
                        tone="violet"
                      />
                    </div>

                    <div className="chart-wrap" style={{ marginTop: 24 }}>
                      <div className="chart-heading">
                        <strong>Streaming throughput</strong>
                        <span>Rows per second · Latest batch</span>
                      </div>
                      <ResponsiveContainer width="100%" height={260}>
                        <BarChart data={streamingChartData}>
                          <CartesianGrid
                            strokeDasharray="3 3"
                            vertical={false}
                          />
                          <XAxis dataKey="name" />
                          <YAxis />
                          <Tooltip />
                          <Bar
                            dataKey="input"
                            name="Input rows/sec"
                            fill="#60a5fa"
                            radius={[4, 4, 0, 0]}
                          />
                          <Bar
                            dataKey="processed"
                            name="Processed rows/sec"
                            fill="#34d399"
                            radius={[4, 4, 0, 0]}
                          />
                        </BarChart>
                      </ResponsiveContainer>
                    </div>

                    <Panel
                      title="Historical streaming throughput"
                      subtitle={`${streamingHistory.length} recorded batch snapshots`}
                    >
                      {historicalThroughput.length < 2 ? (
                        <Empty
                          icon={Activity}
                          title="Collecting historical metrics"
                          detail="More Spark batches are needed to display a throughput trend."
                        />
                      ) : (
                        <div className="chart-wrap">
                          <ResponsiveContainer width="100%" height={300}>
                            <AreaChart data={historicalThroughput}>
                              <CartesianGrid
                                strokeDasharray="3 3"
                                vertical={false}
                              />
                              <XAxis dataKey="time" />
                              <YAxis />
                              <Tooltip />
                              <Area
                                type="monotone"
                                dataKey="raw"
                                name="Raw processed rows/sec"
                                stroke="#3b82f6"
                                fill="#3b82f6"
                                fillOpacity={0.1}
                              />
                              <Area
                                type="monotone"
                                dataKey="quarantine"
                                name="Quarantine processed rows/sec"
                                stroke="#f59e0b"
                                fill="#f59e0b"
                                fillOpacity={0.1}
                              />
                            </AreaChart>
                          </ResponsiveContainer>
                        </div>
                      )}
                    </Panel>

                    <div className="table-wrap" style={{ marginTop: 24 }}>
                      <table className="event-table">
                        <thead>
                          <tr>
                            <th>Streaming query</th>
                            <th>Status</th>
                            <th>Batch ID</th>
                            <th>Input rows</th>
                            <th>Kafka lag</th>
                          </tr>
                        </thead>
                        <tbody>
                          {streaming.queries.map((q) => (
                            <tr key={q.id}>
                              <td>{q.name}</td>
                              <td>
                                <Status ok={q.is_active}>
                                  {q.is_active ? "Running" : "Stopped"}
                                </Status>
                              </td>
                              <td>{q.batch_id ?? "—"}</td>
                              <td>{number(q.input_rows)}</td>
                              <td>{q.offsets_behind_latest ?? "—"}</td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>

                    <div className="pipeline-note">
                      <Clock3 size={16} />
                      <span>
                        Last metrics update: {timeLabel(streaming.updated_at)}
                      </span>
                    </div>
                  </>
                )}
              </Panel>

              <Panel
                title="Pipeline outputs"
                subtitle="Current object-store summary"
              >
                <div className="dataset-list">
                  <div>
                    <span className="dataset-icon">
                      <FileCheck2 size={18} />
                    </span>
                    <span>
                      <strong>Raw event records</strong>
                      <small>events/</small>
                    </span>
                    <strong>{number(rawFiles)} files</strong>
                  </div>
                  <div>
                    <span className="dataset-icon quarantine">
                      <ShieldCheck size={18} />
                    </span>
                    <span>
                      <strong>Quarantined records</strong>
                      <small>quarantine/</small>
                    </span>
                    <strong>{number(quarantineFiles)} files</strong>
                  </div>
                  <div>
                    <span className="dataset-icon">
                      <Layers3 size={18} />
                    </span>
                    <span>
                      <strong>Streaming checkpoints</strong>
                      <small>checkpoints/</small>
                    </span>
                    <strong>
                      {number(overview?.checkpoint_objects || 0)} objects
                    </strong>
                  </div>
                </div>
              </Panel>
            </div>
          )}

          {page === "lake" && (
            <div className="page-stack">
              <div className="metric-grid">
                <Metric
                  icon={Database}
                  label="Storage bucket"
                  value={overview?.bucket || "—"}
                  note="MinIO object storage"
                />
                <Metric
                  icon={FileText}
                  label="Parquet files"
                  value={number(rawFiles)}
                  note="Under events/"
                  tone="green"
                />
                <Metric
                  icon={Boxes}
                  label="Raw data size"
                  value={`${(Number(overview?.raw_bytes || 0) / 1048576).toFixed(2)} MB`}
                  note="Sum of listed Parquet objects"
                  tone="violet"
                />
                <Metric
                  icon={Layers3}
                  label="Checkpoint objects"
                  value={number(overview?.checkpoint_objects || 0)}
                  note="Under checkpoints/"
                  tone="orange"
                />
              </div>
              <Panel
                title="Data lake datasets"
                subtitle="Logical datasets stored in the configured MinIO bucket"
              >
                <div className="dataset-list">
                  <div>
                    <span className="dataset-icon">
                      <FileCheck2 size={18} />
                    </span>
                    <span>
                      <strong>Raw events</strong>
                      <small>
                        s3://{overview?.bucket || "fluxcart-raw"}/events/
                      </small>
                    </span>
                    <div className="dataset-meta">
                      <strong>{number(rawFiles)} files</strong>
                      <small>
                        {(Number(overview?.raw_bytes || 0) / 1048576).toFixed(
                          2,
                        )}{" "}
                        MB
                      </small>
                    </div>
                    <Status ok={rawFiles > 0}>
                      {rawFiles > 0 ? "Available" : "Empty"}
                    </Status>
                  </div>
                  <div>
                    <span className="dataset-icon quarantine">
                      <ShieldCheck size={18} />
                    </span>
                    <span>
                      <strong>Quarantine</strong>
                      <small>
                        s3://{overview?.bucket || "fluxcart-raw"}/quarantine/
                      </small>
                    </span>
                    <div className="dataset-meta">
                      <strong>{number(quarantineFiles)} files</strong>
                      <small>
                        {(
                          Number(overview?.quarantine_bytes || 0) / 1024
                        ).toFixed(1)}{" "}
                        KB
                      </small>
                    </div>
                    <Status ok={true}>Listed</Status>
                  </div>
                  <div>
                    <span className="dataset-icon">
                      <Layers3 size={18} />
                    </span>
                    <span>
                      <strong>Checkpoints</strong>
                      <small>
                        s3://{overview?.bucket || "fluxcart-raw"}/checkpoints/
                      </small>
                    </span>
                    <div className="dataset-meta">
                      <strong>
                        {number(overview?.checkpoint_objects || 0)} objects
                      </strong>
                      <small>Streaming state</small>
                    </div>
                    <Status ok={true}>Listed</Status>
                  </div>
                </div>
              </Panel>
              <Panel
                title="Raw event sample"
                subtitle="Records read from Parquet files"
              >
                <div className="table-tools">
                  <label className="search-box">
                    <Search size={15} />
                    <input
                      value={query}
                      onChange={(e) => setQuery(e.target.value)}
                      placeholder="Search event ID or type..."
                    />
                  </label>
                  <span className="muted-text">
                    {number(filteredEvents.length)} matching records
                  </span>
                </div>
                <EventTable events={filteredEvents.slice(0, 50)} />
              </Panel>
            </div>
          )}

          {page === "quality" && (
            <div className="page-stack">
              <div className="connection-banner quality-banner">
                <span className="connection-icon">
                  <ShieldCheck size={21} />
                </span>
                <div className="connection-copy">
                  <strong>Data quality overview</strong>
                  <span>
                    Based on records and quarantine objects currently exposed by
                    the API
                  </span>
                </div>
                <span className="subtle-tag">Initial quality view</span>
              </div>

              <Panel
                title="Overall data quality score"
                subtitle="Equal-weighted completeness across three validation checks"
              >
                {quality?.status === "connected" ? (
                  <div className="quality-score-section">
                    <div className="quality-score-header">
                      <div>
                        <div className="quality-score-value">
                          {quality.score === null
                            ? "N/A"
                            : `${quality.score.toFixed(2)}%`}
                        </div>
                        <p>
                          {number(quality.valid_records)} of{" "}
                          {number(quality.sample_size)} records passed all
                          checks
                        </p>
                      </div>
                      <span className="subtle-tag">
                        {quality.sample_size.toLocaleString()} records evaluated
                      </span>
                    </div>

                    <div className="quality-score-track">
                      <div
                        className="quality-score-fill"
                        style={{ width: `${quality.score ?? 0}%` }}
                      />
                    </div>

                    <div className="quality-check-grid">
                      {quality.checks.map((check) => (
                        <div className="quality-score-check" key={check.field}>
                          <span>{check.name}</span>
                          <strong>{check.pass_rate.toFixed(1)}%</strong>
                          <small>
                            {number(check.passed)} passed ·{" "}
                            {number(check.missing)} missing
                          </small>
                        </div>
                      ))}
                    </div>

                    <p className="quality-score-note">{quality.scope}</p>
                  </div>
                ) : (
                  <Empty
                    icon={Gauge}
                    title="Quality score unavailable"
                    detail="Waiting for the quality analytics API."
                  />
                )}
              </Panel>

              <div className="metric-grid">
                <Metric
                  icon={AlertTriangle}
                  label="Rejected records"
                  value={number(quarantine.total_records)}
                  note="Actual rejected records in quarantine/"
                  tone="orange"
                />
                <Metric
                  icon={FileCheck2}
                  label="Sampled records"
                  value={number(events.length)}
                  note="Raw event sample returned by API"
                  tone="blue"
                />
                <Metric
                  icon={AlertTriangle}
                  label="Records missing event ID"
                  value={number(events.filter((e) => !e.event_id).length)}
                  note="Within returned sample"
                  tone="violet"
                />
                <Metric
                  icon={Clock3}
                  label="Records missing timestamp"
                  value={number(
                    events.filter((e) => !hasEventTimestamp(e)).length,
                  )}
                  note={`${
                    events.length
                      ? Math.round(
                          (events.filter(hasEventTimestamp).length /
                            events.length) *
                            100,
                        )
                      : 0
                  }% timestamp coverage in sample`}
                  tone="green"
                />
              </div>

              <Panel
                title="Rejection reason breakdown"
                subtitle={`Validation failures across ${number(quarantine.total_records)} quarantined records`}
              >
                {quarantine.status === "unavailable" ? (
                  <Empty
                    icon={AlertTriangle}
                    title="Quarantine data unavailable"
                    detail="The API could not retrieve quarantine analytics."
                  />
                ) : quarantine.reason_counts?.length ? (
                  <div className="quarantine-reasons">
                    {quarantine.reason_counts.map((item) => (
                      <div className="quarantine-reason-row" key={item.reason}>
                        <div className="quarantine-reason-info">
                          <span>{item.reason.replaceAll("_", " ")}</span>
                          <strong>{number(item.count)}</strong>
                        </div>
                        <div className="quarantine-reason-track">
                          <div
                            className="quarantine-reason-fill"
                            style={{
                              width: `${
                                quarantine.total_records
                                  ? Math.min(
                                      100,
                                      (item.count / quarantine.total_records) *
                                        100,
                                    )
                                  : 0
                              }%`,
                            }}
                          />
                        </div>
                      </div>
                    ))}
                    <p className="quarantine-footnote">
                      A single rejected record can contribute to multiple
                      rejection reasons.
                    </p>
                  </div>
                ) : (
                  <Empty
                    icon={ShieldCheck}
                    title="No rejected records found"
                    detail="No validation failures are currently available in quarantine storage."
                  />
                )}
              </Panel>

              <Panel
                title="Recent rejected events"
                subtitle="Latest records retrieved from the quarantine dataset"
              >
                {quarantine.recent_records?.length ? (
                  <div className="table-wrap">
                    <table className="event-table">
                      <thead>
                        <tr>
                          <th>Event ID</th>
                          <th>Event type</th>
                          <th>Timestamp</th>
                          <th>Rejection reason</th>
                          <th>Partition</th>
                        </tr>
                      </thead>
                      <tbody>
                        {quarantine.recent_records.map((record, index) => (
                          <tr
                            key={`${record.kafka_partition}-${record.kafka_offset}-${index}`}
                          >
                            <td className="event-id">
                              {record.event_id || "Missing"}
                            </td>
                            <td>{record.event_type || "Unknown"}</td>
                            <td>{timeLabel(record.timestamp)}</td>
                            <td className="quarantine-reason-cell">
                              {record.rejection_reason || "Not provided"}
                            </td>
                            <td>{record.kafka_partition ?? "—"}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                ) : (
                  <Empty
                    icon={FileText}
                    title="No recent rejected events"
                    detail="Rejected event records will appear here when available."
                  />
                )}
              </Panel>

              <Panel
                title="Quality checks"
                subtitle="Basic validation against available raw event records"
              >
                {[
                  {
                    label: "Event ID present",
                    desc: "Records should include a stable event identifier.",
                    bad: events.filter((e) => !e.event_id).length,
                  },
                  {
                    label: "Event type present",
                    desc: "Records should identify the type of business event.",
                    bad: events.filter((e) => !e.event_type).length,
                  },
                  {
                    label: "Timestamp present",
                    desc: "Records should include an event timestamp.",
                    bad: events.filter((e) => !hasEventTimestamp(e)).length,
                  },
                ].map((check) => (
                  <div className="quality-row" key={check.label}>
                    <span
                      className={`quality-check ${check.bad ? "has-issues" : "pass"}`}
                    >
                      {check.bad ? (
                        <AlertTriangle size={17} />
                      ) : (
                        <Check size={17} />
                      )}
                    </span>
                    <div>
                      <strong>{check.label}</strong>
                      <p>{check.desc}</p>
                    </div>
                    <span
                      className={`quality-result ${check.bad ? "issues" : "passed"}`}
                    >
                      {check.bad
                        ? `${check.bad} missing`
                        : "No issues in sample"}
                    </span>
                  </div>
                ))}
                {!events.length && (
                  <Empty
                    icon={ShieldCheck}
                    title="No sample available"
                    detail="Load event records before running these checks."
                  />
                )}
              </Panel>
              <Panel
                title="Quarantine dataset"
                subtitle="Objects identified under the quarantine prefix"
              >
                <div className="quarantine-summary">
                  <span className="quarantine-symbol">
                    <AlertTriangle size={23} />
                  </span>
                  <div>
                    <strong>{number(quarantineFiles)} Parquet files</strong>
                    <p>
                      {(Number(overview?.quarantine_bytes || 0) / 1024).toFixed(
                        1,
                      )}{" "}
                      KB currently listed under quarantine/.
                    </p>
                  </div>
                </div>
                <p className="footnote">
                  Record-level quarantine analytics are available, including
                  rejection reasons, Kafka metadata, and recent rejected events.
                </p>
              </Panel>
            </div>
          )}

          <footer className="app-footer">
            <span>
              FluxCart <b>·</b> Streaming Data Platform
            </span>
            <span>
              {lastUpdated
                ? `Last updated ${lastUpdated.toLocaleTimeString()}`
                : "Connecting to API"}{" "}
              <b>·</b> Auto-refresh enabled
            </span>
          </footer>
        </div>
      </main>
    </div>
  );
}

function EventTable({ events }) {
  if (!events.length)
    return (
      <Empty
        icon={Database}
        title="No records to display"
        detail="Records will appear here when available from the raw data lake."
      />
    );
  return (
    <div className="table-wrap">
      <table className="event-table">
        <thead>
          <tr>
            <th>Event ID</th>
            <th>Event type</th>
            <th>Timestamp</th>
            <th>Amount</th>
          </tr>
        </thead>
        <tbody>
          {events.map((event, i) => {
            const amount = Number(event.amount);
            const hasAmount =
              event.amount !== null &&
              event.amount !== undefined &&
              event.amount !== "" &&
              Number.isFinite(amount) &&
              amount !== 0;
            return (
              <tr key={event.event_id || i}>
                <td className="event-id">{event.event_id || "—"}</td>
                <td>
                  <span className="event-type">
                    {String(event.event_type || "UNKNOWN").replaceAll("_", " ")}
                  </span>
                </td>
                <td>{timeLabel(event.timestamp)}</td>
                <td className={hasAmount ? "amount-value" : "muted-text"}>
                  {hasAmount ? money(amount) : "—"}
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}
