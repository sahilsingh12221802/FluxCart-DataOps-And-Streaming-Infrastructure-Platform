
import { useCallback, useEffect, useMemo, useState } from "react";
import {
  Activity, AlertTriangle, ArrowDownRight, ArrowUpRight, Boxes,
  Check, ChevronRight, CircleHelp, Clock3, Database, Download,
  ExternalLink, FileCheck2, FileText, Gauge, Layers3, LayoutDashboard,
  RefreshCw, Search, Server, ShieldCheck, ShoppingCart, Workflow, Zap
} from "lucide-react";
import {
  Area, AreaChart, Bar, BarChart, CartesianGrid, ResponsiveContainer,
  Tooltip, XAxis, YAxis
} from "recharts";
import "./App.css";

const API = "http://localhost:8000";

const NAV = [
  { id: "overview", label: "Overview", icon: LayoutDashboard, group: "WORKSPACE" },
  { id: "pipeline", label: "Streaming pipeline", icon: Workflow, group: "WORKSPACE" },
  { id: "lake", label: "Data lake", icon: Database, group: "DATA MANAGEMENT" },
  { id: "quality", label: "Data quality", icon: ShieldCheck, group: "DATA MANAGEMENT" },
];

const money = (n) => new Intl.NumberFormat("en-US", {
  style: "currency", currency: "USD", maximumFractionDigits: 2
}).format(n || 0);

const number = (n) => new Intl.NumberFormat("en-US").format(n || 0);

function Status({ ok, children }) {
  return <span className={`status ${ok ? "status-ok" : "status-warn"}`}>
    <span className="status-dot" />{children}
  </span>;
}

function Metric({ icon: Icon, label, value, note, tone = "blue" }) {
  return <article className="metric-card">
    <div className="metric-top">
      <span className={`metric-icon ${tone}`}><Icon size={19} /></span>
      <span className="metric-label">{label}</span>
    </div>
    <div className="metric-value">{value}</div>
    <div className="metric-note">{note}</div>
  </article>;
}

function Panel({ title, subtitle, action, children, className = "" }) {
  return <section className={`panel ${className}`}>
    <div className="panel-head">
      <div><h3>{title}</h3>{subtitle && <p>{subtitle}</p>}</div>
      {action}
    </div>
    {children}
  </section>;
}

function Empty({ icon: Icon = Database, title, detail }) {
  return <div className="empty-state">
    <span><Icon size={23} /></span><strong>{title}</strong><p>{detail}</p>
  </div>;
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
  const [query, setQuery] = useState("");
  const [loading, setLoading] = useState(true);
  const [lastUpdated, setLastUpdated] = useState(null);
  const [error, setError] = useState("");

  const refresh = useCallback(async (initial = false) => {
    if (initial) setLoading(true);
    try {
      const paths = ["/api/health", "/api/overview", "/api/event-types", "/api/events?limit=500"];
      const responses = await Promise.all(paths.map((path) => fetch(`${API}${path}`)));
      if (responses.some((response) => !response.ok)) throw new Error("One or more API requests failed.");
      const [h, o, t, e] = await Promise.all(responses.map((response) => response.json()));
      setHealth(h);
      setOverview(o);
      setTypes(t.event_types || []);
      setEvents(e.events || []);
      setError([o.error, t.error, e.error].filter(Boolean).join(" · "));
      setLastUpdated(new Date());
    } catch {
      setError("Could not connect to FluxCart API. Confirm the backend is running on port 8000.");
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
      [event.event_id, event.event_type, event.timestamp]
        .some((value) => String(value ?? "").toLowerCase().includes(term))
    );
  }, [events, query]);

  const totalSampled = types.reduce((sum, item) => sum + item.count, 0);
  const purchases = events.filter((e) => e.event_type === "PURCHASE");
  const revenue = purchases.reduce((sum, e) => sum + (Number(e.amount) || 0), 0);
  const revenueEvents = purchases.filter((e) => Number(e.amount) > 0).length;
  const purchaseRate = totalSampled
    ? ((types.find((t) => t.name === "PURCHASE")?.count || 0) / totalSampled) * 100
    : 0;
  const storageHealthy = health?.minio === "healthy";
  const apiHealthy = health?.api === "healthy";

  const currentPage = NAV.find((item) => item.id === page);
  const rawFiles = overview?.parquet_files ?? 0;
  const quarantineFiles = overview?.quarantine_files ?? 0;

  const chartData = types.map((item) => ({
    name: item.name.replaceAll("_", " "),
    events: item.count,
  }));

  return (
    <div className="app-shell">
      <aside className="sidebar">
        <button className="brand" onClick={() => setPage("overview")}>
          <span className="brand-symbol"><Activity size={22} /></span>
          <span className="brand-copy"><strong>FluxCart</strong><small>DATA PLATFORM</small></span>
        </button>

        <div className="workspace-switch">
          <span className="workspace-icon">F</span>
          <span><strong>FluxCart workspace</strong><small>Development environment</small></span>
          <ChevronRight size={15} />
        </div>

        {["WORKSPACE", "DATA MANAGEMENT"].map((group) => (
          <div className="nav-group" key={group}>
            <div className="nav-heading">{group}</div>
            {NAV.filter((item) => item.group === group).map(({ id, label, icon: Icon }) => (
              <button key={id} onClick={() => setPage(id)}
                className={`nav-link ${page === id ? "selected" : ""}`}>
                <Icon size={18} /><span>{label}</span>
                {id === "quality" && quarantineFiles > 0 &&
                  <span className="nav-count">{quarantineFiles}</span>}
              </button>
            ))}
          </div>
        ))}

        <div className="sidebar-spacer" />
        <div className="sidebar-health">
          <div className="health-heading"><span className="live-pulse" /> ENVIRONMENT STATUS</div>
          <div className="health-line"><span>API service</span><Status ok={apiHealthy}>{apiHealthy ? "Online" : "Offline"}</Status></div>
          <div className="health-line"><span>Object storage</span><Status ok={storageHealthy}>{storageHealthy ? "Online" : "Check"}</Status></div>
        </div>
        <div className="sidebar-footer"><span className="avatar">FC</span><span><strong>FluxCart</strong><small>Streaming analytics</small></span></div>
      </aside>

      <main className="main-area">
        <header className="topbar">
          <div className="breadcrumbs"><span>FluxCart</span><ChevronRight size={14} /><strong>{currentPage?.label || "Overview"}</strong></div>
          <div className="top-actions">
            <span className="refresh-time"><span className="live-pulse" /> Auto-refresh · 10s</span>
            <button className="icon-button" title="Refresh data" onClick={() => refresh()}><RefreshCw size={17} /></button>
            <a className="docs-button" href={`${API}/docs`} target="_blank" rel="noreferrer">API docs <ExternalLink size={13} /></a>
          </div>
        </header>

        <div className="page-content">
          <div className="page-title-row">
            <div>
              <div className="eyebrow">REAL-TIME E-COMMERCE INTELLIGENCE</div>
              <h1>{currentPage?.label || "Overview"}</h1>
              <p className="page-description">
                {page === "overview" && "Your streaming data, business activity, and infrastructure at a glance."}
                {page === "pipeline" && "Explore the data flow from event generation to object storage."}
                {page === "lake" && "Inspect the raw datasets persisted by your streaming pipeline."}
                {page === "quality" && "Monitor quarantined data and the quality signals currently available."}
              </p>
            </div>
            <button className="primary-button" onClick={() => refresh()}><RefreshCw size={16} /> Refresh data</button>
          </div>

          {error && <div className="notice"><AlertTriangle size={17} /><span>{error}</span></div>}

          {page === "overview" && <div className="page-stack">
            <div className="connection-banner">
              <span className="connection-icon"><Check size={21} /></span>
              <div className="connection-copy"><strong>{storageHealthy ? "Object storage is connected" : "Checking object storage"}</strong>
                <span>{overview?.bucket ? `Bucket · ${overview.bucket}` : "Waiting for bucket details"}</span></div>
              <Status ok={storageHealthy}>{storageHealthy ? "OPERATIONAL" : "CHECK REQUIRED"}</Status>
            </div>

            <div className="section-title"><h2>Platform snapshot</h2><span>Based on records available through the API</span></div>
            <div className="metric-grid">
              <Metric icon={Activity} label="Events analyzed" value={number(totalSampled)}
                note="Sampled from readable Parquet data" tone="blue" />
              <Metric icon={ShoppingCart} label="Purchase events" value={number(purchases.length)}
                note="In the current event sample" tone="violet" />
              <Metric icon={Gauge} label="Purchase event share" value={`${purchaseRate.toFixed(1)}%`}
                note="Purchases ÷ sampled event counts" tone="green" />
              <Metric icon={Database} label="Raw data stored" value={`${(Number(overview?.raw_bytes || 0) / 1048576).toFixed(1)} MB`}
                note={`${number(rawFiles)} Parquet files`} tone="orange" />
            </div>

            <div className="content-grid">
              <Panel title="Event distribution" subtitle="Frequency by event type in the sampled data"
                action={<span className="subtle-tag"><Activity size={13} /> Event analytics</span>}>
                {chartData.length ? <div className="chart-area">
                  <ResponsiveContainer width="100%" height="100%">
                    <BarChart data={chartData} margin={{ top: 10, right: 10, left: -15, bottom: 35 }}>
                      <CartesianGrid stroke="#edf0f6" strokeDasharray="4 4" vertical={false} />
                      <XAxis dataKey="name" tick={{ fill: "#778198", fontSize: 10 }} angle={-22} textAnchor="end" interval={0} axisLine={false} tickLine={false} />
                      <YAxis allowDecimals={false} tick={{ fill: "#778198", fontSize: 11 }} axisLine={false} tickLine={false} />
                      <Tooltip cursor={{ fill: "#f3f5ff" }} contentStyle={{ borderRadius: 10, border: "1px solid #e5e9f2", fontSize: 12 }} />
                      <Bar dataKey="events" name="Events" fill="#6076e9" radius={[5, 5, 0, 0]} maxBarSize={42} />
                    </BarChart>
                  </ResponsiveContainer>
                </div> : <Empty title="No event data yet" detail="The chart will populate when event records are available." />}
              </Panel>

              <Panel title="Infrastructure health" subtitle="Live API and object storage checks">
                <div className="service-list">
                  <div className="service-row"><span className="service-icon"><Server size={17} /></span><span className="service-copy"><strong>FluxCart API</strong><small>FastAPI service</small></span><Status ok={apiHealthy}>{apiHealthy ? "Healthy" : "Unavailable"}</Status></div>
                  <div className="service-row"><span className="service-icon"><Database size={17} /></span><span className="service-copy"><strong>MinIO storage</strong><small>{overview?.bucket || "Object storage bucket"}</small></span><Status ok={storageHealthy}>{storageHealthy ? "Healthy" : "Unavailable"}</Status></div>
                  <div className="service-row"><span className="service-icon"><Layers3 size={17} /></span><span className="service-copy"><strong>Raw dataset</strong><small>Parquet output files</small></span><Status ok={overview?.status === "connected"}>{overview?.status === "connected" ? "Available" : "Check"}</Status></div>
                </div>
                <div className="storage-card"><div><span>Raw data footprint</span><strong>{(Number(overview?.raw_bytes || 0) / 1048576).toFixed(2)} MB</strong></div><div className="storage-track"><span style={{ width: overview?.raw_bytes ? "100%" : "0%" }} /></div><small>Reported object size · not capacity utilization</small></div>
              </Panel>
            </div>

            <Panel title="Recent event records" subtitle={`${number(events.length)} records returned by the API`}
              action={<button className="text-button" onClick={() => setPage("lake")}>Explore data lake <ChevronRight size={14} /></button>}>
              <div className="table-tools"><label className="search-box"><Search size={15} /><input value={query} onChange={(e) => setQuery(e.target.value)} placeholder="Search event ID or type..." /></label><span className="muted-text">Showing up to 10 rows</span></div>
              <EventTable events={filteredEvents.slice(0, 10)} />
            </Panel>
          </div>}

          {page === "pipeline" && <div className="page-stack">
            <div className="connection-banner"><span className="connection-icon"><Workflow size={21} /></span><div className="connection-copy"><strong>Streaming architecture</strong><span>Event ingestion → stream processing → object storage</span></div><span className="subtle-tag">Architecture view</span></div>
            <Panel title="Data processing flow" subtitle="FluxCart pipeline components">
              <div className="pipeline-flow">
                {[
                  { n: "01", title: "Event simulator", sub: "Synthetic e-commerce events", icon: Zap, cls: "flow-purple" },
                  { n: "02", title: "Apache Kafka", sub: "Topic: ecommerce-events", icon: Activity, cls: "flow-blue" },
                  { n: "03", title: "Apache Spark", sub: "Structured Streaming", icon: Workflow, cls: "flow-orange" },
                  { n: "04", title: "MinIO data lake", sub: "Parquet objects on S3 API", icon: Database, cls: "flow-green" },
                ].map((node, i) => <div className="flow-unit" key={node.n}>
                  <div className={`flow-node ${node.cls}`}><node.icon size={24} /><span>{node.n}</span></div>
                  <strong>{node.title}</strong><small>{node.sub}</small>
                  {i < 3 && <ChevronRight className="flow-arrow" size={21} />}
                </div>)}
              </div>
              <div className="pipeline-note"><CircleHelp size={16} /><span>This diagram represents the configured architecture. Service-level Kafka and Spark health metrics are not yet exposed by the dashboard API.</span></div>
            </Panel>
            <div className="metric-grid">
              <Metric icon={Activity} label="Sampled events" value={number(totalSampled)} note="API event sample" />
              <Metric icon={Database} label="Raw output files" value={number(rawFiles)} note="Parquet objects in MinIO" tone="green" />
              <Metric icon={AlertTriangle} label="Quarantine files" value={number(quarantineFiles)} note="Quarantine Parquet objects" tone="orange" />
              <Metric icon={Server} label="Storage API" value={storageHealthy ? "Healthy" : "Check"} note="MinIO connectivity probe" tone="violet" />
            </div>
            <Panel title="Pipeline outputs" subtitle="Current object-store summary">
              <div className="dataset-list">
                <div><span className="dataset-icon"><FileCheck2 size={18} /></span><span><strong>Raw event records</strong><small>events/</small></span><strong>{number(rawFiles)} files</strong></div>
                <div><span className="dataset-icon quarantine"><ShieldCheck size={18} /></span><span><strong>Quarantined records</strong><small>quarantine/</small></span><strong>{number(quarantineFiles)} files</strong></div>
                <div><span className="dataset-icon"><Layers3 size={18} /></span><span><strong>Streaming checkpoints</strong><small>checkpoints/</small></span><strong>{number(overview?.checkpoint_objects || 0)} objects</strong></div>
              </div>
            </Panel>
          </div>}

          {page === "lake" && <div className="page-stack">
            <div className="metric-grid">
              <Metric icon={Database} label="Storage bucket" value={overview?.bucket || "—"} note="MinIO object storage" />
              <Metric icon={FileText} label="Parquet files" value={number(rawFiles)} note="Under events/" tone="green" />
              <Metric icon={Boxes} label="Raw data size" value={`${(Number(overview?.raw_bytes || 0) / 1048576).toFixed(2)} MB`} note="Sum of listed Parquet objects" tone="violet" />
              <Metric icon={Layers3} label="Checkpoint objects" value={number(overview?.checkpoint_objects || 0)} note="Under checkpoints/" tone="orange" />
            </div>
            <Panel title="Data lake datasets" subtitle="Logical datasets stored in the configured MinIO bucket">
              <div className="dataset-list">
                <div><span className="dataset-icon"><FileCheck2 size={18} /></span><span><strong>Raw events</strong><small>s3://{overview?.bucket || "fluxcart-raw"}/events/</small></span><div className="dataset-meta"><strong>{number(rawFiles)} files</strong><small>{(Number(overview?.raw_bytes || 0) / 1048576).toFixed(2)} MB</small></div><Status ok={rawFiles > 0}>{rawFiles > 0 ? "Available" : "Empty"}</Status></div>
                <div><span className="dataset-icon quarantine"><ShieldCheck size={18} /></span><span><strong>Quarantine</strong><small>s3://{overview?.bucket || "fluxcart-raw"}/quarantine/</small></span><div className="dataset-meta"><strong>{number(quarantineFiles)} files</strong><small>{(Number(overview?.quarantine_bytes || 0) / 1024).toFixed(1)} KB</small></div><Status ok={true}>Listed</Status></div>
                <div><span className="dataset-icon"><Layers3 size={18} /></span><span><strong>Checkpoints</strong><small>s3://{overview?.bucket || "fluxcart-raw"}/checkpoints/</small></span><div className="dataset-meta"><strong>{number(overview?.checkpoint_objects || 0)} objects</strong><small>Streaming state</small></div><Status ok={true}>Listed</Status></div>
              </div>
            </Panel>
            <Panel title="Raw event sample" subtitle="Records read from Parquet files">
              <div className="table-tools"><label className="search-box"><Search size={15} /><input value={query} onChange={(e) => setQuery(e.target.value)} placeholder="Search event ID or type..." /></label><span className="muted-text">{number(filteredEvents.length)} matching records</span></div>
              <EventTable events={filteredEvents.slice(0, 50)} />
            </Panel>
          </div>}

          {page === "quality" && <div className="page-stack">
            <div className="connection-banner quality-banner"><span className="connection-icon"><ShieldCheck size={21} /></span><div className="connection-copy"><strong>Data quality overview</strong><span>Based on records and quarantine objects currently exposed by the API</span></div><span className="subtle-tag">Initial quality view</span></div>
            <div className="metric-grid">
              <Metric icon={ShieldCheck} label="Quarantine files" value={number(quarantineFiles)} note="Parquet objects under quarantine/" tone="orange" />
              <Metric icon={FileCheck2} label="Sampled records" value={number(events.length)} note="Raw event sample returned by API" tone="blue" />
              <Metric icon={AlertTriangle} label="Records missing event ID" value={number(events.filter(e => !e.event_id).length)} note="Within returned sample" tone="violet" />
              <Metric icon={Clock3} label="Records missing timestamp" value={number(events.filter(e => !e.timestamp).length)} note="Within returned sample" tone="green" />
            </div>
            <Panel title="Quality checks" subtitle="Basic validation against available raw event records">
              {[
                { label: "Event ID present", desc: "Records should include a stable event identifier.", bad: events.filter(e => !e.event_id).length },
                { label: "Event type present", desc: "Records should identify the type of business event.", bad: events.filter(e => !e.event_type).length },
                { label: "Timestamp present", desc: "Records should include an event timestamp.", bad: events.filter(e => !e.timestamp).length },
              ].map((check) => <div className="quality-row" key={check.label}>
                <span className={`quality-check ${check.bad ? "has-issues" : "pass"}`}>{check.bad ? <AlertTriangle size={17} /> : <Check size={17} />}</span>
                <div><strong>{check.label}</strong><p>{check.desc}</p></div>
                <span className={`quality-result ${check.bad ? "issues" : "passed"}`}>{check.bad ? `${check.bad} missing` : "No issues in sample"}</span>
              </div>)}
              {!events.length && <Empty icon={ShieldCheck} title="No sample available" detail="Load event records before running these checks." />}
            </Panel>
            <Panel title="Quarantine dataset" subtitle="Objects identified under the quarantine prefix">
              <div className="quarantine-summary"><span className="quarantine-symbol"><AlertTriangle size={23} /></span><div><strong>{number(quarantineFiles)} Parquet files</strong><p>{(Number(overview?.quarantine_bytes || 0) / 1024).toFixed(1)} KB currently listed under quarantine/.</p></div></div>
              <p className="footnote">File counts alone do not indicate the number of rejected events. Record-level quarantine inspection and validation metrics can be added next.</p>
            </Panel>
          </div>}

          <footer className="app-footer"><span>FluxCart <b>·</b> Streaming Data Platform</span><span>{lastUpdated ? `Last updated ${lastUpdated.toLocaleTimeString()}` : "Connecting to API"} <b>·</b> Auto-refresh enabled</span></footer>
        </div>
      </main>
    </div>
  );
}

function EventTable({ events }) {
  if (!events.length) return <Empty icon={Database} title="No records to display" detail="Records will appear here when available from the raw data lake." />;
  return <div className="table-wrap"><table className="event-table">
    <thead><tr><th>Event ID</th><th>Event type</th><th>Timestamp</th><th>Amount</th></tr></thead>
    <tbody>{events.map((event, i) => {
      const amount = Number(event.amount);
      const hasAmount = event.amount !== null && event.amount !== undefined && event.amount !== "" && Number.isFinite(amount) && amount !== 0;
      return <tr key={event.event_id || i}>
        <td className="event-id">{event.event_id || "—"}</td>
        <td><span className="event-type">{String(event.event_type || "UNKNOWN").replaceAll("_", " ")}</span></td>
        <td>{timeLabel(event.timestamp)}</td>
        <td className={hasAmount ? "amount-value" : "muted-text"}>{hasAmount ? money(amount) : "—"}</td>
      </tr>;
    })}</tbody>
  </table></div>;
}
