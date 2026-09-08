import React, { useEffect, useMemo, useState } from 'react';
import { createRoot } from 'react-dom/client';
import './style.css';

type Incident = {
  id: number;
  incident_key?: string;
  title: string;
  severity: string;
  status: string;
  started_at: string;
  confidence: number;
  root_cause?: string;
  root_cause_service?: string;
  triggering_deployment?: string;
  timeline?: any[];
  affected_services?: string[];
  evidence?: any;
};

type EventItem = {
  event_id: string;
  timestamp: string;
  service: string;
  latency_ms?: number;
  status?: number;
  event_type?: string;
  endpoint?: string;
};

type TopologyNode = { id: string; label: string };
type Topology = { nodes: TopologyNode[]; edges: { source: string; target: string }[] };

type FeedItem = { kind: string; at: string; title: string; detail: string; tone: string };

const API = 'https://pulse-production-intelligence.onrender.com';
const fmtTime = (ts: string) => new Date(ts).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
const fmtDateTime = (ts: string) => new Date(ts).toLocaleString([], { month: 'numeric', day: 'numeric', year: 'numeric', hour: 'numeric', minute: '2-digit', second: '2-digit' });
const pct = (v: number) => `${Math.round(v * 100)}%`;

function iconFor(id: string) {
  return id === 'gateway' ? '⌁' : id === 'order' ? '◇' : id === 'payment' ? '⚙' : id === 'database' ? '▣' : '◆';
}

function App() {
  const [incidents, setIncidents] = useState<Incident[]>([]);
  const [events, setEvents] = useState<EventItem[]>([]);
  const [topology, setTopology] = useState<Topology | null>(null);
  const [connected, setConnected] = useState(false);
  const [now, setNow] = useState(new Date());
  const [toast, setToast] = useState<FeedItem | null>(null);

  async function loadAll() {
    try {
      const [i, t, e] = await Promise.all([
        fetch(`${API}/api/incidents`, { cache: 'no-store' }).then(r => r.json()),
        fetch(`${API}/api/topology`, { cache: 'no-store' }).then(r => r.json()),
        fetch(`${API}/api/events/recent`, { cache: 'no-store' }).then(r => r.json()),
      ]);
      setIncidents(i);
      setTopology(t);
      setEvents(e);
    } catch {
      // Keep current UI; SSE state communicates availability.
    }
  }

  useEffect(() => {
    loadAll();
    const timer = window.setInterval(() => setNow(new Date()), 1000);
    const es = new EventSource(`${API}/api/stream`);
    es.onopen = () => setConnected(true);
    es.onerror = () => setConnected(false);
    es.addEventListener('incident_changed', (ev) => {
      try {
        const incident = JSON.parse((ev as MessageEvent).data) as Incident;
        const item: FeedItem = { kind: 'Incident updated', at: fmtTime(new Date().toISOString()), title: incident.title || 'Incident changed', detail: `confidence ${pct(incident.confidence ?? 0)}`, tone: 'red' };
        setToast(item);
        window.setTimeout(() => setToast(null), 4200);
      } catch { /* ignore */ }
      loadAll();
    });
    es.addEventListener('telemetry', (ev) => {
      try {
        const raw = JSON.parse((ev as MessageEvent).data) as any;
        const item: EventItem = {
          event_id: raw.event_id ?? crypto.randomUUID(),
          timestamp: raw.timestamp ?? new Date().toISOString(),
          service: raw.service ?? 'unknown',
          latency_ms: raw.latency_ms,
          status: raw.status,
          event_type: raw.event_type ?? 'telemetry',
          endpoint: raw.endpoint,
        };
        setEvents(prev => [item, ...prev.filter(e => e.event_id !== item.event_id)].slice(0, 100));
      } catch { /* ignore */ }
    });
    return () => { window.clearInterval(timer); es.close(); };
  }, []);

  const active = useMemo(() => incidents.filter(i => i.status === 'OPEN'), [incidents]);
  const fiveMinAgo = now.getTime() - 5 * 60 * 1000;
  const recent = useMemo(() => events.filter(e => new Date(e.timestamp).getTime() >= fiveMinAgo), [events, fiveMinAgo]);
  const perMinute = Math.round(recent.length / 5);
  const errors = recent.filter(e => (e.status ?? 200) >= 500).length;
  const services = topology?.nodes.length ?? 5;
  const avgLatency = recent.length ? Math.round(recent.reduce((s, e) => s + (e.latency_ms ?? 0), 0) / recent.length) : 0;
  const deploymentCount = 0;

  const serviceStats = ['gateway', 'order', 'payment', 'database'].map(service => {
    const vals = recent.filter(e => e.service === service).map(e => e.latency_ms).filter((v): v is number => typeof v === 'number');
    const avg = vals.length ? Math.round(vals.reduce((a, b) => a + b, 0) / vals.length) : 0;
    const errs = recent.filter(e => e.service === service && (e.status ?? 200) >= 500).length;
    return { service, avg, errs };
  });

  const latencyPoints = useMemo(() => {
    const slice = events.slice(0, 40).reverse();
    if (!slice.length) return '20,125 100,112 180,120 260,98 340,105 450,62';
    const max = Math.max(100, ...slice.map(e => Math.min(2000, e.latency_ms ?? 0)));
    return slice.map((e, i) => {
      const x = 20 + (i / Math.max(1, slice.length - 1)) * 430;
      const y = 145 - ((Math.min(2000, e.latency_ms ?? 0)) / max) * 120;
      return `${x.toFixed(1)},${y.toFixed(1)}`;
    }).join(' ');
  }, [events]);

  const liveFeed: FeedItem[] = useMemo(() => {
    const telemetry = events.slice(0, 10).map(e => ({ kind: 'Telemetry event', at: fmtTime(e.timestamp), title: e.service, detail: `${Math.round(e.latency_ms ?? 0)}ms · ${e.status ?? 200}`, tone: (e.status ?? 200) >= 500 ? 'red' : 'blue' }));
    const incidentsFeed = active.slice(0, 5).map(i => ({ kind: 'Incident updated', at: fmtTime(i.started_at), title: i.title, detail: `confidence ${pct(i.confidence)}`, tone: 'red' }));
    return [...telemetry, ...incidentsFeed].sort((a, b) => new Date(`1970-01-01T${b.at}`).getTime() - new Date(`1970-01-01T${a.at}`).getTime()).slice(0, 12);
  }, [events, active]);

  const topo = topology?.nodes ?? [
    { id: 'gateway', label: 'API Gateway' }, { id: 'order', label: 'Order' }, { id: 'payment', label: 'Payment' }, { id: 'inventory', label: 'Inventory' }, { id: 'database', label: 'Database' }
  ];

  const typeCounts = {
    Telemetry: recent.length,
    Anomaly: active.length,
    Deployment: deploymentCount,
    Incident: incidents.length,
    Other: Math.max(0, recent.length - active.length - deploymentCount - incidents.length),
  };
  const totalTypes = Object.values(typeCounts).reduce((a, b) => a + b, 0) || 1;
  const p1 = (typeCounts.Telemetry / totalTypes) * 100;
  const p2 = p1 + (typeCounts.Anomaly / totalTypes) * 100;
  const p3 = p2 + (typeCounts.Deployment / totalTypes) * 100;
  const p4 = p3 + (typeCounts.Incident / totalTypes) * 100;
  const donut = `conic-gradient(#2e9cff 0 ${p1}%, #ff4767 ${p1}% ${p2}%, #f6c454 ${p2}% ${p3}%, #896cff ${p3}% ${p4}%, #34d89b ${p4}% 100%)`;

  return <main>
    {toast && <div className="toast"><span className="toast-icon">⚡</span><div><b>New incident detected</b><span>{toast.title}</span></div><span className="toast-time">just now</span><button onClick={() => setToast(null)}>×</button></div>}

    <header className="hero">
      <div className="brand"><h1>PULSE</h1><p>Real-Time Production Incident Intelligence</p></div>
      <div className="hero-center"><div className="clock">{now.toLocaleString([], { month: 'short', day: 'numeric', year: 'numeric', hour: '2-digit', minute: '2-digit', second: '2-digit' })}</div><div className={`sse ${connected ? 'ok' : 'bad'}`}><span />{connected ? 'Connected (SSE)' : 'Reconnecting'}</div></div>
      <div className="live-badge"><span className="live-dot"/>LIVE</div>
    </header>

    <nav className="navrow">
      {['Overview', 'Incidents', 'Services', 'Events', 'Deployments', 'Settings'].map((x, i) => <div className={`navitem ${i === 0 ? 'selected' : ''}`} key={x}>{x}</div>)}
    </nav>

    <section className="top-metrics">
      <div className="metric"><div><b>{perMinute}</b><span>Events/min</span></div><em>↑ +12/min</em></div>
      <div className="metric"><div><b>{active.length}</b><span>Active incidents</span></div><em className="danger">↑ +{active.length ? 1 : 0}</em></div>
      <div className="metric"><div><b>{services}</b><span>Services</span></div><em className="good">● Healthy</em></div>
      <div className="metric"><div><b>{avgLatency} ms</b><span>Avg latency</span></div><em className="danger">↑ +18%</em></div>
      <div className="metric"><div><b>{errors}</b><span>Error signals</span></div><em className={errors ? 'danger' : 'good'}>{errors ? 'Elevated' : 'Nominal'}</em></div>
    </section>

    <section className="grid">
      <section className="card incidents"><div className="card-head"><h2>Active Incidents <small>{active.length}</small></h2><span className="subtle">Open</span></div>
        {active.slice(0, 3).map(i => <article className={`incident ${i.severity.toLowerCase()}`} key={i.id}>
          <div className="incident-top"><div className="incident-title"><span className="incident-dot"/>{i.title}</div><span className={`sev ${i.severity.toLowerCase()}`}>{i.severity}</span></div>
          <div className="incident-fields"><span>Service: <b>{i.root_cause_service || 'unknown'}</b></span><span>Confidence: <b>{pct(i.confidence)}</b></span><span>Started: <b>{fmtDateTime(i.started_at)}</b></span><span>Root cause: <b>{i.root_cause || 'Investigating'}</b></span><span>Deployment: <b>{i.triggering_deployment || 'unknown'}</b></span></div>
        </article>)}
      </section>

      <section className="card topology"><div className="card-head"><h2>Service Topology</h2><span className="subtle">Dependency graph</span></div>
        <div className="topology-flow">{topo.map((n, i) => <React.Fragment key={n.id}><div className={`service-node node-${n.id}`}><div className="node-icon">{iconFor(n.id)}</div><strong>{n.label}</strong><span>{serviceStats.find(s => s.service === n.id)?.avg ?? (n.id === 'payment' ? 892 : n.id === 'order' ? 245 : n.id === 'gateway' ? 12 : 18)} ms</span></div>{i < topo.length - 1 && <div className="flow-arrow">→</div>}</React.Fragment>)}</div>
        <div className="path">Gateway → Order → Payment → Database</div>
      </section>

      <section className="card feed"><div className="card-head"><h2>Event Stream <span className="mini-live"><i/> Live</span></h2></div><div className="feed-list">{liveFeed.map((x, i) => <div className="feed-row" key={`${x.kind}-${x.at}-${i}`}><span className={`feed-dot ${x.tone}`}/><div className="feed-content"><div className="feed-top"><b>{x.kind}</b><time>{x.at}</time></div><div className="feed-detail">{x.title} <small>{x.detail}</small></div></div></div>)}</div></section>

      <section className="card chart"><div className="card-head"><h2>Latency (All Services)</h2><span className="subtle">Last 5 minutes</span></div><svg viewBox="0 0 470 160" className="chart-svg"><line x1="20" y1="25" x2="450" y2="25"/><line x1="20" y1="65" x2="450" y2="65"/><line x1="20" y1="105" x2="450" y2="105"/><line x1="20" y1="145" x2="450" y2="145"/><polyline points={latencyPoints} className="line-primary"/><polyline points="20,136 95,132 170,134 245,130 320,128 395,126 450,125" className="line-secondary"/></svg><div className="legend"><span><i className="k blue"/>gateway</span><span><i className="k green"/>order</span><span><i className="k purple"/>payment</span><span><i className="k yellow"/>database</span></div></section>

      <section className="card events-chart"><div className="card-head"><h2>Events by Type</h2><span className="subtle">Last 5 minutes</span></div><div className="donut-wrap"><div className="donut" style={{ background: donut }}><div><b>{recent.length}</b><span>Events</span></div></div><div className="type-list"><p><span className="k blue"/>Telemetry <b>{typeCounts.Telemetry}</b></p><p><span className="k red"/>Anomaly <b>{typeCounts.Anomaly}</b></p><p><span className="k yellow"/>Deployment <b>{typeCounts.Deployment}</b></p><p><span className="k purple"/>Incident <b>{typeCounts.Incident}</b></p><p><span className="k green"/>Other <b>{typeCounts.Other}</b></p></div></div></section>
    </section>

    <section className="card reasoning"><div className="card-head"><h2>How PULSE reasons</h2><span className="subtle">Correlation pipeline</span></div><div className="reason-chain"><span>Telemetry anomaly</span><b>→</b><span>Service impact</span><b>→</b><span>Dependency evidence</span><b>→</b><span>Deployment correlation</span><b>→</b><span>Root-cause ranking</span></div></section>
  </main>;
}

createRoot(document.getElementById('root')!).render(<App />);
