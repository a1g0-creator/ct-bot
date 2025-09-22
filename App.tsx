import React, {
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Legend,
} from "recharts";

/* =========================
   Constants & Types
   ========================= */
// HARDCODED: Always use account_id=1
const ACCOUNT_ID = 1;

interface Config {
  apiBaseUrl: string;
  refreshMs: number;
}

interface KPIData {
  netPnl: number;
  high: number;
  low: number;
  maxDrawdownAbs: number;
  maxDrawdownPct: number;
  updatedAt: string;
}

interface TimeSeriesPoint {
  t: number; // epoch ms
  equity: number;
  margin: number;
}

interface MetricsResponse {
  kpi: KPIData;
  series: TimeSeriesPoint[];
}

interface OpenPosition {
  symbol: string;
  side: "Buy" | "Sell";
  qty: number;
  entryPrice?: number | null;
  markPrice?: number | null;
  leverage?: number | null;
  marginMode?: string | null;
  liqPrice?: number | null;
  unrealizedPnl?: number | null; // Fixed: renamed from unrealPnl
  positionIdx: number;
  exchangePositionId?: string | null;
  openedAt?: string | null;
  updatedAt?: string | null;
}

interface ClosedPosition {
  symbol: string;
  side: "Buy" | "Sell";
  qty: number;
  entryPrice?: number | null;
  exitPrice?: number | null;
  markPrice?: number | null;
  leverage?: number | null;
  marginMode?: string | null;
  liqPrice?: number | null;
  realizedPnl?: number | null;
  positionIdx: number;
  exchangePositionId?: string | null;
  openedAt?: string | null;
  closedAt?: string | null;
}

interface OpenPositionsResponse {
  items: OpenPosition[];
  total: number;
  updatedAt: string;
}

interface ClosedPositionsResponse {
  items: ClosedPosition[];
  total: number;
  updatedAt: string;
}

/* =========================
   Utils
   ========================= */
const fmtNum = (v: number | null | undefined, d = 2) =>
  v == null || isNaN(v)
    ? "—"
    : new Intl.NumberFormat("ru-RU", {
        minimumFractionDigits: d,
        maximumFractionDigits: d,
      }).format(v);

const formatDateTime = (epochMs: number) => {
  const d = new Date(epochMs);
  const pad = (n: number) => n.toString().padStart(2, "0");
  return `${pad(d.getDate())}.${pad(d.getMonth() + 1)} ${pad(
    d.getHours()
  )}:${pad(d.getMinutes())}`;
};

/* =========================
   Config Loader
   ========================= */
async function loadConfig(): Promise<Config> {
  try {
    const r = await fetch("/web.config.json", { cache: "no-store" });
    const j = await r.json();
    const __raw = j.API_BASE_URL || "http://127.0.0.1:8080";
    return {
      apiBaseUrl: __raw.startsWith("/")
        ? window.location.origin + __raw
        : __raw,
      refreshMs: Number(j.AUTO_REFRESH_MS || 5000),
    };
  } catch {
    return {
      apiBaseUrl: window.location.origin + "/api",
      refreshMs: 5000,
    };
  }
}

/* =========================
   API Client
   ========================= */
class API {
  private baseUrl = "";

  setBaseUrl(url: string) {
    this.baseUrl = (url || "").replace(/\/+$/, "");
  }

  private makeUrl(path: string) {
    return this.baseUrl ? new URL(path, this.baseUrl).toString() : path;
  }

  async getMetrics(params: {
    from?: Date;
    to?: Date;
  }): Promise<MetricsResponse> {
    const qs = new URLSearchParams({
      accountId: String(ACCOUNT_ID), // Always use 1
    });

    if (params.from) {
      qs.append("from", params.from.toISOString());
    }
    if (params.to) {
      qs.append("to", params.to.toISOString());
    }

    const r = await fetch(`${this.makeUrl("/api/metrics")}?${qs}`);
    if (!r.ok) throw new Error("metrics fetch failed");
    return await r.json();
  }

  async getPositionsOpen(): Promise<OpenPositionsResponse> {
    const qs = new URLSearchParams({
      accountId: String(ACCOUNT_ID), // Always use 1
    }).toString();
    const r = await fetch(`${this.makeUrl("/api/positions/open")}?${qs}`);
    if (!r.ok) throw new Error("open positions fetch failed");
    return await r.json();
  }

  async getPositionsClosed(params: {
    from?: Date;
    to?: Date;
    limit?: number;
    offset?: number;
  }): Promise<ClosedPositionsResponse> {
    const qs = new URLSearchParams({
      accountId: String(ACCOUNT_ID), // Always use 1
      limit: String(params.limit || 100),
      offset: String(params.offset || 0),
    });

    if (params.from) {
      qs.append("from", params.from.toISOString());
    }
    if (params.to) {
      qs.append("to", params.to.toISOString());
    }

    const r = await fetch(`${this.makeUrl("/api/positions/closed")}?${qs}`);
    if (!r.ok) throw new Error("closed positions fetch failed");
    return await r.json();
  }
}

const api = new API();

/* =========================
   Theme & Styles
   ========================= */
const COLORS = {
  bg: "#0b0f14",
  panel: "rgba(17,25,40,0.7)",
  panel2: "rgba(17,25,40,0.45)",
  border: "rgba(148,163,184,0.16)",
  text: "#e8edf2",
  sub: "#9fb1c7",
  accent: "#7aa2ff",
  accent2: "#53f3cf",
  danger: "#e06b6b",
  grid: "#283446",
};

const styles: Record<string, React.CSSProperties> = {
  app: {
    minHeight: "100vh",
    background:
      "radial-gradient(1200px 600px at 80% -10%, rgba(83,243,207,0.08), transparent 65%), #0b0f14",
    padding: "16px",
    color: COLORS.text,
    fontFamily: "-apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif",
  },
  container: { maxWidth: 1400, margin: "0 auto" },
  header: {
    display: "flex",
    alignItems: "center",
    justifyContent: "space-between",
    marginBottom: 20,
    flexWrap: "wrap",
    gap: 10,
  },
  title: {
    fontSize: 26,
    fontWeight: 900,
    letterSpacing: 0.4,
    background: "linear-gradient(135deg,#7aa2ff,#53f3cf)",
    WebkitBackgroundClip: "text",
    WebkitTextFillColor: "transparent",
  },
  controls: { display: "flex", gap: 8, alignItems: "center" },
  button: {
    padding: "8px 16px",
    background: "linear-gradient(135deg,#7aa2ff,#597bc8)",
    border: "none",
    borderRadius: 8,
    color: "#fff",
    fontWeight: 600,
    cursor: "pointer",
    transition: "all 0.2s",
    fontSize: 14,
  },
  select: {
    padding: "8px 12px",
    background: COLORS.panel,
    border: `1px solid ${COLORS.border}`,
    borderRadius: 6,
    color: COLORS.text,
    fontSize: 14,
  },
  kpiRow: {
    display: "grid",
    gridTemplateColumns: "repeat(auto-fit, minmax(200px, 1fr))",
    gap: 16,
    marginBottom: 24,
  },
  kpiCard: {
    background: COLORS.panel,
    border: `1px solid ${COLORS.border}`,
    borderRadius: 12,
    padding: 20,
    position: "relative" as const,
    overflow: "hidden",
    transition: "all 0.3s ease",
  },
  kpiCardGlow: {
    position: "absolute" as const,
    top: 0,
    left: 0,
    right: 0,
    height: "1px",
    background: `linear-gradient(90deg, transparent, ${COLORS.accent}, transparent)`,
    opacity: 0.5,
  },
  kpiLabel: {
    fontSize: 12,
    color: COLORS.sub,
    marginBottom: 8,
    textTransform: "uppercase" as const,
    letterSpacing: 0.5,
  },
  kpiValue: {
    fontSize: 24,
    fontWeight: 800,
    letterSpacing: -0.5,
  },
  chartContainer: {
    background: COLORS.panel2,
    border: `1px solid ${COLORS.border}`,
    borderRadius: 12,
    padding: 20,
    marginBottom: 24,
  },
  sectionTitle: {
    fontSize: 16,
    fontWeight: 700,
    marginBottom: 16,
    color: COLORS.text,
    letterSpacing: 0.3,
  },
  table: {
    width: "100%",
    borderCollapse: "collapse" as const,
  },
  th: {
    textAlign: "left" as const,
    padding: "12px 8px",
    borderBottom: `2px solid ${COLORS.border}`,
    fontSize: 12,
    color: COLORS.sub,
    fontWeight: 600,
    textTransform: "uppercase" as const,
    letterSpacing: 0.5,
  },
  td: {
    padding: "12px 8px",
    borderBottom: `1px solid rgba(148,163,184,0.08)`,
    fontSize: 14,
    color: COLORS.text,
  },
  status: { fontSize: 11, color: COLORS.sub, marginLeft: 8 },
  error: {
    background: "rgba(224,107,107,0.1)",
    border: "1px solid rgba(224,107,107,0.3)",
    borderRadius: 8,
    padding: 12,
    marginBottom: 16,
    color: COLORS.danger,
  },
};

/* =========================
   Components
   ========================= */
const KPICards: React.FC<{ data: KPIData | null }> = ({ data }) => {
  if (!data) {
    return (
      <div style={styles.kpiRow}>
        {[1, 2, 3, 4].map((i) => (
          <div key={i} style={styles.kpiCard}>
            <div style={styles.kpiCardGlow} />
            <div style={styles.kpiLabel}>Loading...</div>
            <div style={styles.kpiValue}>—</div>
          </div>
        ))}
      </div>
    );
  }

  const { netPnl, high, low, maxDrawdownPct } = data;

  return (
    <div style={styles.kpiRow}>
      <div
        style={styles.kpiCard}
        onMouseEnter={(e) =>
          ((e.currentTarget.style.boxShadow =
            netPnl >= 0
              ? "0 18px 36px -16px rgba(83,243,207,.35)"
              : "0 18px 36px -16px rgba(224,107,107,.35)"),
          (e.currentTarget.style.transform = "translateY(-2px)"))
        }
        onMouseLeave={(e) =>
          ((e.currentTarget.style.boxShadow = "none"),
          (e.currentTarget.style.transform = "none"))
        }
      >
        <div style={styles.kpiCardGlow} />
        <div style={styles.kpiLabel}>Net P&L</div>
        <div
          style={{
            ...styles.kpiValue,
            background:
              netPnl >= 0
                ? "linear-gradient(90deg,#53f3cf,#7de7ff)"
                : "linear-gradient(90deg,#e06b6b,#ff9a9a)",
            WebkitBackgroundClip: "text",
            WebkitTextFillColor: "transparent",
          }}
        >
          {netPnl >= 0 ? "+" : ""}
          {fmtNum(netPnl)}
        </div>
      </div>

      <div
        style={styles.kpiCard}
        onMouseEnter={(e) =>
          ((e.currentTarget.style.boxShadow =
            "0 18px 36px -16px rgba(122,162,255,.35)"),
          (e.currentTarget.style.transform = "translateY(-2px)"))
        }
        onMouseLeave={(e) =>
          ((e.currentTarget.style.boxShadow = "none"),
          (e.currentTarget.style.transform = "none"))
        }
      >
        <div style={styles.kpiCardGlow} />
        <div style={styles.kpiLabel}>High</div>
        <div
          style={{
            ...styles.kpiValue,
            background: "linear-gradient(90deg,#9fc5ff,#7aa2ff)",
            WebkitBackgroundClip: "text",
            WebkitTextFillColor: "transparent",
          }}
        >
          {fmtNum(high)}
        </div>
      </div>

      <div
        style={styles.kpiCard}
        onMouseEnter={(e) =>
          ((e.currentTarget.style.boxShadow =
            "0 18px 36px -16px rgba(122,162,255,.35)"),
          (e.currentTarget.style.transform = "translateY(-2px)"))
        }
        onMouseLeave={(e) =>
          ((e.currentTarget.style.boxShadow = "none"),
          (e.currentTarget.style.transform = "none"))
        }
      >
        <div style={styles.kpiCardGlow} />
        <div style={styles.kpiLabel}>Low</div>
        <div
          style={{
            ...styles.kpiValue,
            background: "linear-gradient(90deg,#b8c6d9,#aeb8c5)",
            WebkitBackgroundClip: "text",
            WebkitTextFillColor: "transparent",
          }}
        >
          {fmtNum(low)}
        </div>
      </div>

      <div
        style={styles.kpiCard}
        onMouseEnter={(e) =>
          ((e.currentTarget.style.boxShadow =
            "0 18px 36px -16px rgba(224,107,107,.35)"),
          (e.currentTarget.style.transform = "translateY(-2px)"))
        }
        onMouseLeave={(e) =>
          ((e.currentTarget.style.boxShadow = "none"),
          (e.currentTarget.style.transform = "none"))
        }
      >
        <div style={styles.kpiCardGlow} />
        <div style={styles.kpiLabel}>Max Drawdown</div>
        <div
          style={{
            ...styles.kpiValue,
            background: "linear-gradient(90deg,#ff9a9a,#e06b6b)",
            WebkitBackgroundClip: "text",
            WebkitTextFillColor: "transparent",
          }}
        >
          {fmtNum(maxDrawdownPct)}%
        </div>
      </div>
    </div>
  );
};

const PositionsTable: React.FC<{
  title: string;
  rows: (OpenPosition | ClosedPosition)[];
  showRealized?: boolean;
}> = ({ title, rows, showRealized = false }) => {
  return (
    <div style={{ ...styles.chartContainer, marginBottom: 16 }}>
      <h2 style={styles.sectionTitle}>{title}</h2>
      <div style={{ overflowX: "auto" }}>
        <table style={styles.table}>
          <thead>
            <tr>
              <th style={styles.th}>Symbol</th>
              <th style={styles.th}>Side</th>
              <th style={styles.th}>Qty</th>
              <th style={styles.th}>Entry</th>
              <th style={styles.th}>{showRealized ? "Exit" : "Mark"}</th>
              <th style={styles.th}>Lev</th>
              <th style={styles.th}>Mode</th>
              <th style={styles.th}>Liq</th>
              <th style={styles.th}>
                {showRealized ? "Realized P&L" : "Unrealized P&L"}
              </th>
            </tr>
          </thead>
          <tbody>
            {rows.map((p, idx) => {
              const isClosedPosition = showRealized && "exitPrice" in p;
              const pnl = isClosedPosition
                ? (p as ClosedPosition).realizedPnl
                : (p as OpenPosition).unrealizedPnl;

              return (
                <tr key={idx}>
                  <td style={{ ...styles.td, fontWeight: 600 }}>{p.symbol}</td>
                  <td
                    style={{
                      ...styles.td,
                      color: p.side === "Buy" ? COLORS.accent2 : COLORS.danger,
                    }}
                  >
                    {p.side}
                  </td>
                  <td style={styles.td}>{fmtNum(p.qty, 4)}</td>
                  <td style={styles.td}>{fmtNum(p.entryPrice)}</td>
                  <td style={styles.td}>
                    {isClosedPosition
                      ? fmtNum((p as ClosedPosition).exitPrice)
                      : fmtNum((p as OpenPosition).markPrice)}
                  </td>
                  <td style={styles.td}>{p.leverage ?? "—"}</td>
                  <td style={styles.td}>{p.marginMode ?? "—"}</td>
                  <td style={styles.td}>{fmtNum(p.liqPrice)}</td>
                  <td
                    style={{
                      ...styles.td,
                      color: (pnl ?? 0) >= 0 ? COLORS.accent2 : COLORS.danger,
                      fontWeight: 700,
                    }}
                  >
                    {fmtNum(pnl)}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
};

/* =========================
   Main App Component
   ========================= */
const App: React.FC = () => {
  const [config, setConfig] = useState<Config | null>(null);
  const [days, setDays] = useState<number>(30);

  const [loading, setLoading] = useState<boolean>(false);
  const [error, setError] = useState<string | null>(null);
  const [updatedAt, setUpdatedAt] = useState<string>("");

  const [kpiData, setKpiData] = useState<KPIData | null>(null);
  const [chartData, setChartData] = useState<any[]>([]);
  const [usePercent, setUsePercent] = useState(false);
  const [openPositions, setOpenPositions] = useState<OpenPosition[]>([]);
  const [closedPositions, setClosedPositions] = useState<ClosedPosition[]>([]);

  const fetchingRef = useRef(false);
  const intervalRef = useRef<number | null>(null);

  // Domains for each axis independently
  const [yDomainEquity, yDomainMargin] = useMemo(() => {
    if (!chartData.length) return [["auto", "auto"], ["auto", "auto"]] as const;
    const eqVals = chartData
      .map((d) => d.equity)
      .filter((v) => Number.isFinite(v));
    const mgVals = chartData
      .map((d) => d.margin)
      .filter((v) => Number.isFinite(v));
    const pad = (min: number, max: number, p = 0.05) => {
      const span = Math.max(1, max - min);
      const a = min - span * p;
      const b = max + span * p;
      return [a, b];
    };
    const eqDom = eqVals.length
      ? pad(Math.min(...eqVals), Math.max(...eqVals), 0.05)
      : ["auto", "auto"];
    const mgDom = mgVals.length
      ? pad(Math.min(...mgVals), Math.max(...mgVals), 0.1)
      : ["auto", "auto"];
    return [eqDom, mgDom] as const;
  }, [chartData]);

  // Fetch data
  const fetchData = useCallback(async () => {
    if (!config || fetchingRef.current) return;
    fetchingRef.current = true;

    try {
      setLoading(true);
      setError(null);

      const now = new Date();
      const fromDate = new Date(now.getTime() - days * 24 * 60 * 60 * 1000);

      // (1) Берём «любой» ответ от API
      const [rawMetrics, openData, closedData] = await Promise.all([
        (api.getMetrics({ from: fromDate, to: now }) as unknown) as any,
        api.getPositionsOpen(),
        api.getPositionsClosed({ from: fromDate, to: now, limit: 100 }),
      ]);

      // (2) Унифицируем time-series → [{ t, equity, margin }]
      // NEW: корректное слияние с forward-fill equity, чтобы margin-точки
      // без equity не опускали линию equity в ноль.
      let unifiedSeries: { t: number; equity: number; margin: number }[] = [];

      if (
        Array.isArray(rawMetrics?.series) &&
        rawMetrics.series.length &&
        typeof rawMetrics.series[0]?.t !== "undefined"
      ) {
        unifiedSeries = rawMetrics.series as any[];
      } else if (
        Array.isArray(rawMetrics?.equitySeries) &&
        Array.isArray(rawMetrics?.marginSeries)
      ) {
        const e = rawMetrics.equitySeries as [number, number][];
        const m = rawMetrics.marginSeries as [number, number][];
        const map = new Map<number, { e?: number; m?: number }>();
        for (const [ts, v] of e)
          map.set(ts, { ...(map.get(ts) || {}), e: Number(v) });
        for (const [ts, v] of m)
          map.set(ts, { ...(map.get(ts) || {}), m: Number(v) });

        const keys = Array.from(map.keys()).sort((a, b) => a - b);
        let lastEq: number | undefined = undefined;

        for (const ts of keys) {
          const cur = map.get(ts)!;
          const eq = cur.e != null ? cur.e : lastEq != null ? lastEq : 0;
          const mg = cur.m != null ? cur.m : 0;
          unifiedSeries.push({ t: ts, equity: eq, margin: mg });
          if (cur.e != null) lastEq = cur.e;
        }
      } else {
        unifiedSeries = [];
      }

      // (3) KPI: поддержим оба варианта — "kpis" (бек) или "kpi" (фронт)
      const kpis = rawMetrics?.kpis ?? rawMetrics?.kpi ?? {};
      const equityVals = unifiedSeries
        .map((p) => p.equity)
        .filter((v) => Number.isFinite(v));
      const high = equityVals.length ? Math.max(...equityVals) : 0;
      const low = equityVals.length ? Math.min(...equityVals) : 0;

      // max drawdown (%), peak-to-trough
      let peak = -Infinity;
      let maxDdPct = 0; // ≤ 0
      for (const v of equityVals) {
        peak = Math.max(peak, v);
        if (peak > 0) {
          const dd = (v - peak) / peak; // ≤ 0
          maxDdPct = Math.min(maxDdPct, dd);
        }
      }
      const maxDrawdownPct = Math.abs(maxDdPct * 100);

      const netPnl =
        Number(kpis.realized ?? 0) + Number(kpis.unrealized ?? 0);

      const normalizedKpi = {
        netPnl,
        high,
        low,
        maxDrawdownAbs: high && low ? high - low : 0,
        maxDrawdownPct,
        updatedAt: rawMetrics?.updatedAt ?? new Date().toISOString(),
      };

      // (4) Подготовим данные для recharts с режимом USDT/%
      const firstEq =
        unifiedSeries.find((p) => Number.isFinite(p.equity))?.equity ?? 0;
      const firstMg =
        unifiedSeries.find((p) => Number.isFinite(p.margin))?.margin ?? 0;

      const transformedChartData = unifiedSeries.map((p) => {
        const rec: any = { t: p.t }; // ВАЖНО: числовой timestamp
        if (usePercent) {
          rec.equity = firstEq ? ((p.equity - firstEq) / firstEq) * 100 : 0;
          rec.margin = firstMg ? ((p.margin - firstMg) / (firstMg || 1)) * 100 : 0;
        } else {
          rec.equity = p.equity;
          rec.margin = p.margin;
        }
        return rec;
      });

      // (5) Обновляем стейт
      setKpiData(normalizedKpi);
      setChartData(transformedChartData);
      setOpenPositions(openData.items);
      setClosedPositions(closedData.items);
      setUpdatedAt(new Date().toLocaleTimeString("ru-RU"));
    } catch (err) {
      console.error("Fetch error:", err);
      setError(err instanceof Error ? err.message : "Unknown error");
    } finally {
      setLoading(false);
      fetchingRef.current = false;
    }
  }, [config, days, usePercent]);

  // Init config
  useEffect(() => {
    loadConfig().then((cfg) => {
      api.setBaseUrl(cfg.apiBaseUrl);
      setConfig(cfg);
    });
  }, []);

  // Initial fetch
  useEffect(() => {
    if (config) {
      fetchData();
    }
  }, [config, fetchData]);

  // Auto-refresh
  useEffect(() => {
    if (config && config.refreshMs > 0) {
      intervalRef.current = window.setInterval(fetchData, config.refreshMs);
      return () => {
        if (intervalRef.current) {
          clearInterval(intervalRef.current);
        }
      };
    }
  }, [config, fetchData]);

  return (
    <div style={styles.app}>
      <div style={styles.container}>
        {/* Header */}
        <div style={styles.header}>
          <h1 style={styles.title}>
            Trading Dashboard
            <span style={styles.status}>
              {loading ? "Loading..." : updatedAt && `Updated: ${updatedAt}`}
            </span>
            <span style={{ ...styles.status, color: COLORS.accent2 }}>
              Account ID: {ACCOUNT_ID}
            </span>
          </h1>
          <div style={styles.controls}>
            <select
              style={styles.select}
              value={days}
              onChange={(e) => setDays(Number(e.target.value))}
            >
              <option value={7}>7 days</option>
              <option value={30}>30 days</option>
              <option value={90}>90 days</option>
            </select>

            <button
              style={styles.button}
              onClick={() => setUsePercent((v) => !v)}
              onMouseEnter={(e) =>
                (e.currentTarget.style.background =
                  "linear-gradient(135deg,#8ab0ff,#6a8fdb)")
              }
              onMouseLeave={(e) =>
                (e.currentTarget.style.background =
                  "linear-gradient(135deg,#7aa2ff,#597bc8)")
              }
            >
              {usePercent ? "Show USDT" : "Show %"}
            </button>

            <button
              style={styles.button}
              onClick={fetchData}
              onMouseEnter={(e) =>
                (e.currentTarget.style.background =
                  "linear-gradient(135deg,#8ab0ff,#6a8fdb)")
              }
              onMouseLeave={(e) =>
                (e.currentTarget.style.background =
                  "linear-gradient(135deg,#7aa2ff,#597bc8)")
              }
            >
              Refresh
            </button>
          </div>
        </div>

        {/* Error */}
        {error && <div style={styles.error}>Error: {error}</div>}

        {/* KPI Cards */}
        <KPICards data={kpiData} />

        {/* Chart */}
        <div style={styles.chartContainer}>
          <h2 style={styles.sectionTitle}>Equity & Margin</h2>
          <ResponsiveContainer width="100%" height={300}>
            <LineChart data={chartData}>
              <defs>
                <linearGradient id="equityStroke" x1="0" y1="0" x2="1" y2="0">
                  <stop offset="0%" stopColor="#53f3cf" />
                  <stop offset="100%" stopColor="#7de7ff" />
                </linearGradient>
                <linearGradient id="marginStroke" x1="0" y1="0" x2="1" y2="0">
                  <stop offset="0%" stopColor="#7aa2ff" />
                  <stop offset="100%" stopColor="#9fc5ff" />
                </linearGradient>
              </defs>
              <CartesianGrid strokeDasharray="3 3" stroke={COLORS.grid} />
              <XAxis
                dataKey="t"
                type="number"
                scale="time"
                domain={["auto", "auto"]}
                tickFormatter={(ts: number) => formatDateTime(ts)}
                allowDuplicatedCategory={false}
                stroke={COLORS.sub}
                tick={{ fill: COLORS.sub, fontSize: 11 }}
              />

              {/* ЛЕВАЯ ось — Equity */}
              <YAxis
                yAxisId="equity"
                stroke={COLORS.sub}
                tick={{ fill: COLORS.sub, fontSize: 11 }}
                domain={yDomainEquity as any}
              />

              {/* ПРАВАЯ ось — Margin */}
              <YAxis
                yAxisId="margin"
                orientation="right"
                stroke={COLORS.sub}
                tick={{ fill: COLORS.sub, fontSize: 11 }}
                domain={yDomainMargin as any}
              />

              <Tooltip
                contentStyle={{
                  background: COLORS.panel,
                  border: `1px solid ${COLORS.border}`,
                  borderRadius: 8,
                }}
                labelStyle={{ color: COLORS.text }}
                itemStyle={{ color: COLORS.text }}
                labelFormatter={(ts: any) => formatDateTime(Number(ts))}
                formatter={(value: any) => fmtNum(Number(value))}
              />
              <Legend wrapperStyle={{ fontSize: 12 }} iconType="plainline" />
              <Line
                yAxisId="equity"
                type="monotone"
                dataKey="equity"
                stroke="url(#equityStroke)"
                strokeWidth={2.5}
                dot={false}
                connectNulls
                name="Equity"
              />
              <Line
                yAxisId="margin"
                type="monotone"
                dataKey="margin"
                stroke="url(#marginStroke)"
                strokeWidth={2.5}
                dot={false}
                connectNulls
                name="Margin"
              />
            </LineChart>
          </ResponsiveContainer>
        </div>

        {/* Tables */}
        <PositionsTable
          title="Текущие позиции"
          rows={openPositions}
          showRealized={false}
        />

        <PositionsTable
          title="Закрытые позиции"
          rows={closedPositions}
          showRealized={true}
        />
      </div>
    </div>
  );
};

export default App;
