import { useMemo, useState } from 'react';
import {
  AlertTriangle,
  Calendar,
  Activity,
  Users,
  Package,
  Briefcase,
  Target,
  ShieldAlert,
  BarChart2,
  ChevronRight,
} from 'lucide-react';

type RawRow = Record<string, unknown>;

type AnomalyItem = {
  name: string;
  amount: number;
};

type NormalizedRow = {
  ticketId: string;
  date: string;
  amount: number;
  zScore: number | null;
  flag: string;
  customer: string;
  item: string;
  rep: string;
};

type AnomalyScanProps = {
  rows?: RawRow[];
  csvRows?: RawRow[];
  onReviewTicket?: (ticketId: string) => void;
};

const formatCurrency = (val: number) =>
  new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD' }).format(val);

const toNumber = (value: unknown): number => {
  if (typeof value === 'number') {
    return Number.isFinite(value) ? value : 0;
  }
  const text = String(value ?? '').replaceAll(',', '').replace('$', '').trim();
  if (!text) return 0;
  const parsed = Number(text);
  return Number.isFinite(parsed) ? parsed : 0;
};

const toText = (value: unknown, fallback = 'Unknown'): string => {
  const text = String(value ?? '').trim();
  return text || fallback;
};

const normalizeDateText = (value: unknown): string => {
  const text = String(value ?? '').trim();
  if (!text) return 'Unknown';
  return text.split('T')[0].split(' ')[0] || text;
};

const normalizeRow = (row: RawRow): NormalizedRow => ({
  ticketId: toText(row['Ticket Number'] ?? row['ticket_number']),
  date: normalizeDateText(row['Date'] ?? row['date']),
  amount: toNumber(row['Credit Request Total'] ?? row['credit_request_total']),
  zScore: (() => {
    const n = toNumber(row['Z Score'] ?? row['z_score']);
    return n === 0 && !String(row['Z Score'] ?? row['z_score'] ?? '').trim() ? null : n;
  })(),
  flag: toText(row['Anomaly Flag'] ?? row['anomaly_reason'], 'Unknown'),
  customer: toText(row['Customer Number'] ?? row['customer_number']),
  item: toText(row['Item Number'] ?? row['item_number']),
  rep: toText(row['Sales Rep'] ?? row['sales_rep']),
});

const topByDimension = (rows: NormalizedRow[], key: 'customer' | 'item' | 'rep', prefix?: string): AnomalyItem[] => {
  const map = new Map<string, number>();
  for (const row of rows) {
    const name = key === 'customer' ? row.customer : key === 'item' ? row.item : row.rep;
    map.set(name, (map.get(name) || 0) + row.amount);
  }
  return Array.from(map.entries())
    .sort((a, b) => b[1] - a[1])
    .slice(0, 5)
    .map(([name, amount]) => ({
      name: prefix ? `${prefix}${name}` : name,
      amount,
    }));
};

const minMaxWindow = (rows: NormalizedRow[]) => {
  const dates = rows
    .map((row) => row.date)
    .filter((date) => /^\d{4}-\d{2}-\d{2}$/.test(date))
    .sort();
  if (!dates.length) return { start: 'n/a', end: 'n/a' };
  return { start: dates[0], end: dates[dates.length - 1] };
};

export function AnomalyScan({ rows = [], csvRows = [], onReviewTicket }: AnomalyScanProps) {
  const [activeTab, setActiveTab] = useState<'overview' | 'extreme'>('overview');

  const previewRows = useMemo(() => rows.map(normalizeRow), [rows]);
  const fullRows = useMemo(() => (csvRows.length > 0 ? csvRows : rows).map(normalizeRow), [csvRows, rows]);

  const anomalyCount = fullRows.length;
  const totalImpact = fullRows.reduce((sum, row) => sum + row.amount, 0);
  const window = minMaxWindow(fullRows);

  const customers = topByDimension(fullRows, 'customer');
  const items = topByDimension(fullRows, 'item', 'Item ');
  const reps = topByDimension(fullRows, 'rep');

  const maxCustomer = Math.max(1, ...customers.map((c) => c.amount));
  const maxItem = Math.max(1, ...items.map((i) => i.amount));
  const maxRep = Math.max(1, ...reps.map((r) => r.amount));

  return (
    <div className="w-full max-w-6xl mx-auto space-y-6">
      <div className="bg-obsidian-950/60 backdrop-blur-2xl border border-white/[0.08] rounded-3xl overflow-hidden shadow-2xl relative transition-all group/container">
        <div className="absolute inset-0 bg-gradient-to-br from-rose-500/[0.04] via-transparent to-transparent opacity-100 pointer-events-none" />

        <div className="relative p-6 md:p-8">
          <div className="flex flex-col md:flex-row md:items-start justify-between gap-6 mb-8 border-b border-white/[0.06] pb-8">
            <div className="flex items-center gap-4">
              <div className="relative">
                <div className="w-14 h-14 rounded-2xl bg-gradient-to-br from-obsidian-800 to-obsidian-900 border border-white/[0.08] flex items-center justify-center flex-shrink-0 shadow-[0_0_20px_rgba(244,63,94,0.1)] transition-colors group-hover/container:border-rose-500/30">
                  <AlertTriangle className="w-7 h-7 text-rose-400 drop-shadow-[0_0_8px_rgba(244,63,94,0.4)]" />
                </div>
                <div className="absolute -top-1 -right-1 w-3 h-3 bg-rose-500 rounded-full border-2 border-obsidian-900 animate-pulse" />
              </div>
              <div className="flex flex-col">
                <div className="flex items-center gap-3">
                  <h2 className="text-3xl font-bold font-display tracking-tight text-white drop-shadow-sm">
                    Credit Anomaly Scan
                  </h2>
                  <span className="px-3 py-1 rounded-full text-[10px] font-bold bg-rose-500/10 border border-rose-500/30 text-rose-400 uppercase tracking-widest flex items-center gap-1.5 shadow-[0_0_10px_rgba(244,63,94,0.2)]">
                    <Activity className="w-3 h-3" /> Live
                  </span>
                </div>
                <div className="flex items-center gap-2 mt-1.5 text-slate-400 text-sm font-medium">
                  <Calendar className="w-4 h-4 text-slate-500" />
                  <span>Last 90 Days</span>
                  <span className="text-slate-600 mx-1">•</span>
                  <span className="font-mono text-xs">{window.start} → {window.end}</span>
                </div>
              </div>
            </div>

            <div className="flex gap-4">
              <div className="bg-obsidian-900/80 border border-white/[0.06] rounded-2xl p-4 flex flex-col min-w-[140px] hover:bg-obsidian-800 transition-colors group/kpi">
                <span className="text-[10px] font-bold text-slate-500 uppercase tracking-[0.2em] mb-1">Anomalies Found</span>
                <span className="text-3xl font-bold font-display text-white group-hover/kpi:text-rose-400 transition-colors">{anomalyCount}</span>
              </div>
              <div className="bg-obsidian-900/80 border border-white/[0.06] rounded-2xl p-4 flex flex-col min-w-[180px] hover:bg-obsidian-800 transition-colors group/kpi">
                <span className="text-[10px] font-bold text-slate-500 uppercase tracking-[0.2em] mb-1">Total Impact</span>
                <span className="text-3xl font-bold font-display text-rose-400 group-hover/kpi:text-rose-300 transition-colors tracking-tight">
                  {formatCurrency(totalImpact)}
                </span>
              </div>
            </div>
          </div>

          <div className="mb-10 bg-gradient-to-r from-indigo-500/[0.08] to-transparent rounded-2xl border border-indigo-500/20 p-5 relative overflow-hidden group/rules">
            <div className="absolute top-0 right-0 p-4 opacity-5 group-hover/rules:opacity-10 transition-opacity">
              <Target className="w-32 h-32 text-indigo-400 -mt-8 -mr-8" />
            </div>
            <div className="relative z-10 flex flex-col md:flex-row md:items-center gap-6">
              <div className="flex items-center gap-3">
                <div className="p-2 rounded-xl bg-indigo-500/10 border border-indigo-500/20">
                  <ShieldAlert className="w-5 h-5 text-indigo-400" />
                </div>
                <div>
                  <h3 className="text-[11px] font-bold text-indigo-300 uppercase tracking-[0.2em]">Detection Rules Applied</h3>
                  <p className="text-xs text-indigo-200/60 mt-0.5">Dual-factor anomaly scanning active</p>
                </div>
              </div>

              <div className="h-10 w-px bg-indigo-500/20 hidden md:block" />

              <div className="flex-1 grid grid-cols-1 md:grid-cols-2 gap-4">
                <div className="flex items-start gap-3 bg-white/[0.02] p-3 rounded-xl border border-white/[0.04]">
                  <BarChart2 className="w-4 h-4 text-cyan-400 mt-0.5" />
                  <div>
                    <span className="block text-sm font-semibold text-slate-300">Statistical Outlier</span>
                    <span className="block text-xs font-mono text-slate-500 mt-1">
                      Amt ≥ <span className="text-cyan-400">$500</span> WITH |z-score| ≥ <span className="text-cyan-400">3.0</span>
                    </span>
                  </div>
                </div>
                <div className="flex items-start gap-3 bg-white/[0.02] p-3 rounded-xl border border-white/[0.04]">
                  <AlertTriangle className="w-4 h-4 text-rose-400 mt-0.5" />
                  <div>
                    <span className="block text-sm font-semibold text-slate-300">Management Hard Cap</span>
                    <span className="block text-xs font-mono text-slate-500 mt-1">
                      Any single credit ≥ <span className="text-rose-400">$2,500</span>
                    </span>
                  </div>
                </div>
              </div>
            </div>
          </div>

          <div className="flex gap-2 mb-6 border-b border-white/[0.06] pb-px">
            <button
              onClick={() => setActiveTab('overview')}
              className={`px-4 py-2.5 text-sm font-bold transition-all relative ${activeTab === 'overview' ? 'text-white' : 'text-slate-500 hover:text-slate-300'}`}
            >
              Distribution Overview
              {activeTab === 'overview' && (
                <div className="absolute bottom-0 left-0 w-full h-0.5 bg-rose-500 shadow-[0_0_10px_rgba(244,63,94,0.5)] rounded-t-full" />
              )}
            </button>
            <button
              onClick={() => setActiveTab('extreme')}
              className={`px-4 py-2.5 text-sm font-bold transition-all relative flex items-center gap-2 ${activeTab === 'extreme' ? 'text-white' : 'text-slate-500 hover:text-slate-300'}`}
            >
              Extreme Anomalies
              <span className="px-1.5 py-0.5 rounded text-[9px] bg-white/10 text-white font-mono">{previewRows.length}</span>
              {activeTab === 'extreme' && (
                <div className="absolute bottom-0 left-0 w-full h-0.5 bg-rose-500 shadow-[0_0_10px_rgba(244,63,94,0.5)] rounded-t-full" />
              )}
            </button>
          </div>

          {activeTab === 'overview' && (
            <div className="grid grid-cols-1 lg:grid-cols-3 gap-6 animate-fade-in">
              <div className="bg-obsidian-900/50 rounded-2xl border border-white/[0.04] overflow-hidden flex flex-col group/col hover:bg-obsidian-900/80 transition-colors">
                <div className="p-4 border-b border-white/[0.04] flex items-center gap-3 bg-white/[0.02]">
                  <div className="p-1.5 rounded-lg bg-emerald-500/10 border border-emerald-500/20 text-emerald-400">
                    <Users className="w-4 h-4" />
                  </div>
                  <h3 className="text-sm font-bold text-slate-200">Top Customers</h3>
                </div>
                <div className="p-4 space-y-4">
                  {customers.map((c, i) => (
                    <div key={`${c.name}-${i}`} className="group/item cursor-default">
                      <div className="flex justify-between items-baseline mb-1.5">
                        <span className="text-xs font-semibold text-slate-300 group-hover/item:text-white transition-colors">{c.name}</span>
                        <span className="text-xs font-mono text-rose-300">{formatCurrency(c.amount)}</span>
                      </div>
                      <div className="h-1.5 w-full bg-obsidian-950 rounded-full overflow-hidden border border-white/[0.02]">
                        <div
                          className="h-full bg-emerald-500 rounded-full transition-all duration-1000 ease-out group-hover/item:bg-emerald-400 group-hover/item:shadow-[0_0_8px_rgba(16,185,129,0.5)]"
                          style={{ width: `${(c.amount / maxCustomer) * 100}%` }}
                        />
                      </div>
                    </div>
                  ))}
                </div>
              </div>

              <div className="bg-obsidian-900/50 rounded-2xl border border-white/[0.04] overflow-hidden flex flex-col group/col hover:bg-obsidian-900/80 transition-colors">
                <div className="p-4 border-b border-white/[0.04] flex items-center gap-3 bg-white/[0.02]">
                  <div className="p-1.5 rounded-lg bg-cyan-500/10 border border-cyan-500/20 text-cyan-400">
                    <Package className="w-4 h-4" />
                  </div>
                  <h3 className="text-sm font-bold text-slate-200">Top Items</h3>
                </div>
                <div className="p-4 space-y-4">
                  {items.map((item, i) => (
                    <div key={`${item.name}-${i}`} className="group/item cursor-default">
                      <div className="flex justify-between items-baseline mb-1.5">
                        <span className="text-xs font-semibold text-slate-300 group-hover/item:text-white transition-colors truncate max-w-[140px]" title={item.name}>
                          {item.name}
                        </span>
                        <span className="text-xs font-mono text-rose-300">{formatCurrency(item.amount)}</span>
                      </div>
                      <div className="h-1.5 w-full bg-obsidian-950 rounded-full overflow-hidden border border-white/[0.02]">
                        <div
                          className="h-full bg-cyan-500 rounded-full transition-all duration-1000 ease-out group-hover/item:bg-cyan-400 group-hover/item:shadow-[0_0_8px_rgba(6,182,212,0.5)]"
                          style={{ width: `${(item.amount / maxItem) * 100}%` }}
                        />
                      </div>
                    </div>
                  ))}
                </div>
              </div>

              <div className="bg-obsidian-900/50 rounded-2xl border border-white/[0.04] overflow-hidden flex flex-col group/col hover:bg-obsidian-900/80 transition-colors">
                <div className="p-4 border-b border-white/[0.04] flex items-center gap-3 bg-white/[0.02]">
                  <div className="p-1.5 rounded-lg bg-orange-500/10 border border-orange-500/20 text-orange-400">
                    <Briefcase className="w-4 h-4" />
                  </div>
                  <h3 className="text-sm font-bold text-slate-200">Top Sales Reps</h3>
                </div>
                <div className="p-4 space-y-4">
                  {reps.map((rep, i) => (
                    <div key={`${rep.name}-${i}`} className="group/item cursor-default">
                      <div className="flex justify-between items-baseline mb-1.5">
                        <span className="text-[11px] font-semibold text-slate-300 group-hover/item:text-white transition-colors truncate max-w-[140px]" title={rep.name}>
                          {rep.name}
                        </span>
                        <span className="text-xs font-mono text-rose-300">{formatCurrency(rep.amount)}</span>
                      </div>
                      <div className="h-1.5 w-full bg-obsidian-950 rounded-full overflow-hidden border border-white/[0.02]">
                        <div
                          className="h-full bg-orange-500 rounded-full transition-all duration-1000 ease-out group-hover/item:bg-orange-400 group-hover/item:shadow-[0_0_8px_rgba(249,115,22,0.5)]"
                          style={{ width: `${(rep.amount / maxRep) * 100}%` }}
                        />
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            </div>
          )}

          {activeTab === 'extreme' && (
            <div className="bg-obsidian-900/40 border border-white/[0.04] rounded-2xl overflow-hidden animate-fade-in">
              <div className="px-6 py-4 border-b border-white/[0.04] bg-white/[0.02]">
                <h3 className="text-sm font-bold text-white flex items-center gap-2">
                  <Activity className="w-4 h-4 text-cyan-400" />
                  Data Preview
                  <span className="text-slate-500 font-mono text-xs font-normal">Ranked by severity</span>
                </h3>
              </div>
              <div className="overflow-x-auto">
                <table className="w-full text-left border-collapse">
                  <thead className="bg-obsidian-950/80">
                    <tr>
                      <th className="px-6 py-4 text-[10px] uppercase tracking-[0.2em] font-bold text-slate-500">Ticket ID</th>
                      <th className="px-6 py-4 text-[10px] uppercase tracking-[0.2em] font-bold text-slate-500">Date</th>
                      <th className="px-6 py-4 text-[10px] uppercase tracking-[0.2em] font-bold text-slate-500">Amount</th>
                      <th className="px-6 py-4 text-[10px] uppercase tracking-[0.2em] font-bold text-slate-500">Z-Score</th>
                      <th className="px-6 py-4 text-[10px] uppercase tracking-[0.2em] font-bold text-slate-500">Anomaly Flag</th>
                      <th className="px-6 py-4 text-[10px] uppercase tracking-[0.2em] font-bold text-slate-500">Action</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-white/[0.02] text-sm">
                    {previewRows.map((row, i) => (
                      <tr key={`${row.ticketId}-${row.date}-${i}`} className="hover:bg-white/[0.02] transition-colors group/row">
                        <td className="px-6 py-4 font-semibold text-white">{row.ticketId}</td>
                        <td className="px-6 py-4 text-slate-400 font-mono text-xs">{row.date}</td>
                        <td className="px-6 py-4 font-mono text-rose-300 font-bold">{formatCurrency(row.amount)}</td>
                        <td className="px-6 py-4 font-mono text-cyan-300 font-semibold">{row.zScore == null ? 'n/a' : row.zScore.toFixed(2)}</td>
                        <td className="px-6 py-4">
                          <span className={`inline-flex items-center px-2 py-0.5 rounded text-[10px] font-bold uppercase tracking-widest border ${
                            row.flag.toLowerCase().includes('both')
                              ? 'bg-purple-500/10 text-purple-300 border-purple-500/20'
                              : row.flag.toLowerCase().includes('cap')
                                ? 'bg-rose-500/10 text-rose-300 border-rose-500/20'
                                : 'bg-cyan-500/10 text-cyan-300 border-cyan-500/20'
                          }`}>
                            {row.flag}
                          </span>
                        </td>
                        <td className="px-6 py-4">
                          <button
                            type="button"
                            onClick={() => {
                              if (!onReviewTicket) return;
                              const ticket = row.ticketId.trim();
                              if (!ticket || ticket.toLowerCase() === 'unknown') return;
                              onReviewTicket(ticket);
                            }}
                            disabled={!onReviewTicket || !row.ticketId || row.ticketId.toLowerCase() === 'unknown'}
                            className="text-xs font-semibold text-slate-400 hover:text-white disabled:text-slate-600 disabled:cursor-not-allowed flex items-center gap-1 group/btn transition-colors"
                          >
                            Review <ChevronRight className="w-3 h-3 group-hover/btn:translate-x-0.5 transition-transform" />
                          </button>
                        </td>
                      </tr>
                    ))}
                    {previewRows.length === 0 && (
                      <tr>
                        <td colSpan={6} className="px-6 py-8 text-center text-slate-500 text-sm">
                          No anomaly rows returned.
                        </td>
                      </tr>
                    )}
                  </tbody>
                </table>
              </div>
            </div>
          )}

        </div>
      </div>
    </div>
  );
}
