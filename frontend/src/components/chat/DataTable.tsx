import React, { useState, useMemo } from 'react';
import { Download, Table as TableIcon, Search, X, ChevronUp, ChevronDown, ChevronsUpDown } from 'lucide-react';

type Column = { key: string; label: string };

export type DataTableProps = {
    rows: Record<string, unknown>[];
    meta?: { columns?: string[]; csv_filename?: string; csv_rows?: Record<string, unknown>[]; csv_row_count?: number };
    onDrillDown?: (type: 'ticket' | 'customer' | 'item', value: string) => void;
};

// ─── Column definitions ───────────────────────────────────────────────────────

const ALL_COLUMNS: Column[] = [
    { key: 'Date', label: 'Date' },
    { key: 'Ticket Number', label: 'Ticket #' },
    { key: 'Customer Number', label: 'Customer #' },
    { key: 'Invoice Number', label: 'Invoice #' },
    { key: 'Item Number', label: 'Item #' },
    { key: 'QTY', label: 'QTY' },
    { key: 'Unit Price', label: 'Unit Price' },
    { key: 'Corrected Unit Price', label: 'Corrected Unit Price' },
    { key: 'Credit Type', label: 'Credit Type' },
    { key: 'Credit Request Total', label: 'Credit Request Total' },
    { key: 'Issue Type', label: 'Issue Type' },
    { key: 'Reason for Credit', label: 'Reason for Credit' },
    { key: 'Requested By', label: 'Requested By' },
    { key: 'EDI Service Provider', label: 'EDI Service Provider' },
    { key: 'RTN_CR_No', label: 'RTN/CR #' },
    { key: 'Type', label: 'Type' },
    { key: 'Sales Rep', label: 'Sales Rep' },
    { key: 'Account', label: 'Account' },
    { key: 'Item', label: 'Item' },
    { key: 'Amount', label: 'Amount' },
    { key: 'Last Updated', label: 'Last Updated' },
    { key: 'Status', label: 'Status' },
];

function resolveColumns(columns?: string[]): Column[] {
    if (!columns?.length) return ALL_COLUMNS;
    return columns.map(key => ALL_COLUMNS.find(c => c.key === key) ?? { key, label: key });
}

// ─── Formatting utilities ─────────────────────────────────────────────────────

const INDY_TZ = 'America/Indiana/Indianapolis';
const DATE_ONLY_KEYS = new Set(['date']);
const DATETIME_KEYS = new Set(['last_status_time', 'last_status', 'last_updated', 'last_updated_at', 'update_timestamp', 'updated_at', 'created_at']);

const normalizeKey = (k: string) => k.trim().toLowerCase().replace(/[\s-]+/g, '_');

function parseDate(v: unknown): Date | null {
    if (v instanceof Date) return isNaN(v.getTime()) ? null : v;
    if (typeof v === 'number') { const d = new Date(v); return isNaN(d.getTime()) ? null : d; }
    if (typeof v === 'string') {
        if (/^\d{4}-\d{2}-\d{2}$/.test(v.trim())) return null;
        const d = new Date(v);
        return isNaN(d.getTime()) ? null : d;
    }
    return null;
}

function fmtDate(v: unknown): string {
    if (typeof v === 'string' && /^\d{4}-\d{2}-\d{2}$/.test(v.trim())) return v.trim();
    const d = parseDate(v);
    if (!d) return String(v);
    return new Intl.DateTimeFormat('en-CA', { timeZone: INDY_TZ, year: 'numeric', month: '2-digit', day: '2-digit' }).format(d);
}

function fmtDateTime(v: unknown): string {
    if (typeof v === 'string' && /^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}(?::\d{2})?$/.test(v.trim()))
        return v.trim().slice(0, 16);
    const d = parseDate(v);
    if (!d) return String(v);
    const parts = new Intl.DateTimeFormat('en-CA', {
        timeZone: INDY_TZ, year: 'numeric', month: '2-digit', day: '2-digit',
        hour: '2-digit', minute: '2-digit', hour12: false,
    }).formatToParts(d);
    const lk = Object.fromEntries(parts.map(p => [p.type, p.value]));
    return `${lk.year}-${lk.month}-${lk.day} ${lk.hour}:${lk.minute}`;
}

function fmtNumber(v: unknown): string {
    const n = Number(v);
    return Number.isFinite(n)
        ? new Intl.NumberFormat('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 }).format(n)
        : String(v);
}

function fmtZScore(v: unknown): string {
    const n = Number(v);
    return Number.isFinite(n) ? `${n >= 0 ? '+' : ''}${n.toFixed(2)}` : String(v);
}

function downloadCsv(rows: Record<string, unknown>[], filename: string, columns: Column[]) {
    const header = columns.map(c => c.label).join(',');
    const lines = rows.map(row =>
        columns.map(c => {
            const val = row[c.key];
            if (val == null) return '';
            return `"${String(val).replace(/"/g, '""').replace(/\n/g, ' ')}"`;
        }).join(',')
    );
    const blob = new Blob([[header, ...lines].join('\n')], { type: 'text/csv;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url; a.download = filename; a.click();
    URL.revokeObjectURL(url);
}

// ─── Sort helpers ─────────────────────────────────────────────────────────────

type SortDir = 'asc' | 'desc';

function sortRows(rows: Record<string, unknown>[], key: string, dir: SortDir): Record<string, unknown>[] {
    return [...rows].sort((a, b) => {
        const av = a[key], bv = b[key];
        if (av == null && bv == null) return 0;
        if (av == null) return 1;
        if (bv == null) return -1;
        const an = Number(av), bn = Number(bv);
        const numCompare = Number.isFinite(an) && Number.isFinite(bn) ? an - bn : 0;
        const cmp = numCompare !== 0 ? numCompare : String(av).localeCompare(String(bv));
        return dir === 'asc' ? cmp : -cmp;
    });
}

// ─── Drill-down column sets ───────────────────────────────────────────────────

const DRILLDOWN_COLS: Record<string, 'ticket' | 'customer' | 'item'> = {
    'Ticket Number': 'ticket',
    'Customer Number': 'customer',
    'Account': 'customer',
    'Item Number': 'item',
    'Item': 'item',
};

// ─── Component ────────────────────────────────────────────────────────────────

export function DataTable({ rows, meta, onDrillDown }: DataTableProps) {
    const columns = useMemo(() => resolveColumns(meta?.columns), [meta?.columns]);
    const [filter, setFilter] = useState('');
    const [sortKey, setSortKey] = useState<string | null>(null);
    const [sortDir, setSortDir] = useState<SortDir>('asc');

    const handleSort = (key: string) => {
        if (sortKey === key) {
            setSortDir(d => d === 'asc' ? 'desc' : 'asc');
        } else {
            setSortKey(key);
            setSortDir('asc');
        }
    };

    const filtered = useMemo(() => {
        if (!filter.trim()) return rows;
        const q = filter.toLowerCase();
        return rows.filter(row =>
            columns.some(col => {
                const v = row[col.key];
                return v != null && String(v).toLowerCase().includes(q);
            })
        );
    }, [rows, columns, filter]);

    const sorted = useMemo(() =>
        sortKey ? sortRows(filtered, sortKey, sortDir) : filtered,
        [filtered, sortKey, sortDir]
    );

    const csvSource = meta?.csv_rows?.length ? meta.csv_rows : rows;
    const csvFilename = meta?.csv_filename;

    return (
        <div className="w-full max-w-4xl bg-obsidian-950/40 border border-white/[0.08] rounded-3xl overflow-hidden shadow-2xl backdrop-blur-xl">
            {/* Header */}
            <div className="px-6 py-4 border-b border-white/[0.04] flex flex-col sm:flex-row sm:items-center gap-3 bg-gradient-to-b from-white/[0.02] to-transparent">
                <div className="flex items-center gap-3 text-sm font-semibold text-white flex-1 min-w-0">
                    <div className="w-8 h-8 rounded-lg bg-cyan-500/10 border border-cyan-500/20 flex items-center justify-center text-cyan-400 flex-shrink-0">
                        <TableIcon className="w-4 h-4" />
                    </div>
                    <span className="font-display tracking-wide">Data Preview</span>
                    <span className="text-slate-500 font-normal font-mono text-xs">
                        ({sorted.length}{sorted.length !== rows.length ? ` of ${rows.length}` : ''} rows)
                    </span>
                </div>

                <div className="flex items-center gap-2 flex-shrink-0">
                    {/* Filter input */}
                    <div className="relative flex items-center">
                        <Search className="absolute left-2.5 w-3.5 h-3.5 text-slate-500 pointer-events-none" />
                        <input
                            type="text"
                            value={filter}
                            onChange={e => setFilter(e.target.value)}
                            placeholder="Filter rows…"
                            className="pl-8 pr-7 py-1.5 text-xs rounded-lg bg-white/[0.04] border border-white/[0.08] text-slate-200 placeholder-slate-600 focus:outline-none focus:border-cyan-500/40 focus:bg-white/[0.06] transition-colors w-36"
                        />
                        {filter && (
                            <button
                                onClick={() => setFilter('')}
                                className="absolute right-2 text-slate-500 hover:text-slate-300 transition-colors"
                            >
                                <X className="w-3 h-3" />
                            </button>
                        )}
                    </div>

                    {csvFilename && (
                        <button
                            onClick={() => downloadCsv(csvSource, csvFilename, columns)}
                            className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg bg-cyan-500/10 hover:bg-cyan-500/20 border border-cyan-500/20 text-xs font-medium text-cyan-300 transition-colors whitespace-nowrap"
                        >
                            <Download className="w-3.5 h-3.5" />
                            CSV{typeof meta?.csv_row_count === 'number' ? ` (${meta.csv_row_count})` : ''}
                        </button>
                    )}
                </div>
            </div>

            {/* Table */}
            <div className="overflow-x-auto overflow-y-auto max-h-[520px] overscroll-x-contain">
                {sorted.length === 0 ? (
                    <div className="py-12 text-center text-sm text-slate-500">
                        {filter ? 'No rows match your filter.' : 'No data.'}
                    </div>
                ) : (
                    <table className="min-w-[1100px] w-max divide-y divide-white/10">
                        <thead className="sticky top-0 z-10">
                            <tr>
                                {columns.map(col => {
                                    const isSorted = sortKey === col.key;
                                    return (
                                        <th
                                            key={col.key}
                                            onClick={() => handleSort(col.key)}
                                            className="px-6 py-3.5 text-left cursor-pointer select-none bg-obsidian-950/90 backdrop-blur-md group/th hover:bg-white/[0.03] transition-colors"
                                        >
                                            <div className="flex items-center gap-1.5">
                                                <span className={`text-[10px] font-bold uppercase tracking-[0.2em] whitespace-nowrap transition-colors ${isSorted ? 'text-cyan-400' : 'text-slate-500 group-hover/th:text-slate-400'}`}>
                                                    {col.label}
                                                </span>
                                                {isSorted ? (
                                                    sortDir === 'asc'
                                                        ? <ChevronUp className="w-3 h-3 text-cyan-400" />
                                                        : <ChevronDown className="w-3 h-3 text-cyan-400" />
                                                ) : (
                                                    <ChevronsUpDown className="w-3 h-3 text-slate-700 opacity-0 group-hover/th:opacity-100 transition-opacity" />
                                                )}
                                            </div>
                                        </th>
                                    );
                                })}
                            </tr>
                        </thead>
                        <tbody className="divide-y divide-white/5 bg-transparent">
                            {sorted.map((row, rowIdx) => (
                                <tr key={rowIdx} className="hover:bg-white/[0.03] transition-colors group/row">
                                    {columns.map(col => {
                                        const value = row[col.key];
                                        const nk = normalizeKey(col.key);
                                        const drillType = DRILLDOWN_COLS[col.key];

                                        let content: React.ReactNode;

                                        if (value == null || value === '') {
                                            content = <span className="text-slate-600 italic text-xs">N/A</span>;
                                        } else if (drillType && onDrillDown) {
                                            content = (
                                                <button
                                                    onClick={() => onDrillDown(drillType, String(value))}
                                                    className="text-cyan-400 hover:text-cyan-300 hover:underline underline-offset-2 font-mono text-sm transition-colors"
                                                    title={`Drill into ${drillType} ${value}`}
                                                >
                                                    {String(value)}
                                                </button>
                                            );
                                        } else if (DATE_ONLY_KEYS.has(nk)) {
                                            content = <span className="font-mono text-sm">{fmtDate(value)}</span>;
                                        } else if (DATETIME_KEYS.has(nk)) {
                                            content = <span className="font-mono text-sm">{fmtDateTime(value)}</span>;
                                        } else if (col.key === 'Amount' || col.key === 'Credit Request Total') {
                                            content = <span className="text-emerald-400 font-medium font-mono text-sm">{fmtNumber(value)}</span>;
                                        } else if (col.key === 'Z Score' || col.key === 'z_score') {
                                            const tone = String(value).startsWith('-') ? 'text-cyan-400' : 'text-rose-400';
                                            content = <span className={`font-mono text-sm ${tone}`}>{fmtZScore(value)}</span>;
                                        } else if (col.key === 'Anomaly Flag' || col.key === 'anomaly_reason') {
                                            const flag = String(value).toLowerCase();
                                            const badge = flag.includes('hard')
                                                ? 'bg-rose-500/20 text-rose-300 border-rose-500/30'
                                                : flag.includes('both')
                                                    ? 'bg-purple-500/20 text-purple-300 border-purple-500/30'
                                                    : flag.includes('stat')
                                                        ? 'bg-amber-500/20 text-amber-300 border-amber-500/30'
                                                        : 'bg-slate-500/20 text-slate-300 border-slate-500/30';
                                            content = (
                                                <span className={`inline-flex items-center px-2 py-0.5 rounded text-xs font-medium border ${badge}`}>
                                                    {String(value)}
                                                </span>
                                            );
                                        } else if (col.key === 'Status') {
                                            content = (
                                                <span className="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-blue-500/10 text-blue-300 border border-blue-500/20 max-w-[300px] truncate" title={String(value)}>
                                                    {String(value)}
                                                </span>
                                            );
                                        } else {
                                            content = <span className="text-sm">{String(value)}</span>;
                                        }

                                        return (
                                            <td key={col.key} className="px-6 py-3.5 text-slate-300 whitespace-nowrap">
                                                {content}
                                            </td>
                                        );
                                    })}
                                </tr>
                            ))}
                        </tbody>
                    </table>
                )}
            </div>
        </div>
    );
}
