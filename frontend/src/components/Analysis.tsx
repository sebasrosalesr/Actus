import {
    Activity, AlertCircle, Calendar,
    Clock, Tags, FileText, CheckCircle2,
    Zap, Hash, Target, Sparkles, TrendingUp
} from 'lucide-react';

export type TicketAnalysisMeta = {
    ticket_id?: string;
    primary_root_cause?: string;
    supporting_root_causes?: string[];
    sales_reps?: string[];
    account_prefixes?: string[];
    credit_total?: number;
    line_count?: number;
    entered_to_credited_days?: number | null;
    investigation_to_credited_days?: number | null;
    days_open?: number | null;
    days_pending_billing_to_credit?: number | null;
    threshold_exceeded?: boolean;
    is_credited?: boolean;
    last_status_timestamp?: string | null;
    last_status_event_type?: string | null;
    invoice_numbers?: string[];
    item_numbers?: string[];
    investigation_highlights?: string[];
    investigation_highlights_source?: string | null;
    investigation_highlights_model?: string | null;
    answer?: string;
};

export type ItemAnalysisMeta = {
    item_number?: string;
    ticket_count?: number;
    invoice_count?: number;
    line_count?: number;
    total_credit?: number;
    root_cause_counts?: Record<string, number>;
    root_cause_counts_all?: Record<string, number>;
    sales_rep_counts?: Record<string, number>;
    account_prefix_counts?: Record<string, number>;
    tickets?: string[];
    invoices?: string[];
    first_seen?: string | null;
    last_seen?: string | null;
    answer?: string;
};

const formatCurrency = (value: number | null | undefined) => {
    if (value == null) return '$0.00';
    return new Intl.NumberFormat('en-US', {
        style: 'currency',
        currency: 'USD',
    }).format(value);
};

const previewList = (list: string[] | undefined, max: number) => {
    if (!list || list.length === 0) return 'none';
    const sliced = list.slice(0, max);
    const suffix = list.length > max ? ` +${list.length - max} more` : '';
    return sliced.join(', ') + suffix;
};

const sortCountEntries = (values?: Record<string, number>) => {
    const entries = Object.entries(values ?? {});
    entries.sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]));
    return entries;
};

export function Analysis({
    data,
    suggestions = [],
    onSuggestionClick,
}: {
    data: TicketAnalysisMeta;
    suggestions?: { label?: string; prefix?: string }[];
    onSuggestionClick: (query: string) => void;
}) {
    const highlightSource = String(data.investigation_highlights_source || '').trim();
    const highlightModel = String(data.investigation_highlights_model || '').trim();
    const highlightSourceLabel =
        highlightSource === 'openrouter_primary'
            ? 'OpenRouter'
            : highlightSource === 'openrouter_fallback'
                ? 'OpenRouter (fallback)'
                : highlightSource === 'heuristic'
                    ? 'Heuristic fallback'
                    : '';

    return (
        <div className="w-full max-w-5xl relative group/analysis">
            {/* Background Glow Effect */}
            <div className="absolute -inset-0.5 bg-gradient-to-br from-cyan-500/20 via-indigo-500/10 to-transparent rounded-3xl blur-xl opacity-50 group-hover/analysis:opacity-100 transition duration-700 pointer-events-none"></div>

            <div className="relative bg-obsidian-950/80 border border-white/[0.08] rounded-3xl overflow-hidden shadow-2xl backdrop-blur-xl transition-all duration-500 hover:border-cyan-500/30">
                {/* Header Section */}
                <div className="p-6 md:p-8 flex flex-col md:flex-row md:items-center justify-between gap-6 border-b border-white/[0.04] bg-gradient-to-b from-white/[0.02] to-transparent">
                    <div className="flex items-start gap-4">
                        <div className="w-12 h-12 rounded-2xl bg-gradient-to-br from-cyan-400/20 to-indigo-600/20 border border-cyan-500/30 flex items-center justify-center flex-shrink-0 shadow-[0_0_15px_rgba(34,211,238,0.15)] group-hover/analysis:shadow-[0_0_25px_rgba(34,211,238,0.3)] transition-all">
                            <Activity className="w-6 h-6 text-cyan-400" />
                        </div>
                        <div>
                            <div className="flex items-center gap-3">
                                <h2 className="text-2xl font-bold font-display tracking-tight text-white drop-shadow-sm">
                                    Ticket <span className="text-transparent bg-clip-text bg-gradient-to-r from-cyan-400 to-indigo-400">{data.ticket_id || 'Unknown'}</span> Analysis
                                </h2>
                                {data.is_credited ? (
                                    <span className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full bg-emerald-500/10 border border-emerald-500/20 text-emerald-400 text-xs font-semibold shadow-[0_0_10px_rgba(16,185,129,0.1)]">
                                        <CheckCircle2 className="w-3 h-3" /> Credited
                                    </span>
                                ) : (
                                    <span className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full bg-amber-500/10 border border-amber-500/20 text-amber-400 text-xs font-semibold">
                                        <Clock className="w-3 h-3" /> Open
                                    </span>
                                )}
                            </div>
                            <p className="text-sm text-slate-400 mt-1.5 flex items-center gap-2 font-medium">
                                <Hash className="w-3.5 h-3.5" />
                                {Number(data.line_count ?? 0)} invoice lines
                            </p>
                        </div>
                    </div>
                    
                    {/* Primary Metric: Credit Total */}
                    <div className="px-6 py-4 rounded-2xl bg-obsidian-900 border border-emerald-500/20 shadow-[0_0_20px_rgba(16,185,129,0.05)] flex flex-col items-end min-w-[180px] group-hover/analysis:border-emerald-500/40 transition-colors">
                        <span className="text-[10px] uppercase tracking-[0.2em] text-emerald-500/80 font-bold mb-1">Total Credit</span>
                        <div className="text-emerald-400 font-mono text-3xl font-bold tracking-tight">
                            {formatCurrency(data.credit_total)}
                        </div>
                    </div>
                </div>

                <div className="p-6 md:p-8 space-y-8">
                    {/* Root Causes Section */}
                    <div className="flex flex-col gap-3">
                        <h3 className="text-xs font-bold text-slate-500 uppercase tracking-[0.15em] flex items-center gap-2">
                            <Target className="w-4 h-4" /> Root Cause Identification
                        </h3>
                        <div className="flex flex-wrap gap-2.5">
                            <div className="px-3 py-1.5 rounded-xl bg-gradient-to-r from-amber-500/10 to-orange-500/10 border border-amber-500/30 text-amber-300 text-sm font-medium shadow-[0_0_10px_rgba(245,158,11,0.1)] hover:scale-105 transition-transform cursor-default">
                                <span className="text-amber-500/60 text-xs uppercase tracking-wider mr-2 font-bold">Primary</span>
                                {data.primary_root_cause || 'unidentified'}
                            </div>
                            {(data.supporting_root_causes ?? []).map((root, idx) => (
                                <div key={idx} className="px-3 py-1.5 rounded-xl bg-white/[0.03] border border-white/10 text-slate-300 text-sm font-medium hover:bg-white/[0.06] hover:border-cyan-500/30 hover:text-cyan-200 transition-all cursor-default">
                                    {root}
                                </div>
                            ))}
                        </div>
                    </div>

                    {/* Meta Info Grid */}
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                        {/* Timeline & Metrics Card */}
                        <div className="bg-obsidian-900/50 border border-white/[0.05] rounded-2xl p-5 hover:bg-obsidian-900/80 transition-colors">
                            <div className="flex items-center gap-2 mb-4">
                                <Calendar className="w-4 h-4 text-indigo-400" />
                                <h3 className="text-xs font-bold text-indigo-400 uppercase tracking-widest">Timeline Metrics</h3>
                            </div>
                            <div className="space-y-3">
                                {[
                                    { label: 'Entry to Credited', value: `${data.entered_to_credited_days ?? 'n/a'} days` },
                                    { label: 'Investigation to Credited', value: `${data.investigation_to_credited_days ?? 'n/a'} days` },
                                    { label: 'Days Open', value: data.days_open ?? 'n/a' },
                                    { label: 'Pending Billing to Credit', value: data.days_pending_billing_to_credit ?? 'n/a' },
                                ].map((item, i) => (
                                    <div key={i} className="flex justify-between items-center text-sm group">
                                        <span className="text-slate-400 group-hover:text-slate-300 transition-colors">{item.label}</span>
                                        <span className="font-mono text-slate-200 group-hover:text-white transition-colors">{item.value}</span>
                                    </div>
                                ))}
                            </div>
                        </div>

                        {/* Assignment & Status Card */}
                        <div className="bg-obsidian-900/50 border border-white/[0.05] rounded-2xl p-5 hover:bg-obsidian-900/80 transition-colors">
                            <div className="flex items-center gap-2 mb-4">
                                <AlertCircle className="w-4 h-4 text-cyan-400" />
                                <h3 className="text-xs font-bold text-cyan-400 uppercase tracking-widest">Attributes & Status</h3>
                            </div>
                            <div className="space-y-3">
                                <div className="flex justify-between items-center text-sm group">
                                    <span className="text-slate-400">Sales Reps</span>
                                    <span className="text-slate-200 truncate max-w-[150px]" title={data.sales_reps?.join(', ')}>
                                        {previewList(data.sales_reps, 6)}
                                    </span>
                                </div>
                                <div className="flex justify-between items-center text-sm">
                                    <span className="text-slate-400">Account Prefixes</span>
                                    <span className="text-slate-200 font-mono">
                                        {previewList(data.account_prefixes, 6)}
                                    </span>
                                </div>
                                <div className="flex justify-between items-center text-sm">
                                    <span className="text-slate-400">Threshold Exceeded</span>
                                    {data.threshold_exceeded ? (
                                        <span className="px-2 py-0.5 rounded bg-rose-500/10 text-rose-400 text-xs font-bold uppercase">Yes</span>
                                    ) : (
                                        <span className="px-2 py-0.5 rounded bg-slate-500/10 text-slate-400 text-xs font-bold uppercase">No</span>
                                    )}
                                </div>
                                <div className="flex justify-between items-center text-sm pt-1 border-t border-white/[0.05]">
                                    <span className="text-slate-400">Last Status</span>
                                    <div className="text-right">
                                        <div className="text-cyan-300 font-medium capitalize">{data.last_status_event_type || 'Unknown'}</div>
                                        {data.last_status_timestamp && (
                                            <div className="text-[10px] text-slate-500 font-mono">{data.last_status_timestamp}</div>
                                        )}
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>

                    {/* Lists Grid */}
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                        <div className="bg-white/[0.02] border border-white/[0.04] rounded-2xl p-5">
                            <div className="flex items-center gap-2 mb-3">
                                <FileText className="w-4 h-4 text-slate-400" />
                                <h3 className="text-[10px] font-bold text-slate-500 uppercase tracking-widest">Invoices</h3>
                            </div>
                            <p className="text-sm font-mono text-slate-300 leading-relaxed">
                                {previewList(data.invoice_numbers, 8)}
                            </p>
                        </div>
                        <div className="bg-white/[0.02] border border-white/[0.04] rounded-2xl p-5">
                            <div className="flex items-center gap-2 mb-3">
                                <Tags className="w-4 h-4 text-slate-400" />
                                <h3 className="text-[10px] font-bold text-slate-500 uppercase tracking-widest">Items</h3>
                            </div>
                            <p className="text-sm font-mono text-slate-300 leading-relaxed">
                                {previewList(data.item_numbers, 8)}
                            </p>
                        </div>
                    </div>

                    {/* Investigation Highlights */}
                    {(data.investigation_highlights ?? []).length > 0 && (
                        <div className="bg-gradient-to-r from-cyan-900/10 to-transparent border-l-2 border-cyan-500 p-5 rounded-r-2xl">
                            <div className="flex items-center justify-between gap-3 mb-4">
                                <div className="flex items-center gap-2">
                                <Sparkles className="w-4 h-4 text-cyan-400" />
                                <h3 className="text-xs font-bold text-cyan-400 uppercase tracking-widest">Key Investigation Highlights</h3>
                                </div>
                                {highlightSourceLabel && (
                                    <div className="text-[10px] text-slate-400 font-mono px-2 py-1 rounded-lg bg-white/[0.03] border border-white/[0.08]">
                                        {highlightSourceLabel}{highlightModel ? ` · ${highlightModel}` : ''}
                                    </div>
                                )}
                            </div>
                            <ul className="space-y-2.5">
                                {(data.investigation_highlights ?? []).map((highlight, idx) => (
                                    <li key={idx} className="flex items-start gap-3 text-sm text-slate-200 leading-relaxed group">
                                        <div className="mt-1.5 w-1.5 h-1.5 rounded-full bg-cyan-500/50 group-hover:bg-cyan-400 group-hover:shadow-[0_0_8px_rgba(34,211,238,0.8)] transition-all flex-shrink-0"></div>
                                        <span>{highlight}</span>
                                    </li>
                                ))}
                            </ul>
                        </div>
                    )}

                    {/* Suggested Follow-ups */}
                    {suggestions.length > 0 && (
                        <div className="pt-4 border-t border-white/[0.05]">
                            <h3 className="text-[10px] uppercase tracking-[0.2em] text-slate-500 font-bold mb-4 flex items-center gap-2">
                                <Zap className="w-3.5 h-3.5 text-amber-400" /> Suggested Action
                            </h3>
                            <div className="flex flex-wrap gap-2.5">
                                {suggestions.slice(0, 3).map((item, idx) => (
                                    <button
                                        key={idx}
                                        type="button"
                                        onClick={() => onSuggestionClick(String(item.prefix || '').trim())}
                                        className="group flex items-center gap-2 px-4 py-2 rounded-xl bg-white/[0.03] hover:bg-cyan-500/10 border border-white/[0.08] hover:border-cyan-500/30 transition-all duration-300 active:scale-95"
                                    >
                                        <span className="text-xs font-bold text-slate-500 group-hover:text-cyan-500/50 transition-colors">0{idx + 1}</span>
                                        <span className="text-sm font-medium text-slate-300 group-hover:text-cyan-200 transition-colors">
                                            {item.label || item.prefix}
                                        </span>
                                        <TrendingUp className="w-3.5 h-3.5 text-slate-600 group-hover:text-cyan-400 transition-colors ml-1 opacity-0 group-hover:opacity-100 -translate-x-2 group-hover:translate-x-0" />
                                    </button>
                                ))}
                            </div>
                        </div>
                    )}
                </div>
            </div>
        </div>
    );
}

export function ItemAnalysis({
    data,
    suggestions = [],
    onSuggestionClick,
}: {
    data: ItemAnalysisMeta;
    suggestions?: { label?: string; prefix?: string }[];
    onSuggestionClick: (query: string) => void;
}) {
    const primaryRoots = sortCountEntries(data.root_cause_counts);
    const allRoots = sortCountEntries(data.root_cause_counts_all);
    const topSalesReps = sortCountEntries(data.sales_rep_counts).slice(0, 5);
    const topPrefixes = sortCountEntries(data.account_prefix_counts).slice(0, 5);

    return (
        <div className="w-full max-w-5xl relative group/analysis">
            <div className="absolute -inset-0.5 bg-gradient-to-br from-cyan-500/20 via-indigo-500/10 to-transparent rounded-3xl blur-xl opacity-50 group-hover/analysis:opacity-100 transition duration-700 pointer-events-none"></div>

            <div className="relative bg-obsidian-950/80 border border-white/[0.08] rounded-3xl overflow-hidden shadow-2xl backdrop-blur-xl transition-all duration-500 hover:border-cyan-500/30">
                <div className="p-6 md:p-8 flex flex-col md:flex-row md:items-center justify-between gap-6 border-b border-white/[0.04] bg-gradient-to-b from-white/[0.02] to-transparent">
                    <div className="flex items-start gap-4">
                        <div className="w-12 h-12 rounded-2xl bg-gradient-to-br from-cyan-400/20 to-indigo-600/20 border border-cyan-500/30 flex items-center justify-center flex-shrink-0 shadow-[0_0_15px_rgba(34,211,238,0.15)] group-hover/analysis:shadow-[0_0_25px_rgba(34,211,238,0.3)] transition-all">
                            <Activity className="w-6 h-6 text-cyan-400" />
                        </div>
                        <div>
                            <h2 className="text-2xl font-bold font-display tracking-tight text-white drop-shadow-sm">
                                Item <span className="text-transparent bg-clip-text bg-gradient-to-r from-cyan-400 to-indigo-400">{data.item_number || 'Unknown'}</span> Analysis
                            </h2>
                            <p className="text-sm text-slate-400 mt-1.5 flex items-center gap-2 font-medium">
                                <Hash className="w-3.5 h-3.5" />
                                {Number(data.ticket_count ?? 0)} tickets • {Number(data.invoice_count ?? 0)} invoices • {Number(data.line_count ?? 0)} lines
                            </p>
                        </div>
                    </div>

                    <div className="px-6 py-4 rounded-2xl bg-obsidian-900 border border-emerald-500/20 shadow-[0_0_20px_rgba(16,185,129,0.05)] flex flex-col items-end min-w-[180px] group-hover/analysis:border-emerald-500/40 transition-colors">
                        <span className="text-[10px] uppercase tracking-[0.2em] text-emerald-500/80 font-bold mb-1">Total Credit</span>
                        <div className="text-emerald-400 font-mono text-3xl font-bold tracking-tight">
                            {formatCurrency(data.total_credit)}
                        </div>
                    </div>
                </div>

                <div className="p-6 md:p-8 space-y-8">
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                        <div className="bg-obsidian-900/50 border border-white/[0.05] rounded-2xl p-5">
                            <div className="flex items-center gap-2 mb-4">
                                <Target className="w-4 h-4 text-amber-400" />
                                <h3 className="text-xs font-bold text-amber-400 uppercase tracking-widest">Primary Root Causes</h3>
                            </div>
                            <div className="flex flex-wrap gap-2">
                                {primaryRoots.map(([root, count]) => (
                                    <span key={`primary-${root}`} className="px-2 py-1 rounded-lg bg-amber-500/10 border border-amber-500/20 text-amber-200 text-xs">
                                        {root} ({count})
                                    </span>
                                ))}
                                {primaryRoots.length === 0 && <span className="text-sm text-slate-500">No primary root causes found.</span>}
                            </div>
                        </div>

                        <div className="bg-obsidian-900/50 border border-white/[0.05] rounded-2xl p-5">
                            <div className="flex items-center gap-2 mb-4">
                                <Sparkles className="w-4 h-4 text-cyan-400" />
                                <h3 className="text-xs font-bold text-cyan-400 uppercase tracking-widest">All Root Causes (Line-Level)</h3>
                            </div>
                            <div className="flex flex-wrap gap-2">
                                {allRoots.map(([root, count]) => (
                                    <span key={`all-${root}`} className="px-2 py-1 rounded-lg bg-cyan-500/10 border border-cyan-500/20 text-cyan-200 text-xs">
                                        {root} ({count})
                                    </span>
                                ))}
                                {allRoots.length === 0 && <span className="text-sm text-slate-500">No root causes found.</span>}
                            </div>
                        </div>
                    </div>

                    <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                        <div className="bg-white/[0.02] border border-white/[0.04] rounded-2xl p-5">
                            <div className="text-[10px] font-bold text-slate-500 uppercase tracking-widest mb-2">Top Sales Reps</div>
                            <p className="text-sm text-slate-300">
                                {topSalesReps.map(([rep, count]) => `${rep} (${count})`).join(', ') || 'none'}
                            </p>
                        </div>
                        <div className="bg-white/[0.02] border border-white/[0.04] rounded-2xl p-5">
                            <div className="text-[10px] font-bold text-slate-500 uppercase tracking-widest mb-2">Top Account Prefixes</div>
                            <p className="text-sm text-slate-300">
                                {topPrefixes.map(([prefix, count]) => `${prefix} (${count})`).join(', ') || 'none'}
                            </p>
                        </div>
                    </div>

                    <div className="bg-white/[0.02] border border-white/[0.04] rounded-2xl p-5">
                        <div className="text-[10px] font-bold text-slate-500 uppercase tracking-widest mb-2">Activity Window</div>
                        <p className="text-sm text-slate-300">
                            First observed: <span className="font-mono">{data.first_seen ?? 'unknown'}</span>
                            {' • '}
                            Last observed: <span className="font-mono">{data.last_seen ?? 'unknown'}</span>
                        </p>
                    </div>

                    {data.answer && (
                        <div className="bg-gradient-to-r from-cyan-900/10 to-transparent border-l-2 border-cyan-500 p-5 rounded-r-2xl">
                            <div className="flex items-center gap-2 mb-4">
                                <FileText className="w-4 h-4 text-cyan-400" />
                                <h3 className="text-xs font-bold text-cyan-400 uppercase tracking-widest">Summary</h3>
                            </div>
                            <p className="text-sm text-slate-200 whitespace-pre-wrap">{data.answer}</p>
                        </div>
                    )}

                    {suggestions.length > 0 && (
                        <div className="pt-4 border-t border-white/[0.05]">
                            <h3 className="text-[10px] uppercase tracking-[0.2em] text-slate-500 font-bold mb-4 flex items-center gap-2">
                                <Zap className="w-3.5 h-3.5 text-amber-400" /> Suggested Action
                            </h3>
                            <div className="flex flex-wrap gap-2.5">
                                {suggestions.slice(0, 3).map((item, idx) => (
                                    <button
                                        key={idx}
                                        type="button"
                                        onClick={() => onSuggestionClick(String(item.prefix || '').trim())}
                                        className="group flex items-center gap-2 px-4 py-2 rounded-xl bg-white/[0.03] hover:bg-cyan-500/10 border border-white/[0.08] hover:border-cyan-500/30 transition-all duration-300 active:scale-95"
                                    >
                                        <span className="text-xs font-bold text-slate-500 group-hover:text-cyan-500/50 transition-colors">0{idx + 1}</span>
                                        <span className="text-sm font-medium text-slate-300 group-hover:text-cyan-200 transition-colors">
                                            {item.label || item.prefix}
                                        </span>
                                        <TrendingUp className="w-3.5 h-3.5 text-slate-600 group-hover:text-cyan-400 transition-colors ml-1 opacity-0 group-hover:opacity-100 -translate-x-2 group-hover:translate-x-0" />
                                    </button>
                                ))}
                            </div>
                        </div>
                    )}
                </div>
            </div>
        </div>
    );
}
