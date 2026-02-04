import { useEffect, useState } from "react";
import { Search, Sparkles, FileText, Database, ArrowRight, X, Activity } from "lucide-react";

type RagSnippet = {
  text: string;
  chunk_type: string; // "summary" | "status" | "note" | etc
};

type RagResult = {
  ticket_id: string;
  score: number;
  reason_for_credit: string | null;
  root_cause?: string | null;
  root_causes_all?: string[];
  invoice_count: number;
  has_multiple_invoices: boolean;
  snippet_count: number;
  next_action?: string;
  action_confidence?: "high" | "medium" | "low";
  action_reason_codes?: string[];
  action_tag?: string;
  ui?: {
    score_label?: string;
    score_band?: "low" | "medium" | "high" | string;
    score_value?: number;
  };
  meta?: {
    terminal_decision?: boolean;
    closure_note?: string;
    resolution?: {
      status?: string;
      method?: string;
      credit_numbers?: string[];
      credit_number?: string;
      credit_date?: string;
      amount?: number;
      closed_date?: string;
      pricing_corrected_date?: string;
      verified_by?: string;
    };
  };
  snippets: RagSnippet[];
};

type RagResponse = {
  results: RagResult[];
};

type NextActionTraceResponse = {
  ticket_id?: string;
  decision: {
    next_action: string;
    action_confidence: string;
    action_reason_codes: string[];
    action_tag?: string;
    action_rule_id?: string;
  };
  context: Record<string, any>;
  trace: Array<Record<string, any>>;
};

function formatScore(score: number) {
  // cosine similarity: 0..1-ish
  return score.toFixed(3);
}

function extractField(text: string, field: string): string | null {
  // naive extraction like: "customer: xxx"
  // works with your current summary format
  const re = new RegExp(`${field}:\\s*([^\\s|]+)`, "i");
  const m = text.match(re);
  return m ? m[1] : null;
}

function extractInvoice(text: string): string | null {
  return extractField(text, "invoice");
}

function extractCustomer(text: string): string | null {
  return extractField(text, "customer");
}

function extractTotalCredit(text: string): string | null {
  // total_credit: 85.69
  const re = /total_credit:\s*([0-9]+(?:\.[0-9]+)?)/i;
  const m = text.match(re);
  return m ? m[1] : null;
}

export function RagResults({
  apiBase = import.meta.env.VITE_API_BASE_URL ?? import.meta.env.VITE_API_BASE ?? "http://127.0.0.1:8000",
}: {
  apiBase?: string;
}) {
  const [query, setQuery] = useState("");
  const [topK, setTopK] = useState(10);
  const [openRefsFor, setOpenRefsFor] = useState<string | null>(null);
  const [openTraceFor, setOpenTraceFor] = useState<string | null>(null);
  const [expandedCrFor, setExpandedCrFor] = useState<string | null>(null);
  const [refsMap, setRefsMap] = useState<
    Record<string, { invoice_ids: string[]; item_numbers: string[] }>
  >({});
  const [refsLoading, setRefsLoading] = useState<Record<string, boolean>>({});
  const [traceMap, setTraceMap] = useState<Record<string, NextActionTraceResponse>>({});
  const [traceLoading, setTraceLoading] = useState<Record<string, boolean>>({});
  const [loading, setLoading] = useState(false);
  const [data, setData] = useState<RagResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [isMounted, setIsMounted] = useState(false);

  useEffect(() => {
    const id = requestAnimationFrame(() => setIsMounted(true));
    return () => cancelAnimationFrame(id);
  }, []);

  async function runSearch() {
    setLoading(true);
    setError(null);
    try {
      const res = await fetch(`${apiBase}/rag/search`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ query, top_k: topK }),
      });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const json = (await res.json()) as RagResponse;
      setData(json);
    } catch (e: any) {
      setError(e?.message ?? "Search failed");
    } finally {
      setLoading(false);
    }
  }

  async function loadRefs(ticketId: string) {
    if (openRefsFor === ticketId) {
      setOpenRefsFor(null);
      return;
    }

    // If we already have data, just toggle open
    if (refsMap[ticketId]) {
      setOpenRefsFor(ticketId);
      return;
    }

    setRefsLoading((p) => ({ ...p, [ticketId]: true }));
    try {
      const res = await fetch(`${apiBase}/rag/ticket/${ticketId}/refs`);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const json = await res.json();
      setRefsMap((p) => ({
        ...p,
        [ticketId]: {
          invoice_ids: json.invoice_ids || [],
          item_numbers: json.item_numbers || [],
        },
      }));
      setOpenRefsFor(ticketId);
    } finally {
      setRefsLoading((p) => ({ ...p, [ticketId]: false }));
    }
  }

  async function loadTrace(ticketId: string, result: RagResult) {
    if (openTraceFor === ticketId) {
      setOpenTraceFor(null);
      return;
    }

    if (traceMap[ticketId]) {
      setOpenTraceFor(ticketId);
      return;
    }

    setTraceLoading((p) => ({ ...p, [ticketId]: true }));
    try {
      const res = await fetch(`${apiBase}/rag/next-action/trace`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          ticket_id: result.ticket_id,
          reason_for_credit: result.reason_for_credit,
          snippets: result.snippets,
        }),
      });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const json = (await res.json()) as NextActionTraceResponse;
      setTraceMap((p) => ({ ...p, [ticketId]: json }));
      setOpenTraceFor(ticketId);
    } catch (e: any) {
      setError(e?.message ?? "Trace failed");
    } finally {
      setTraceLoading((p) => ({ ...p, [ticketId]: false }));
    }
  }

  const results = data?.results ?? [];

  return (
    <div className="w-full max-w-6xl mx-auto p-4 md:p-8 font-sans text-slate-200">
      {/* Background Ambience */}
      <div className="fixed inset-0 overflow-hidden pointer-events-none -z-10">
        <div className="absolute top-[20%] left-[10%] w-[600px] h-[600px] bg-indigo-900/10 rounded-full blur-[120px] animate-pulse-slow"></div>
        <div className="absolute bottom-[20%] right-[10%] w-[500px] h-[500px] bg-cyan-900/10 rounded-full blur-[100px] animate-float"></div>
      </div>

      <div
        className={`transition-opacity duration-500 ease-out ${isMounted ? "opacity-100" : "opacity-0"}`}
      >
        <div className="flex flex-col gap-6 mb-8">

          {/* Header Section */}
          <div className="flex flex-col md:flex-row items-end justify-between gap-6 relative z-10">
            <div>
              <div className="flex items-center gap-3 mb-2">
                <div className="p-2.5 rounded-xl bg-gradient-to-br from-cyan-500/20 to-blue-600/20 border border-cyan-500/20 shadow-[0_0_30px_-5px_rgba(6,182,212,0.3)]">
                  <Database className="w-6 h-6 text-cyan-400" />
                </div>
                <h2 className="text-3xl font-bold font-display tracking-tight text-white">
                  Neural Search
                </h2>
              </div>
              <p className="text-slate-400 text-sm font-medium pl-1">
                Semantic analysis & credit intelligence
              </p>
            </div>
          </div>

          {/* Search Bar */}
          <div className="relative group z-20">
            <div className="absolute -inset-1 bg-gradient-to-r from-cyan-500/20 via-indigo-500/20 to-cyan-500/20 rounded-2xl blur-md opacity-40 group-hover:opacity-75 transition duration-700"></div>
            <div className="relative flex items-center bg-obsidian-900/90 backdrop-blur-xl border border-white/10 group-hover:border-white/20 rounded-2xl p-2 shadow-2xl transition-all duration-300">
              <div className="pl-4 text-cyan-400/80 group-focus-within:text-cyan-400 transition-colors">
                <Search className="w-6 h-6" />
              </div>
              <input
                className="flex-1 bg-transparent border-none px-4 py-4 text-lg text-slate-100 placeholder:text-slate-500 focus:ring-0 focus:outline-none font-sans"
                value={query}
                onChange={(e) => setQuery(e.target.value)}
                placeholder="Describe the issue (e.g., 'unexpected credit spike', 'pricing error')"
                onKeyDown={(e) => e.key === 'Enter' && runSearch()}
              />

              <div className="h-8 w-[1px] bg-white/10 mx-2"></div>

              <div className="flex items-center gap-3 pr-2">
                <div className="hidden md:flex flex-col items-end mr-2">
                  <span className="text-[10px] font-bold text-slate-500 uppercase tracking-widest">
                    Max Results
                  </span>
                  <input
                    className="w-12 bg-transparent text-right text-sm font-bold text-slate-300 focus:text-cyan-400 focus:outline-none transition-colors border-b border-transparent focus:border-cyan-500/50"
                    type="number"
                    min={1}
                    max={50}
                    value={topK}
                    onChange={(e) => setTopK(Number(e.target.value))}
                  />
                </div>

                <button
                  className="px-8 py-3 rounded-xl bg-cyan-500 hover:bg-cyan-400 text-obsidian-950 font-bold text-sm tracking-wide transition-all duration-300 shadow-[0_0_20px_-5px_rgba(6,182,212,0.4)] hover:shadow-[0_0_30px_-5px_rgba(6,182,212,0.6)] hover:scale-[1.02] active:scale-[0.98] disabled:opacity-50 disabled:cursor-not-allowed disabled:transform-none"
                  onClick={runSearch}
                  disabled={loading}
                >
                  {loading ? (
                    <div className="flex items-center gap-2">
                      <Activity className="w-4 h-4 animate-spin" />
                      <span>Analyzing...</span>
                    </div>
                  ) : (
                    <div className="flex items-center gap-2">
                      <Sparkles className="w-4 h-4" />
                      <span>Generate Insights</span>
                    </div>
                  )}
                </button>
              </div>
            </div>
          </div>

          {error && (
            <div className="rounded-xl border border-red-500/30 bg-red-500/10 p-4 text-sm text-red-200 flex items-center gap-3 animate-fade-in shadow-lg shadow-red-900/10">
              <div className="p-2 rounded-full bg-red-500/20">
                <X className="w-4 h-4" />
              </div>
              {error}
            </div>
          )}
        </div>

        <div className="flex flex-col gap-4">
          {results.map((r) => {
            const summarySnippet =
              (r.snippets ?? []).find((s) => s.chunk_type === "summary") ?? r.snippets?.[0];
            const head = summarySnippet?.text ?? "";
            const customer = extractCustomer(head);
            const invoice = extractInvoice(head);
            const totalCredit = extractTotalCredit(head);
            const scoreValue = typeof r.ui?.score_value === "number" ? r.ui?.score_value : r.score;
            const confidenceLabel = r.action_confidence
              ? r.action_confidence.charAt(0).toUpperCase() + r.action_confidence.slice(1)
              : null;
            const confidenceClass =
              r.action_confidence === "high"
                ? "bg-emerald-500/10 border-emerald-500/30 text-emerald-300"
                : r.action_confidence === "medium"
                  ? "bg-amber-500/10 border-amber-500/30 text-amber-300"
                  : r.action_confidence === "low"
                    ? "bg-slate-500/10 border-slate-500/30 text-slate-300"
                    : "";
            const isCompleted = r.action_tag === "completed";
            const closureNote = r.meta?.terminal_decision ? r.meta?.closure_note : null;
            const rootCause = r.root_cause;
            const rootCausesAll = (r.root_causes_all ?? []).filter(Boolean);
            const creditNumbers: string[] =
              r.meta?.resolution?.credit_numbers && r.meta?.resolution?.credit_numbers.length > 0
                ? r.meta?.resolution?.credit_numbers
                : r.meta?.resolution?.credit_number
                  ? [r.meta?.resolution?.credit_number]
                  : [];
            const showAllCr = expandedCrFor === r.ticket_id;
            const crVisible = creditNumbers.length > 10 && !showAllCr
              ? creditNumbers.slice(0, 10)
              : creditNumbers;
            const trace = traceMap[r.ticket_id];
            // item numbers are shown in the refs panel when opened

            return (
              <div
                key={r.ticket_id}
                className="group relative bg-obsidian-950/40 backdrop-blur-md border border-white/[0.08] rounded-2xl overflow-hidden transition-all duration-300 hover:border-cyan-500/30 hover:shadow-2xl hover:shadow-cyan-900/10"
              >
                <div className="absolute inset-0 bg-gradient-to-br from-cyan-500/[0.03] via-transparent to-transparent opacity-0 group-hover:opacity-100 transition-opacity pointer-events-none" />

                <div className="relative p-6 md:p-8">
                  <div className="flex flex-col md:flex-row items-start justify-between gap-6">
                    <div className="flex-1 space-y-5">

                      {/* Header Row */}
                      <div className="flex items-start gap-4">
                        <div className="mt-1 w-12 h-12 rounded-2xl bg-gradient-to-br from-slate-800 to-slate-900 border border-white/10 flex items-center justify-center shadow-inner">
                          <FileText className="w-5 h-5 text-cyan-400" />
                        </div>
                        <div>
                          <div className="flex items-center gap-3">
                            <h3 className="text-2xl font-bold font-display text-white tracking-tight">
                              {r.ticket_id}
                            </h3>
                            {isCompleted && (
                              <span className="px-2.5 py-0.5 rounded-full text-[10px] font-bold bg-emerald-500/10 border border-emerald-500/20 text-emerald-400 uppercase tracking-wider">
                                Resolved
                              </span>
                            )}
                            {r.score > 0.8 && !isCompleted && (
                              <span className="px-2.5 py-0.5 rounded-full text-[10px] font-bold bg-cyan-500/10 border border-cyan-500/20 text-cyan-400 uppercase tracking-wider">
                                High Match
                              </span>
                            )}
                          </div>

                          <div className="flex items-center gap-3 mt-1.5 text-sm font-medium text-slate-400">
                            <div className="flex items-center gap-2 bg-white/[0.03] rounded-lg px-2 py-1 border border-white/[0.05]">
                              <span className="text-xs uppercase tracking-wider text-slate-500">Sim</span>
                              <div className="h-1.5 w-16 bg-slate-800 rounded-full overflow-hidden">
                                <div
                                  className="h-full bg-gradient-to-r from-cyan-500 to-blue-500"
                                  style={{ width: `${Math.min(scoreValue * 100, 100)}%` }}
                                />
                              </div>
                              <span className="text-cyan-300 font-mono">{formatScore(scoreValue)}</span>
                            </div>
                          </div>
                        </div>
                      </div>

                      {/* Tags & Metadata */}
                      <div className="flex flex-wrap gap-2 text-sm">
                        {(rootCause || rootCausesAll.length > 0) && (
                          <div className="px-3 py-1.5 rounded-lg bg-amber-500/10 border border-amber-500/20 text-amber-200 flex items-center gap-2">
                            <span className="w-1.5 h-1.5 rounded-full bg-amber-400 animate-pulse" />
                            <span className="font-semibold">{rootCause || rootCausesAll[0]}</span>
                            {rootCausesAll.length > 1 && (
                              <span className="px-1.5 py-0.5 rounded bg-amber-500/20 text-[10px] uppercase font-bold text-amber-300">
                                + {(rootCausesAll.length - 1)} more
                              </span>
                            )}
                          </div>
                        )}
                        {rootCausesAll.length > 1 && (
                          <div className="flex flex-wrap gap-1.5">
                            {rootCausesAll
                              .filter((c) => c !== rootCause)
                              .slice(0, 3)
                              .map((cause) => (
                                <span
                                  key={cause}
                                  className="px-2 py-1.5 rounded-lg bg-white/[0.03] border border-white/[0.08] text-xs text-slate-400"
                                >
                                  {cause}
                                </span>
                              ))}
                          </div>
                        )}
                      </div>

                      <div className="grid grid-cols-2 sm:flex sm:flex-wrap gap-3 text-xs font-mono text-slate-400 mt-2">
                        {customer && (
                          <div className="px-3 py-2 rounded-lg bg-slate-900/50 border border-white/5 flex flex-col sm:flex-row sm:items-center sm:gap-2">
                            <span className="text-slate-600 uppercase tracking-wider text-[10px] font-bold">Customer</span>
                            <span className="text-slate-300">{customer}</span>
                          </div>
                        )}
                        {invoice && (
                          <div className="px-3 py-2 rounded-lg bg-slate-900/50 border border-white/5 flex flex-col sm:flex-row sm:items-center sm:gap-2">
                            <span className="text-slate-600 uppercase tracking-wider text-[10px] font-bold">Invoice</span>
                            <span className="text-cyan-300">{invoice}</span>
                          </div>
                        )}
                        {totalCredit && (
                          <div className="px-3 py-2 rounded-lg bg-slate-900/50 border border-white/5 flex flex-col sm:flex-row sm:items-center sm:gap-2">
                            <span className="text-slate-600 uppercase tracking-wider text-[10px] font-bold">Credit</span>
                            <span className="text-emerald-400">${totalCredit}</span>
                          </div>
                        )}
                      </div>
                    </div>

                    {/* Action Buttons */}
                    <div className="flex flex-row md:flex-col gap-2 w-full md:w-auto shrink-0 mt-4 md:mt-0">
                      <button
                        className="flex-1 md:w-36 px-4 py-2.5 rounded-xl bg-cyan-500/10 hover:bg-cyan-500/20 border border-cyan-500/30 text-cyan-300 text-sm font-semibold transition-all group/btn flex items-center justify-center gap-2 hover:shadow-[0_0_15px_rgba(6,182,212,0.2)]"
                        onClick={() => console.log("open ticket", r.ticket_id)}
                      >
                        <span>Open Ticket</span>
                        <ArrowRight className="w-4 h-4 group-hover/btn:translate-x-0.5 transition-transform" />
                      </button>

                      <div className="flex gap-2">
                        <button
                          className={`flex-1 px-3 py-2.5 rounded-xl border text-sm font-medium transition-all flex items-center justify-center gap-2 ${openRefsFor === r.ticket_id
                            ? 'bg-slate-800 border-white/20 text-white'
                            : 'bg-white/[0.03] hover:bg-white/[0.06] border-white/10 text-slate-400'
                            }`}
                          onClick={() => loadRefs(r.ticket_id)}
                          disabled={!!refsLoading[r.ticket_id]}
                          title="View References"
                        >
                          {refsLoading[r.ticket_id] ? <Activity className="w-4 h-4 animate-spin" /> : <Database className="w-4 h-4" />}
                        </button>
                        <button
                          className={`flex-1 px-3 py-2.5 rounded-xl border text-sm font-medium transition-all flex items-center justify-center gap-2 ${openTraceFor === r.ticket_id
                            ? 'bg-slate-800 border-white/20 text-white'
                            : 'bg-white/[0.03] hover:bg-white/[0.06] border-white/10 text-slate-400'
                            }`}
                          onClick={() => loadTrace(r.ticket_id, r)}
                          disabled={!!traceLoading[r.ticket_id]}
                          title="View Logic Trace"
                        >
                          {traceLoading[r.ticket_id] ? <Activity className="w-4 h-4 animate-spin" /> : <Activity className="w-4 h-4" />}
                        </button>
                      </div>
                    </div>
                  </div>

                  {/* Collapsible Sections */}
                  <div className={`grid transition-all duration-300 ease-[cubic-bezier(0.4,0,0.2,1)] ${openRefsFor === r.ticket_id ? 'grid-rows-[1fr] mt-6 opacity-100' : 'grid-rows-[0fr] opacity-0'}`}>
                    <div className="overflow-hidden border-t border-white/[0.08] pt-6">
                      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                        <div className="bg-black/20 rounded-xl p-4 border border-white/5">
                          <div className="flex items-center justify-between mb-3 text-[10px] font-bold text-slate-500 uppercase tracking-widest">
                            <span>Linked Invoices</span>
                            <span className="bg-white/10 px-1.5 py-0.5 rounded text-white">{refsMap[r.ticket_id]?.invoice_ids?.length ?? 0}</span>
                          </div>
                          <div className="flex flex-wrap gap-2">
                            {(refsMap[r.ticket_id]?.invoice_ids ?? []).map((inv) => (
                              <span key={inv} className="px-2 py-1 rounded-md bg-white/5 border border-white/5 text-xs font-mono text-cyan-300/90 cursor-default">
                                {inv}
                              </span>
                            ))}
                            {(refsMap[r.ticket_id]?.invoice_ids ?? []).length === 0 && (
                              <span className="text-xs text-slate-600 italic">No linked invoices found</span>
                            )}
                          </div>
                        </div>

                        <div className="bg-black/20 rounded-xl p-4 border border-white/5">
                          <div className="flex items-center justify-between mb-3 text-[10px] font-bold text-slate-500 uppercase tracking-widest">
                            <span>Item Numbers</span>
                            <span className="bg-white/10 px-1.5 py-0.5 rounded text-white">{refsMap[r.ticket_id]?.item_numbers?.length ?? 0}</span>
                          </div>
                          <div className="flex flex-wrap gap-2">
                            {(refsMap[r.ticket_id]?.item_numbers ?? []).map((it) => (
                              <span key={it} className="px-2 py-1 rounded-md bg-white/5 border border-white/5 text-xs font-mono text-emerald-300/90 cursor-default">
                                {it}
                              </span>
                            ))}
                            {(refsMap[r.ticket_id]?.item_numbers ?? []).length === 0 && (
                              <span className="text-xs text-slate-600 italic">No items found</span>
                            )}
                          </div>
                        </div>
                      </div>
                    </div>
                  </div>

                  <div className={`grid transition-all duration-300 ease-[cubic-bezier(0.4,0,0.2,1)] ${openTraceFor === r.ticket_id ? 'grid-rows-[1fr] mt-6 opacity-100' : 'grid-rows-[0fr] opacity-0'}`}>
                    <div className="overflow-hidden border-t border-white/[0.08] pt-6">
                      <div className="bg-black/20 rounded-xl p-4 border border-white/5 relative">
                        <div className="flex items-center justify-between mb-3 text-[10px] font-bold text-slate-500 uppercase tracking-widest">
                          <span>Rule Trace Logic</span>
                          <span className="px-2 py-1 rounded bg-white/5 border border-white/5 text-slate-400 font-mono">
                            {trace?.decision?.action_rule_id ?? "N/A"}
                          </span>
                        </div>
                        <pre className="text-[11px] text-slate-300 leading-relaxed font-mono whitespace-pre-wrap max-h-80 overflow-auto custom-scrollbar">
                          {trace ? JSON.stringify(trace, null, 2) : "Loading trace..."}
                        </pre>
                      </div>
                    </div>
                  </div>

                  {/* Evidence Section - Styled as a coherent block */}
                  <div className="mt-6 pt-6 border-t border-white/[0.08]">
                    <div className="flex items-center gap-2 mb-4">
                      <div className="p-1 rounded bg-white/5">
                        <Sparkles className="w-3.5 h-3.5 text-cyan-400" />
                      </div>
                      <span className="text-xs font-bold text-slate-400 uppercase tracking-widest">Insights & Evidence</span>
                    </div>

                    <div className="space-y-4">
                      {/* Next Action Box */}
                      {(closureNote || r.next_action) && (
                        <div className="rounded-xl border border-indigo-500/20 bg-indigo-500/[0.05] p-4 relative overflow-hidden">
                          <div className="absolute top-0 right-0 p-2 opacity-10">
                            <Activity className="w-16 h-16 text-indigo-500" />
                          </div>
                          <div className="relative z-10">
                            <div className="flex items-center gap-2 mb-2">
                              <span className="text-indigo-300 text-[10px] font-bold uppercase tracking-widest">Recommended Action</span>
                              {confidenceLabel && (
                                <span className={`px-2 py-0.5 rounded border text-[10px] font-bold uppercase ${confidenceClass}`}>
                                  {confidenceLabel} Confidence
                                </span>
                              )}
                            </div>
                            <p className="text-sm text-indigo-100/90 leading-relaxed">
                              {closureNote || r.next_action}
                            </p>
                          </div>
                        </div>
                      )}

                      {/* Credit Numbers */}
                      {creditNumbers.length > 0 && (
                        <div className="flex items-start gap-3 p-3 rounded-xl bg-white/[0.02] border border-white/[0.05]">
                          <span className="text-xs font-bold text-slate-500 uppercase mt-1">Credits</span>
                          <div className="flex flex-wrap gap-2 flex-1">
                            {crVisible.map((cr: string) => (
                              <span key={cr} className="px-2 py-1 rounded bg-slate-800/50 text-xs font-mono text-cyan-300/90 border border-white/5">
                                {cr}
                              </span>
                            ))}
                            {creditNumbers.length > 10 && (
                              <button
                                className="text-xs text-cyan-400 hover:text-cyan-300 underline underline-offset-2"
                                onClick={() => setExpandedCrFor(showAllCr ? null : r.ticket_id)}
                              >
                                {showAllCr ? "Show less" : `+${creditNumbers.length - 10} more`}
                              </button>
                            )}
                          </div>
                        </div>
                      )}

                      {/* Content Area */}
                      <div className="bg-black/10 rounded-xl border border-white/5 min-h-[100px] relative">
                        <div className="p-4">
                          {summarySnippet ? (
                            <div className="text-sm text-slate-300 leading-relaxed whitespace-pre-line font-sans">
                              {summarySnippet.text}
                            </div>
                          ) : (
                            <p className="text-slate-500 italic text-sm">No summary available.</p>
                          )}
                        </div>
                      </div>
                    </div>
                  </div>
                </div>
              </div>
            );
          })}

          {!loading && data && results.length === 0 && (
            <div className="rounded-2xl border border-dashed border-white/10 p-8 text-center">
              <div className="flex justify-center mb-4">
                <div className="p-3 rounded-full bg-slate-800/50 text-slate-500">
                  <Search className="w-6 h-6" />
                </div>
              </div>
              <p className="text-slate-400 font-medium">No suitable matches found.</p>
              <p className="text-slate-500 text-sm mt-1">Try adjusting your search terms or increasing the limit.</p>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
