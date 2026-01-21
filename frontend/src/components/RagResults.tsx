import { useEffect, useState } from "react";
import { Search, Sparkles, FileText, Database, Layers, ArrowRight, X, ChevronDown, ChevronRight, Activity } from "lucide-react";

type RagSnippet = {
  text: string;
  chunk_type: string; // "summary" | "status" | "note" | etc
};

type RagResult = {
  ticket_id: string;
  score: number;
  reason_for_credit: string | null;
  root_cause?: string | null;
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

function compactSnippet(text: string, maxLen = 220) {
  const clean = text.replace(/\s+/g, " ").trim();
  if (clean.length <= maxLen) return clean;
  return clean.slice(0, maxLen - 1) + "…";
}

export function RagResults({
  apiBase = import.meta.env.VITE_API_BASE ?? "http://127.0.0.1:8000",
}: {
  apiBase?: string;
}) {
  const [query, setQuery] = useState("");
  const [topK, setTopK] = useState(10);
  const [openRefsFor, setOpenRefsFor] = useState<string | null>(null);
  const [openTraceFor, setOpenTraceFor] = useState<string | null>(null);
  const [expandedSnippets, setExpandedSnippets] = useState<string | null>(null);
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
        <div className="flex flex-col md:flex-row items-start md:items-center justify-between gap-4">
          <div>
            <div className="flex items-center gap-3 mb-2">
              <div className="p-2 rounded-lg bg-gradient-to-br from-cyan-500/20 to-blue-600/20 border border-white/10 shadow-lg shadow-cyan-900/20">
                <Database className="w-5 h-5 text-cyan-400" />
              </div>
              <h2 className="text-2xl font-bold font-display bg-gradient-to-r from-white via-slate-100 to-slate-400 bg-clip-text text-transparent">
                Neural Search
              </h2>
            </div>
            <p className="text-slate-400 text-sm">Semantic analysis of ticket data and credit trends</p>
          </div>
        </div>

        {/* Search Bar */}
        <div className="relative group z-10">
          <div className="absolute -inset-0.5 bg-gradient-to-r from-cyan-500/20 to-indigo-500/20 rounded-xl blur opacity-50 group-hover:opacity-100 transition duration-500"></div>
          <div className="relative flex items-center bg-obsidian-900/80 backdrop-blur-xl border border-white/10 rounded-xl p-2 shadow-2xl">
            <div className="pl-3 text-cyan-400">
              <Search className="w-5 h-5" />
            </div>
            <input
              className="flex-1 bg-transparent border-none px-4 py-2 text-slate-200 placeholder:text-slate-600 focus:ring-0 focus:outline-none font-sans"
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              placeholder='Try: "priced wrong", "uom pricing error", "big credit spike"'
              onKeyDown={(e) => e.key === 'Enter' && runSearch()}
            />

            <div className="h-6 w-[1px] bg-white/10 mx-2"></div>

            <div className="flex items-center gap-2 pr-2">
              <span className="text-xs text-slate-500 font-bold uppercase tracking-wider hidden md:block">Limit</span>
              <input
                className="w-16 bg-slate-800/50 border border-white/10 rounded-lg px-2 py-1.5 text-center text-sm text-slate-200 focus:border-cyan-500/50 focus:outline-none transition-colors"
                type="number"
                min={1}
                max={50}
                value={topK}
                onChange={(e) => setTopK(Number(e.target.value))}
              />
            </div>

            <div className="h-6 w-[1px] bg-white/10 mx-2"></div>

            <button
              className="px-6 py-2 rounded-lg bg-cyan-500/10 hover:bg-cyan-500/20 border border-cyan-500/20 text-cyan-400 font-medium transition-all duration-300 hover:shadow-[0_0_15px_rgba(6,182,212,0.3)] disabled:opacity-50 disabled:cursor-not-allowed group/btn relative overflow-hidden"
              onClick={runSearch}
              disabled={loading}
            >
              <div className="absolute inset-0 bg-gradient-to-r from-cyan-400/0 via-cyan-400/10 to-cyan-400/0 translate-x-[-100%] group-hover/btn:translate-x-[100%] transition-transform duration-700"></div>
              <span className="relative flex items-center gap-2">
                {loading ? (
                  <>
                    <Activity className="w-4 h-4 animate-spin" />
                    <span>Processing...</span>
                  </>
                ) : (
                  <>
                    <Sparkles className="w-4 h-4" />
                    <span>Analyze</span>
                  </>
                )}
              </span>
            </button>
          </div>
        </div>

        {error && (
          <div className="rounded-xl border border-red-500/20 bg-red-500/10 p-4 text-sm text-red-200 flex items-center gap-3 animate-fade-in">
            <div className="p-1.5 rounded-full bg-red-500/20">
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
          const evidenceSnippets = (r.snippets ?? []).filter((s) => s !== summarySnippet);
          const head = summarySnippet?.text ?? "";
          const customer = extractCustomer(head);
          const invoice = extractInvoice(head);
          const totalCredit = extractTotalCredit(head);
          const scoreLabel = r.ui?.score_label ?? "Similarity Score";
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
          const summaryHasStatus = /\[(\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2})\]|\bstatus:\b/i.test(
            head
          );
          const latestStatusNoteRe =
            /\blatest status\b|^\s*(wip|open)\s*:|\bnot started\b|\bon macro\b/i;
          const filteredEvidence = summaryHasStatus
            ? evidenceSnippets.filter((s) => !latestStatusNoteRe.test(s.text || ""))
            : evidenceSnippets;

          const trace = traceMap[r.ticket_id];
          return (
            <div
              key={r.ticket_id}
              className="group relative bg-obsidian-900/40 backdrop-blur-md border border-white/[0.06] rounded-2xl p-6 transition-all duration-300 hover:bg-obsidian-900/60 hover:border-cyan-500/30 hover:shadow-lg hover:shadow-cyan-900/10"
            >
              {/* Card Glow */}
              <div className="absolute inset-0 bg-gradient-to-br from-cyan-500/[0.02] to-transparent rounded-2xl opacity-0 group-hover:opacity-100 transition-opacity pointer-events-none"></div>

              <div className="relative flex flex-col md:flex-row items-start justify-between gap-6">
                <div className="flex-1 space-y-4">
                  <div className="flex items-center gap-3">
                    <div className="w-10 h-10 rounded-xl bg-slate-800/50 flex items-center justify-center border border-white/5 group-hover:border-cyan-500/30 transition-colors">
                      <FileText className="w-5 h-5 text-slate-400 group-hover:text-cyan-400 transition-colors" />
                    </div>
                    <div>
                      <div className="text-xl font-bold font-display text-slate-200 group-hover:text-white transition-colors flex items-center gap-2">
                        {r.ticket_id}
                        {isCompleted && (
                          <span className="px-2 py-0.5 rounded text-[10px] font-bold bg-emerald-500/10 border border-emerald-500/30 text-emerald-300 uppercase tracking-wider">
                            Completed
                          </span>
                        )}
                        {r.score > 0.8 && !isCompleted && (
                          <span className="px-2 py-0.5 rounded text-[10px] font-bold bg-emerald-500/10 border border-emerald-500/20 text-emerald-400 uppercase tracking-wider">High Match</span>
                        )}
                      </div>
                      <div className="flex items-center gap-2 text-xs font-mono text-cyan-400/80 mt-0.5">
                        <span>{scoreLabel}:</span>
                        <div className="h-1.5 w-16 bg-slate-800 rounded-full overflow-hidden">
                          <div className="h-full bg-cyan-400" style={{ width: `${Math.min(scoreValue * 100, 100)}%` }}></div>
                        </div>
                        <span>{formatScore(scoreValue)}</span>
                        <span
                          className="text-slate-500 cursor-help"
                          title="Scores are semantic similarity, so only exact matches are 1.0."
                        >
                          ⓘ
                        </span>
                      </div>
                    </div>
                  </div>

                  <div className="flex flex-wrap gap-2 text-sm">
                    {rootCause && (
                      <div className="px-3 py-1.5 rounded-lg bg-amber-500/10 border border-amber-500/20 text-amber-300 flex items-center gap-2">
                        <span className="text-amber-200/80 text-xs font-bold uppercase">Root Cause</span>
                        <span className="font-medium text-white">{rootCause}</span>
                      </div>
                    )}
                    {customer && (
                      <div className="px-3 py-1.5 rounded-lg bg-white/[0.03] border border-white/[0.06] text-slate-300 flex items-center gap-2">
                        <span className="text-slate-500 text-xs font-bold uppercase">Customer</span>
                        <span className="font-medium text-white">{customer}</span>
                      </div>
                    )}
                    {invoice && (
                      <div className="px-3 py-1.5 rounded-lg bg-white/[0.03] border border-white/[0.06] text-slate-300 flex items-center gap-2">
                        <span className="text-slate-500 text-xs font-bold uppercase">Invoice</span>
                        <span className="font-mono text-cyan-300">{invoice}</span>
                      </div>
                    )}
                    {totalCredit && (
                      <div className="px-3 py-1.5 rounded-lg bg-white/[0.03] border border-white/[0.06] text-slate-300 flex items-center gap-2">
                        <span className="text-slate-500 text-xs font-bold uppercase">Total</span>
                        <span className="font-mono text-emerald-400">${totalCredit}</span>
                      </div>
                    )}
                    {r.has_multiple_invoices && (
                      <div className="px-3 py-1.5 rounded-lg bg-violet-500/10 border border-violet-500/20 text-violet-300 flex items-center gap-2">
                        <Layers className="w-3.5 h-3.5" />
                        <span>Multi-invoice</span>
                      </div>
                    )}
                  </div>
                </div>

                <div className="flex flex-col sm:flex-row items-stretch sm:items-center gap-3 w-full md:w-auto">
                  {/* Action Buttons */}
                  <button
                    className="px-4 py-2 rounded-lg bg-slate-800/50 hover:bg-slate-800 border border-white/10 hover:border-white/20 text-slate-300 hover:text-white text-sm font-medium transition-all flex items-center justify-center gap-2"
                    onClick={() => {
                      console.log("open ticket", r.ticket_id);
                    }}
                  >
                    Open Ticket
                    <ArrowRight className="w-4 h-4" />
                  </button>
                  <button
                    className={`px-4 py-2 rounded-lg border text-sm font-medium transition-all flex items-center justify-center gap-2 ${openRefsFor === r.ticket_id
                        ? 'bg-cyan-500/10 border-cyan-500/30 text-cyan-300'
                        : 'bg-slate-800/50 hover:bg-slate-800 border-white/10 hover:border-white/20 text-slate-300'
                      }`}
                    onClick={() => loadRefs(r.ticket_id)}
                    disabled={!!refsLoading[r.ticket_id]}
                  >
                    {refsLoading[r.ticket_id] ? (
                      <Activity className="w-4 h-4 animate-spin" />
                    ) : (
                      <Database className="w-4 h-4" />
                    )}
                    {openRefsFor === r.ticket_id ? 'Hide Data' : 'View Data'}
                  </button>
                  <button
                    className={`px-4 py-2 rounded-lg border text-sm font-medium transition-all flex items-center justify-center gap-2 ${openTraceFor === r.ticket_id
                        ? 'bg-violet-500/10 border-violet-500/30 text-violet-300'
                        : 'bg-slate-800/50 hover:bg-slate-800 border-white/10 hover:border-white/20 text-slate-300'
                      }`}
                    onClick={() => loadTrace(r.ticket_id, r)}
                    disabled={!!traceLoading[r.ticket_id]}
                  >
                    {traceLoading[r.ticket_id] ? (
                      <Activity className="w-4 h-4 animate-spin" />
                    ) : (
                      <ChevronRight className="w-4 h-4" />
                    )}
                    {openTraceFor === r.ticket_id ? 'Hide Trace' : 'Trace'}
                  </button>
                </div>
              </div>

              {/* Collapsible References Section */}
              <div className={`grid transition-all duration-300 ease-in-out ${openRefsFor === r.ticket_id ? 'grid-rows-[1fr] mt-6 pt-6 border-t border-white/[0.06]' : 'grid-rows-[0fr]'}`}>
                <div className="overflow-hidden">
                  <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                    <div className="bg-obsidian-950/50 rounded-xl p-4 border border-white/[0.06]">
                      <div className="flex items-center justify-between mb-3 text-xs font-bold text-slate-500 uppercase tracking-widest">
                        <span>Linked Invoices</span>
                        <span className="bg-slate-800 px-1.5 py-0.5 rounded text-slate-400">{refsMap[r.ticket_id]?.invoice_ids?.length ?? 0}</span>
                      </div>
                      <div className="flex flex-wrap gap-2">
                        {(refsMap[r.ticket_id]?.invoice_ids ?? []).map((inv) => (
                          <span key={inv} className="px-2.5 py-1 rounded bg-slate-800/50 border border-white/5 text-xs font-mono text-cyan-300/90 hover:border-cyan-500/30 transition-colors cursor-default">
                            {inv}
                          </span>
                        ))}
                        {(refsMap[r.ticket_id]?.invoice_ids ?? []).length === 0 && (
                          <span className="text-sm text-slate-600 italic">No linked invoices found</span>
                        )}
                      </div>
                    </div>

                    <div className="bg-obsidian-950/50 rounded-xl p-4 border border-white/[0.06]">
                      <div className="flex items-center justify-between mb-3 text-xs font-bold text-slate-500 uppercase tracking-widest">
                        <span>Item Numbers</span>
                        <span className="bg-slate-800 px-1.5 py-0.5 rounded text-slate-400">{refsMap[r.ticket_id]?.item_numbers?.length ?? 0}</span>
                      </div>
                      <div className="flex flex-wrap gap-2">
                        {(refsMap[r.ticket_id]?.item_numbers ?? []).map((it) => (
                          <span key={it} className="px-2.5 py-1 rounded bg-slate-800/50 border border-white/5 text-xs font-mono text-emerald-300/90 hover:border-emerald-500/30 transition-colors cursor-default">
                            {it}
                          </span>
                        ))}
                        {(refsMap[r.ticket_id]?.item_numbers ?? []).length === 0 && (
                          <span className="text-sm text-slate-600 italic">No items found</span>
                        )}
                      </div>
                    </div>
                  </div>
                </div>
              </div>

              <div
                className={`grid transition-all duration-300 ease-in-out ${openTraceFor === r.ticket_id ? 'grid-rows-[1fr] mt-6 pt-6 border-t border-white/[0.06]' : 'grid-rows-[0fr]'}`}
              >
                <div className="overflow-hidden">
                  <div className="bg-obsidian-950/50 rounded-xl p-4 border border-white/[0.06]">
                    <div className="flex items-center justify-between mb-3 text-xs font-bold text-slate-500 uppercase tracking-widest">
                      <span>Rule Trace</span>
                      <span className="text-[10px] font-mono text-slate-400">
                        {trace?.decision?.action_rule_id ?? "fallback"}
                      </span>
                    </div>
                    <pre className="text-xs text-slate-300 leading-relaxed whitespace-pre-wrap max-h-80 overflow-auto">
                      {trace ? JSON.stringify(trace, null, 2) : "No trace loaded."}
                    </pre>
                  </div>
                </div>
              </div>

              {/* Evidence Snippets */}
              <div className="mt-6">
                <div className="flex items-center gap-2 mb-3">
                  <Sparkles className="w-3.5 h-3.5 text-slate-500" />
                  <span className="text-xs font-bold text-slate-500 uppercase tracking-widest">Semantic Evidence</span>
                </div>

                <div className="space-y-2">
                  {summarySnippet && (
                    <div className="group/snippet relative rounded-xl border border-white/[0.04] bg-white/[0.02] hover:bg-white/[0.04] p-3 transition-colors">
                      <div className="flex items-center gap-2 mb-1.5">
                        <span className="px-1.5 py-0.5 rounded-[4px] bg-white/5 border border-white/5 text-[10px] font-bold uppercase tracking-wider text-slate-400 group-hover/snippet:text-cyan-400 transition-colors">
                          summary
                        </span>
                      </div>
                      <div className="text-sm text-slate-300 leading-relaxed font-sans opacity-90 whitespace-pre-line">
                        {summarySnippet.text}
                      </div>
                    </div>
                  )}

                  {(closureNote || r.next_action) && (
                    <div className="rounded-xl border border-white/[0.04] bg-white/[0.02] p-3">
                      <div className="flex items-center gap-2 mb-1.5">
                        <span className="px-1.5 py-0.5 rounded-[4px] bg-white/5 border border-white/5 text-[10px] font-bold uppercase tracking-wider text-slate-400">
                          next action
                        </span>
                        {confidenceLabel && (
                          <span
                            className={`px-1.5 py-0.5 rounded-[4px] border text-[10px] font-bold uppercase tracking-wider ${confidenceClass}`}
                          >
                            {confidenceLabel}
                          </span>
                        )}
                      </div>
                      <div className="text-sm text-slate-300 leading-relaxed font-sans opacity-90">
                        {closureNote || r.next_action}
                      </div>
                    </div>
                  )}

                  {creditNumbers.length > 0 && (
                    <div className="rounded-xl border border-white/[0.04] bg-white/[0.02] p-3">
                      <div className="flex items-center justify-between mb-2 text-xs font-bold text-slate-500 uppercase tracking-widest">
                        <span>CR Numbers</span>
                        <span className="bg-slate-800 px-1.5 py-0.5 rounded text-slate-400">
                          {creditNumbers.length}
                        </span>
                      </div>
                      <div className="flex flex-wrap gap-2">
                        {crVisible.map((cr: string) => (
                          <span
                            key={cr}
                            className="px-2.5 py-1 rounded bg-slate-800/50 border border-white/5 text-xs font-mono text-cyan-300/90 hover:border-cyan-500/30 transition-colors cursor-default"
                          >
                            {cr}
                          </span>
                        ))}
                      </div>
                      {creditNumbers.length > 10 && (
                        <div className="mt-2">
                          <button
                            className="text-xs text-cyan-300 hover:text-cyan-200 transition-colors"
                            onClick={() =>
                              setExpandedCrFor(showAllCr ? null : r.ticket_id)
                            }
                          >
                            {showAllCr
                              ? "Show less"
                              : `Show all (${creditNumbers.length})`}
                          </button>
                        </div>
                      )}
                    </div>
                  )}

                  {filteredEvidence.length > 0 && (
                    <>
                      {expandedSnippets === r.ticket_id &&
                        filteredEvidence.map((s, idx) => (
                          <div
                            key={idx}
                            className="group/snippet relative rounded-xl border border-white/[0.04] bg-white/[0.02] hover:bg-white/[0.04] p-3 transition-colors"
                          >
                            <div className="flex items-center gap-2 mb-1.5">
                              <span className="px-1.5 py-0.5 rounded-[4px] bg-white/5 border border-white/5 text-[10px] font-bold uppercase tracking-wider text-slate-400 group-hover/snippet:text-cyan-400 transition-colors">
                                {s.chunk_type}
                              </span>
                            </div>
                            <div className="text-sm text-slate-300 leading-relaxed font-sans opacity-90 whitespace-pre-wrap">
                              {s.chunk_type === "note" ? (s.text || "") : compactSnippet(s.text)}
                            </div>
                          </div>
                        ))}

                      <button
                        onClick={() => setExpandedSnippets(expandedSnippets === r.ticket_id ? null : r.ticket_id)}
                        className="w-full py-2 flex items-center justify-center gap-2 text-xs font-medium text-slate-500 hover:text-cyan-400 transition-colors mt-2 group/expand"
                      >
                        {expandedSnippets === r.ticket_id
                          ? "Hide evidence"
                          : `Show evidence (${filteredEvidence.length})`}
                        {expandedSnippets === r.ticket_id ? (
                          <ChevronDown className="w-3 h-3 group-hover/expand:rotate-180 transition-transform" />
                        ) : (
                          <ChevronRight className="w-3 h-3 group-hover/expand:translate-x-0.5 transition-transform" />
                        )}
                      </button>
                    </>
                  )}
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
