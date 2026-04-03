import React, { useState, useRef, useEffect } from 'react';
import { Sparkles, TrendingUp, Zap, MessageSquare, BarChart, Menu, LogOut, ChevronDown, ChevronUp } from 'lucide-react';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import Mockup from './Mockup';
import RootCauses, { type RootCauseItem } from './RootCauses';
import { RagResults } from './RagResults';
import { Analysis, CustomerAnalysis, ItemAnalysis, type TicketAnalysisMeta, type ItemAnalysisMeta, type CustomerAnalysisMeta } from './Analysis';
import { AnomalyScan } from './AnomalyScan';
import { DataTable } from './chat/DataTable';
import { AutoModePipeline, AutoModeLoading } from './chat/AutoModePipeline';
import { ErrorCard } from './chat/ErrorCard';
import { ChatInput } from './chat/ChatInput';
import { ChatSidebar } from './chat/ChatSidebar';
import type { Message, AskMode, RootCausePayload, UserContext } from './chat/types';
import { getContextualSuggestions } from './chat/types';

// ─── re-export types used by sub-components ────────────────────────────────────
export type { TicketAnalysisMeta, ItemAnalysisMeta, CustomerAnalysisMeta };

type ActusChatProps = {
    userEmail?: string;
    onLogout?: () => void;
};

const SUGGESTED_INTENTS = [
    {
        label: 'Give me a credit overview for the last month',
        query: 'give me a credit overview for the last month',
        mode: 'auto' as const,
    },
    {
        label: 'Give me a credit overview with RTN updates and root causes for the last 6 months',
        query: 'give me a credit overview with RTN updates and root causes for the last 6 months',
        mode: 'auto' as const,
    },
    {
        label: 'Which customers are driving the most credited volume in the last 3 months',
        query: 'which customers are driving the most credited volume in the last 3 months',
        mode: 'auto' as const,
    },
    {
        label: 'Where are billing queue delays accumulating',
        query: 'where are billing queue delays accumulating',
        mode: 'auto' as const,
    },
    {
        label: 'Stalled tickets',
        query: 'show stalled tickets for the last 60 days',
        scope: 'last 60 days',
        mode: 'manual' as const,
    },
    {
        label: 'Credit trends',
        query: 'show me the credit trends',
        mode: 'manual' as const,
    },
] as const;

// ─── Helpers ───────────────────────────────────────────────────────────────────

const createId = () => {
    if (typeof crypto !== 'undefined' && 'randomUUID' in crypto) return crypto.randomUUID();
    return `${Date.now()}-${Math.random().toString(16).slice(2)}`;
};

const formatCompactCurrency = (value: number) =>
    new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD', maximumFractionDigits: 0, notation: 'compact' }).format(value);

const formatCurrency = (value: number | null | undefined) => {
    if (value == null || !Number.isFinite(value)) return 'N/A';
    return new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD', minimumFractionDigits: 2, maximumFractionDigits: 2 }).format(value);
};

const toRootCauseItems = (payload?: RootCausePayload): RootCauseItem[] => {
    const data = payload?.data ?? [];
    return data.map((item, index) => ({
        rank: index + 1,
        rootCause: item.root_cause || 'Unspecified',
        creditRequestTotal: Number(item.credit_request_total || 0),
        recordCount: Number(item.record_count || 0),
    }));
};

const splitInvestigationNote = (content: string) => {
    const marker = '\n\n**Note:**';
    const idx = content.indexOf(marker);
    if (idx === -1) return { collapsed: content, full: content, hasCollapse: false };
    const header = content.slice(0, idx + marker.length);
    const body = content.slice(idx + marker.length).trim();
    if (!body) return { collapsed: header, full: content, hasCollapse: true };
    const firstChunk = body.split(/\n{2,}/)[0] || '';
    const collapsed = firstChunk ? `${header}\n${firstChunk}` : header;
    return { collapsed, full: content, hasCollapse: true };
};

const fmtMsgTime = (ts?: number) => {
    if (!ts) return '';
    return new Intl.DateTimeFormat('en-US', { hour: '2-digit', minute: '2-digit', hour12: false }).format(new Date(ts));
};

const normalizeCustomerPrefix = (value: string) => {
    const text = String(value || '').trim().toUpperCase();
    if (!text) return '';
    const alphaPrefix = text.match(/^[A-Z]+/)?.[0];
    return alphaPrefix || text;
};

// ─── Component ────────────────────────────────────────────────────────────────

export default function ActusChat({ userEmail, onLogout }: ActusChatProps) {
    const apiBase = (
        (import.meta.env.VITE_API_BASE_URL as string | undefined) ??
        (import.meta.env.VITE_API_BASE as string | undefined) ??
        ''
    ).replace(/\/$/, '');

    const [messages, setMessages] = useState<Message[]>([]);
    const [input, setInput] = useState('');
    const [isTyping, setIsTyping] = useState(false);
    const [isSidebarOpen, setIsSidebarOpen] = useState(false);
    const [chartLib, setChartLib] = useState<any>(null);
    const [chartLibError, setChartLibError] = useState(false);
    const [activeView, setActiveView] = useState<'chat' | 'rag'>('chat');
    const [askMode, setAskMode] = useState<AskMode>('auto');
    const [expandedNoteIds, setExpandedNoteIds] = useState<Record<string, boolean>>({});
    const [expandedSystemUpdateIds, setExpandedSystemUpdateIds] = useState<Record<string, boolean>>({});
    const [pendingFollowup, setPendingFollowup] = useState<{ intent: string; prefix: string } | null>(null);
    const [pendingChoices, setPendingChoices] = useState<Array<{ label: string; prefix: string }> | null>(null);

    const messagesEndRef = useRef<HTMLDivElement>(null);
    const lastMessageRef = useRef<HTMLDivElement>(null);
    const scrollContainerRef = useRef<HTMLDivElement>(null);

    // ── User context ───────────────────────────────────────────────────────────

    const fallbackUserContext: UserContext = {
        name: 'Sebastian Rosales', firstName: 'Sebastian',
        role: 'Staff Engineer', location: 'SF Office', lastLogin: '9:12 AM',
    };
    const [userContext, setUserContext] = useState<UserContext>({
        name: '', firstName: '', role: fallbackUserContext.role,
        location: fallbackUserContext.location, lastLogin: fallbackUserContext.lastLogin,
    });

    const resolvedUserEmail =
        userEmail ||
        localStorage.getItem('actusUserEmail') ||
        (import.meta.env.VITE_USER_EMAIL as string | undefined) || '';

    const formatLoginTime = (value?: number | string) => {
        if (!value) return fallbackUserContext.lastLogin;
        const n = typeof value === 'number' ? value : Number(value);
        return Number.isFinite(n)
            ? new Intl.DateTimeFormat('en-US', { hour: 'numeric', minute: '2-digit' }).format(new Date(n))
            : String(value);
    };

    const nameFromEmail = (email: string) => {
        const envName = (import.meta.env.VITE_USER_NAME as string | undefined) || (import.meta.env.VITE_USER_FIRST_NAME as string | undefined);
        if (envName) return envName;
        const base = email.split('@')[0] || '';
        return base
            ? base.split(/[._-]+/).filter(Boolean).map(p => p.charAt(0).toUpperCase() + p.slice(1)).join(' ')
            : fallbackUserContext.name;
    };

    useEffect(() => {
        if (!resolvedUserEmail) return;
        setUserContext(p => ({ ...p, name: '', firstName: '' }));
        const controller = new AbortController();
        (async () => {
            try {
                const endpoint = apiBase
                    ? `${apiBase}/api/user-context?email=${encodeURIComponent(resolvedUserEmail)}`
                    : `/api/user-context?email=${encodeURIComponent(resolvedUserEmail)}`;
                const res = await fetch(endpoint, { signal: controller.signal });
                if (!res.ok) throw new Error(`user context failed ${res.status}`);
                const p = await res.json() as {
                    email?: string; name?: string; first_name?: string; last_name?: string;
                    firstName?: string; lastName?: string; role?: string; location?: string; last_login?: number | string;
                };
                const inferredName = p.name || p.first_name || p.firstName
                    || [p.first_name, p.last_name].filter(Boolean).join(' ')
                    || [p.firstName, p.lastName].filter(Boolean).join(' ');
                const resolvedName = inferredName || nameFromEmail(p.email || resolvedUserEmail);
                const formatRole = (v?: string) => v
                    ? v.split(/[\s_-]+/).filter(Boolean).map(pt => pt.charAt(0).toUpperCase() + pt.slice(1)).join(' ')
                    : fallbackUserContext.role;
                setUserContext({
                    name: resolvedName,
                    firstName: p.firstName || p.first_name || resolvedName.split(' ')[0] || fallbackUserContext.firstName,
                    role: formatRole(p.role),
                    location: p.location || 'Unknown location',
                    lastLogin: formatLoginTime(p.last_login),
                });
            } catch (e) {
                if (e instanceof DOMException && e.name === 'AbortError') return;
                console.warn('Failed to load user context', e);
                setUserContext(p => ({ ...p, name: '', firstName: '' }));
            }
        })();
        return () => controller.abort();
    }, [apiBase, resolvedUserEmail]);

    // ── Recent intents ─────────────────────────────────────────────────────────

    const [recentIntents, setRecentIntents] = useState<string[]>(() => {
        try { const r = localStorage.getItem('actusRecentIntents'); return r ? JSON.parse(r) : []; }
        catch { return []; }
    });

    const addRecentIntent = (intent: string) => {
        setRecentIntents(prev => {
            const next = [intent, ...prev].slice(0, 3);
            localStorage.setItem('actusRecentIntents', JSON.stringify(next));
            return next;
        });
    };

    // ── Chart library ──────────────────────────────────────────────────────────

    useEffect(() => {
        let mounted = true;
        import(/* @vite-ignore */ 'recharts')
            .then(mod => { if (mounted) { setChartLib(mod); setChartLibError(false); } })
            .catch(() => { if (mounted) { setChartLib(null); setChartLibError(true); } });
        return () => { mounted = false; };
    }, []);

    const renderChartPlaceholder = (label: string) => (
        <div className="flex items-center justify-center h-full text-sm text-slate-400 bg-slate-950/40 border border-white/5 rounded-lg">{label}</div>
    );

    const renderCreditAmountPlot = (data: Array<any>) => {
        if (!chartLib) return renderChartPlaceholder(chartLibError ? 'Chart library unavailable.' : 'Loading chart…');
        const { ResponsiveContainer, ComposedChart, Bar, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend } = chartLib;
        return (
            <ResponsiveContainer width="100%" height="100%">
                <ComposedChart data={data}>
                    <CartesianGrid strokeDasharray="3 3" stroke="rgba(148,163,184,0.2)" />
                    <XAxis dataKey="bucket" tick={{ fill: '#94a3b8', fontSize: 11 }} />
                    <YAxis tick={{ fill: '#94a3b8', fontSize: 11 }} tickFormatter={(v: number) => formatCompactCurrency(v)} />
                    <Tooltip
                        cursor={{ fill: 'rgba(255,255,255,0.03)' }}
                        content={({ active, payload, label }: any) => {
                            if (!active || !payload?.length) return null;
                            return (
                                <div className="bg-slate-900/90 border border-white/10 rounded-lg p-3 shadow-xl backdrop-blur-md">
                                    <p className="text-xs text-slate-400 mb-2 font-medium">{label}</p>
                                    <div className="space-y-1.5">
                                        {payload.map((entry: any, i: number) => {
                                            const lbl = entry.name === 'with_cr_usd' ? 'With CR #'
                                                : entry.name === 'without_cr_usd' ? 'Without CR #'
                                                : entry.name === 'trend_usd' ? '3-Month Trend' : 'Total';
                                            return (
                                                <div key={i} className="flex items-center gap-2 text-xs">
                                                    <span className="w-2 h-2 rounded-full" style={{ backgroundColor: entry.color }} />
                                                    <span style={{ color: '#e2e8f0' }}>{lbl}:</span>
                                                    <span className="font-mono font-medium text-white">
                                                        {entry.name !== 'trend_usd' ? '$' : ''}{Number(entry.value || 0).toLocaleString()}
                                                    </span>
                                                </div>
                                            );
                                        })}
                                    </div>
                                </div>
                            );
                        }}
                    />
                    <Legend formatter={(value: any) => {
                        const lbl = value === 'with_cr_usd' ? 'With CR #' : value === 'without_cr_usd' ? 'Without CR #' : value === 'trend_usd' ? '3-Month Trend' : 'Total';
                        return <span style={{ color: value === 'with_cr_usd' ? '#5B7DB1' : '#94a3b8' }}>{lbl}</span>;
                    }} />
                    <Bar dataKey="with_cr_usd" stackId="credits" fill="#0b2a4a" radius={[4, 4, 0, 0]} />
                    <Bar dataKey="without_cr_usd" stackId="credits" fill="#4f6d8a" radius={[4, 4, 0, 0]} />
                    <Line dataKey="trend_usd" stroke="#c1121f" strokeWidth={2.5} dot={{ r: 3 }} />
                </ComposedChart>
            </ResponsiveContainer>
        );
    };

    // ── Scroll management ──────────────────────────────────────────────────────

    useEffect(() => {
        const last = messages[messages.length - 1];
        if (!last || last.role !== 'assistant') return;
        const container = scrollContainerRef.current;
        const messageEl = lastMessageRef.current;
        if (!container || !messageEl) return;
        requestAnimationFrame(() => {
            const cRect = container.getBoundingClientRect();
            const mRect = messageEl.getBoundingClientRect();
            const target = container.scrollTop + (mRect.top - cRect.top) - 140;
            container.scrollTo({ top: Math.max(target, 0), behavior: 'smooth' });
        });
    }, [messages]);

    // ── Note collapse ──────────────────────────────────────────────────────────

    const toggleNoteExpansion = (id: string) =>
        setExpandedNoteIds(p => ({ ...p, [id]: !p[id] }));

    const toggleSystemUpdatesExpansion = (id: string) =>
        setExpandedSystemUpdateIds(p => ({ ...p, [id]: !p[id] }));

    // ── Conversation management ────────────────────────────────────────────────

    const clearMessages = () => {
        setMessages([]);
        setPendingFollowup(null);
        setPendingChoices(null);
    };

    const cancelFollowup = () => setPendingFollowup(null);

    // ── Send message ───────────────────────────────────────────────────────────

    const sendMessage = async (
        messageText: string,
        options?: { showUser?: boolean; bypassPending?: boolean; modeOverride?: AskMode },
    ) => {
        const trimmed = messageText.trim();
        if (!trimmed || isTyping) return;

        const bypassPending = Boolean(options?.bypassPending);
        const shouldUseFollowup = !bypassPending && pendingFollowup && !/^cancel|never mind|nevermind$/i.test(trimmed);
        const choiceMatch = !bypassPending && pendingChoices && /^\d$/.test(trimmed) ? Number(trimmed) : null;
        const resolvedMessage = choiceMatch && pendingChoices && pendingChoices[choiceMatch - 1]
            ? pendingChoices[choiceMatch - 1].prefix
            : shouldUseFollowup ? `${pendingFollowup!.prefix} ${trimmed}` : trimmed;

        if (shouldUseFollowup) setPendingFollowup(null);
        if (choiceMatch) setPendingChoices(null);
        if (bypassPending) { setPendingFollowup(null); setPendingChoices(null); }

        const showUser = options?.showUser !== false;
        if (showUser) {
            setMessages(prev => [...prev, { id: createId(), role: 'user', content: trimmed, timestamp: Date.now() }]);
            setInput('');
        }
        setIsTyping(true);

        const t0 = performance.now();
        try {
            const endpoint = apiBase ? `${apiBase}/api/ask` : '/api/ask';
            const res = await fetch(endpoint, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ query: resolvedMessage, mode: options?.modeOverride ?? askMode }),
            });
            const t1 = performance.now();

            if (!res.ok) throw new Error(`Request failed with ${res.status}`);

            const data = await res.json() as {
                text?: string;
                rows?: Record<string, unknown>[];
                meta?: Message['meta'];
            };
            console.info('[ask] fetch=%dms json=%dms', Math.round(t1 - t0), Math.round(performance.now() - t1));

            const assistantMessage: Message = {
                id: createId(),
                role: 'assistant',
                content: data.text || '',
                rows: Array.isArray(data.rows) ? data.rows : undefined,
                timestamp: Date.now(),
                meta: data.meta,
            };

            const resolvedIntentId = String(data.meta?.intent_id || data.meta?.intent || '').trim().toLowerCase();
            const hasAnomalyColumns = Array.isArray(data.meta?.columns) && data.meta!.columns!.some(col => {
                const n = String(col || '').trim().toLowerCase();
                return n === 'anomaly flag' || n === 'z score' || n === 'anomaly_reason';
            });
            if (resolvedIntentId === 'credit_anomalies' || hasAnomalyColumns) {
                assistantMessage.meta = { ...(assistantMessage.meta || {}), anomaly_scan: true, show_table: false };
            }
            if (data.meta?.follow_up?.intent && data.meta?.follow_up?.prefix) {
                setPendingFollowup({ intent: String(data.meta.follow_up.intent), prefix: String(data.meta.follow_up.prefix) });
            }
            if (Array.isArray(data.meta?.suggestions)) {
                setPendingChoices(
                    data.meta!.suggestions!
                        .filter((item): item is { label: string; prefix: string } => Boolean(item?.label && item?.prefix))
                        .map(item => ({ label: item.label, prefix: item.prefix }))
                );
            }
            if (data.meta?.creditTrends) {
                assistantMessage.creditTrends = {
                    ...data.meta.creditTrends,
                    chartData: [
                        { date: '2025-01-01', withCr: 68000, withoutCr: 4000, trend: 68000 },
                        { date: '2025-02-01', withCr: 35000, withoutCr: 29000, trend: 36000 },
                        { date: '2025-03-01', withCr: 23000, withoutCr: 28000, trend: 32000 },
                        { date: '2025-04-01', withCr: 58000, withoutCr: 4000, trend: 20000 },
                        { date: '2025-05-01', withCr: 31000, withoutCr: 31000, trend: 39000 },
                        { date: '2025-06-01', withCr: 30000, withoutCr: 44000, trend: 30000 },
                        { date: '2025-07-01', withCr: 27000, withoutCr: 52000, trend: 32000 },
                        { date: '2025-08-01', withCr: 42000, withoutCr: 72000, trend: 25000 },
                        { date: '2025-09-01', withCr: 59000, withoutCr: 21000, trend: 44000 },
                        { date: '2025-10-01', withCr: 49000, withoutCr: 20000, trend: 58000 },
                        { date: '2025-11-01', withCr: 38000, withoutCr: 15000, trend: 50000 },
                        { date: '2025-12-01', withCr: 45000, withoutCr: 12000, trend: 39000 },
                    ],
                };
            }

            setMessages(prev => [...prev, assistantMessage]);
            if (showUser) addRecentIntent(trimmed);
        } catch (error) {
            console.error(error);
            const errMsg = error instanceof Error ? error.message : 'Unknown error';
            setMessages(prev => [...prev, {
                id: createId(),
                role: 'assistant',
                content: errMsg,
                isError: true,
                originalQuery: trimmed,
                timestamp: Date.now(),
            }]);
        } finally {
            setIsTyping(false);
        }
    };

    const handleSend = () => sendMessage(input, { showUser: true });

    const handleMessageLinkClick = (event: React.MouseEvent) => {
        const anchor = (event.target as HTMLElement | null)?.closest('a');
        if (!anchor) return;
        const href = anchor.getAttribute('href') || '';
        if (href.startsWith('actus://ask/')) {
            event.preventDefault();
            sendMessage(decodeURIComponent(href.replace('actus://ask/', '')), { showUser: false, bypassPending: true, modeOverride: 'manual' });
        }
    };

    // ── Contextual sidebar suggestions ─────────────────────────────────────────

    const lastAssistantMsg = [...messages].reverse().find(m => m.role === 'assistant' && !m.isError);
    const contextualSuggestions = getContextualSuggestions(lastAssistantMsg?.meta);

    // ─────────────────────────────────────────────────────────────────────────
    // Render
    // ─────────────────────────────────────────────────────────────────────────

    return (
        <div className="flex flex-col h-screen bg-obsidian-950 text-slate-100 overflow-hidden font-sans selection:bg-cyan-500/30">
            {/* Background orbs */}
            <div className="fixed inset-0 overflow-hidden pointer-events-none -z-0">
                <div className="absolute top-[10%] left-[-5%] w-[600px] h-[600px] bg-indigo-900/10 rounded-full blur-[120px] animate-pulse-slow" />
                <div className="absolute bottom-[10%] right-[-5%] w-[500px] h-[500px] bg-cyan-900/10 rounded-full blur-[100px] animate-float" style={{ animationDelay: '2s' }} />
                <div className="absolute top-[40%] left-[60%] w-[400px] h-[400px] bg-violet-900/10 rounded-full blur-[100px] animate-pulse-slow opacity-50" />
            </div>

            {/* ── Header ─────────────────────────────────────────────────────── */}
            <header className="fixed top-0 inset-x-0 z-50 glass border-b-0">
                <div className="max-w-[90rem] mx-auto px-6 h-[88px] flex items-center justify-between">
                    {/* Logo */}
                    <div className="flex items-center gap-4">
                        <div className="relative group">
                            <div className="w-10 h-10 rounded-xl bg-gradient-to-br from-cyan-400 to-indigo-600 flex items-center justify-center shadow-glow group-hover:shadow-glow-lg transition-all duration-300">
                                <Sparkles className="w-5 h-5 text-white" />
                            </div>
                            <div className="absolute -top-1 -right-1 w-2.5 h-2.5 bg-emerald-400 rounded-full border-2 border-obsidian-900 animate-pulse" />
                        </div>
                        <h1 className="text-xl font-bold font-display tracking-tight bg-gradient-to-r from-white via-slate-200 to-slate-400 bg-clip-text text-transparent drop-shadow-sm">
                            Actus
                        </h1>
                    </div>

                    <div className="flex items-center gap-4">
                        {/* View toggle */}
                        <div className="relative grid grid-cols-2 p-1 rounded-full bg-white/[0.04] border border-white/[0.08] backdrop-blur-sm">
                            <div className={`absolute top-1 bottom-1 w-[calc(50%-4px)] rounded-full bg-white/10 transition-all duration-200 ease-out ${activeView === 'chat' ? 'left-1' : 'left-[calc(50%+3px)]'}`} />
                            <button
                                onClick={() => setActiveView('chat')}
                                className={`relative z-10 px-3 py-1.5 text-xs font-semibold rounded-full transition-colors ${activeView === 'chat' ? 'text-white' : 'text-slate-400 hover:text-white'}`}
                            >Chat</button>
                            <button
                                onClick={() => setActiveView('rag')}
                                className={`relative z-10 px-3 py-1.5 text-xs font-semibold rounded-full transition-colors ${activeView === 'rag' ? 'text-white' : 'text-slate-400 hover:text-white'}`}
                            >RAG</button>
                        </div>

                        {onLogout && (
                            <button
                                onClick={onLogout}
                                className="hidden sm:inline-flex items-center gap-2 px-3 py-2 rounded-full bg-white/[0.04] border border-white/[0.08] text-xs font-semibold text-slate-400 hover:text-white hover:bg-white/[0.08] transition-all"
                            >
                                <LogOut className="w-3.5 h-3.5" />
                                Sign out
                            </button>
                        )}

                        <button
                            onClick={() => setIsSidebarOpen(o => !o)}
                            className="p-2.5 hover:bg-white/[0.05] rounded-xl transition-all duration-300 text-slate-400 hover:text-white border border-transparent hover:border-white/[0.05] group"
                        >
                            <Menu className="w-5 h-5 group-hover:scale-110 transition-transform" />
                        </button>
                    </div>
                </div>
            </header>

            {/* ── Sidebar ────────────────────────────────────────────────────── */}
            <ChatSidebar
                isOpen={isSidebarOpen}
                onClose={() => setIsSidebarOpen(false)}
                suggestedIntents={SUGGESTED_INTENTS as unknown as Array<{ label: string; query?: string; scope?: string; mode?: AskMode }>}
                recentIntents={recentIntents}
                userContext={userContext}
                contextualSuggestions={contextualSuggestions}
                onSendMessage={(query, opts) => sendMessage(query, { showUser: true, bypassPending: opts?.bypassPending, modeOverride: opts?.modeOverride })}
                onClearMessages={clearMessages}
                hasMessages={messages.length > 0}
            />

            {/* Scroll edge fades */}
            <div className="fixed top-[88px] inset-x-0 h-10 bg-gradient-to-b from-obsidian-950 to-transparent pointer-events-none z-40" />
            <div className="fixed bottom-0 inset-x-0 h-52 bg-gradient-to-t from-obsidian-950/95 to-transparent pointer-events-none z-40" />

            {/* ── Message area ───────────────────────────────────────────────── */}
            <div ref={scrollContainerRef} className="flex-1 overflow-y-auto pt-[120px] pb-40 scroll-smooth">
                {activeView === 'rag' ? (
                    <div className="max-w-6xl mx-auto px-6 pb-16">
                        <RagResults />
                    </div>
                ) : (
                    <div className="max-w-6xl mx-auto px-6 space-y-8 flex flex-col min-h-full">

                        {/* Empty state */}
                        {messages.length === 0 && (
                            <div className="flex-1 flex flex-col items-center justify-center -mt-20 animate-fade-in z-10">
                                <div className="relative group mb-10">
                                    <div className="absolute -inset-1 bg-gradient-to-r from-cyan-500 to-indigo-600 rounded-full blur opacity-40 group-hover:opacity-75 transition duration-1000 group-hover:duration-200" />
                                    <div className="w-24 h-24 rounded-3xl bg-obsidian-900 border border-white/10 flex items-center justify-center shadow-2xl relative z-10 group-hover:scale-105 transition-transform duration-500">
                                        <Sparkles className="w-12 h-12 text-cyan-400 group-hover:text-white transition-colors duration-500" />
                                    </div>
                                    <div className="absolute -top-2 -right-2 w-6 h-6 bg-emerald-500 rounded-full border-4 border-obsidian-950 animate-pulse z-20" />
                                </div>

                                {(() => {
                                    const displayName = userContext.firstName || userContext.name;
                                    const hour = new Date(new Date().toLocaleString('en-US', { timeZone: 'America/Indiana/Indianapolis' })).getHours();
                                    const greeting = hour < 5 || hour >= 18 ? 'Good evening' : hour < 12 ? 'Good morning' : 'Good afternoon';
                                    return (
                                        <h1 className="text-5xl md:text-6xl font-bold font-display tracking-tight text-center mb-8 flex flex-col items-center gap-3">
                                            <span className="bg-gradient-to-r from-white via-slate-200 to-slate-500 bg-clip-text text-transparent drop-shadow-sm">{greeting},</span>
                                            {displayName
                                                ? <span key={displayName} className="bg-gradient-to-r from-cyan-200 via-cyan-300 to-indigo-400 bg-clip-text text-transparent opacity-0 animate-[fadeInUp_0.8s_ease-out_0.2s_forwards] drop-shadow-sm">{displayName}</span>
                                                : <span className="h-10 md:h-14 w-64 rounded-full bg-white/5 animate-pulse" />
                                            }
                                        </h1>
                                    );
                                })()}

                                <p className="text-slate-400 text-lg text-center mb-12 max-w-lg leading-relaxed font-light">
                                    Actus is ready to analyze real-time data trends and anomalies.
                                </p>

                                <div className="grid grid-cols-1 md:grid-cols-3 gap-5 w-full max-w-3xl">
                                    {[
                                        { label: 'Check Credit Trends', cmd: 'Show me the credit trends', icon: <TrendingUp className="w-5 h-5 text-cyan-400" />, desc: 'Analyze latest movements', accent: 'from-cyan-500/60', hover: 'hover:border-cyan-500/30', iconHover: 'group-hover/card:border-cyan-500/20' },
                                        { label: 'Priority Tickets', cmd: 'Show priority tickets', icon: <Zap className="w-5 h-5 text-amber-400" />, desc: 'View urgent items', accent: 'from-amber-500/60', hover: 'hover:border-amber-500/30', iconHover: 'group-hover/card:border-amber-500/20' },
                                        { label: 'Help Functions', cmd: 'What can you do?', icon: <MessageSquare className="w-5 h-5 text-purple-400" />, desc: 'Explore capabilities', accent: 'from-purple-500/60', hover: 'hover:border-purple-500/30', iconHover: 'group-hover/card:border-purple-500/20' },
                                    ].map((item, idx) => (
                                        <button
                                            key={idx}
                                            onClick={() => sendMessage(item.cmd, { showUser: true, bypassPending: true })}
                                            className={`flex flex-col items-start gap-4 p-5 rounded-2xl bg-white/[0.03] border border-white/[0.06] ${item.hover} hover:bg-white/[0.08] transition-all duration-300 group/card text-left relative overflow-hidden`}
                                        >
                                            {/* Top accent bar */}
                                            <div className={`absolute top-0 left-0 right-0 h-[2px] bg-gradient-to-r ${item.accent} to-transparent opacity-60 group-hover/card:opacity-100 transition-opacity`} />
                                            <div className="absolute top-0 right-0 p-4 opacity-0 group-hover/card:opacity-100 transition-opacity">
                                                <div className="w-16 h-16 bg-gradient-to-bl from-white/[0.04] to-transparent rounded-full blur-xl" />
                                            </div>
                                            <div className={`p-2.5 rounded-xl bg-obsidian-950 border border-white/10 ${item.iconHover} transition-all duration-300`}>
                                                {item.icon}
                                            </div>
                                            <div>
                                                <span className="block text-sm font-bold text-slate-200 group-hover/card:text-white mb-1">{item.label}</span>
                                                <span className="block text-xs text-slate-500 group-hover/card:text-slate-400">{item.desc}</span>
                                            </div>
                                        </button>
                                    ))}
                                </div>
                            </div>
                        )}

                        {/* Message list */}
                        {messages.map((message, idx) => {
                            const noteSummary = message.meta?.note_summary;
                            const noteSummarySource = String(noteSummary?.source || '').trim();
                            const noteSummarySourceLabel = noteSummarySource === 'openrouter_primary' ? 'OpenRouter'
                                : noteSummarySource === 'openrouter_fallback' ? 'OpenRouter (fallback)' : '';
                            const noteSummaryModel = String(noteSummary?.model || '').trim();
                            const isNoteExpanded = expandedNoteIds[message.id] ?? false;
                            const noteSplit = splitInvestigationNote(message.content ?? '');
                            const shouldCollapseNote = Boolean(noteSummary && noteSplit.hasCollapse);
                            const noteDisplayContent = shouldCollapseNote && !isNoteExpanded ? noteSplit.collapsed : noteSplit.full;

                            const systemUpdatesSummary = message.meta?.system_updates_summary;
                            const isSystemUpdatesExpanded = expandedSystemUpdateIds[message.id] ?? false;
                            const visibleSystemBatches = Array.isArray(systemUpdatesSummary?.batches)
                                ? (isSystemUpdatesExpanded
                                    ? systemUpdatesSummary!.batches
                                    : systemUpdatesSummary!.batches.slice(0, Math.max(1, systemUpdatesSummary!.recent_limit || 3)))
                                : [];

                            const isAnomalyMessage = Boolean(
                                message.meta?.anomaly_scan
                                || message.meta?.intent_id === 'credit_anomalies'
                                || message.meta?.intent === 'credit_anomalies'
                            );
                            const isAutoModeMessage = Boolean(message.meta?.auto_mode?.enabled);
                            const executedIntents = Array.isArray(message.meta?.auto_mode?.executed_intents)
                                ? message.meta!.auto_mode!.executed_intents!
                                : [];

                            const hideTextForDashboard = message.role === 'assistant'
                                && !isAutoModeMessage
                                && (message.creditTrends || message.meta?.rootCauses || isAnomalyMessage);

                            return (
                                <div
                                    key={message.id}
                                    ref={idx === messages.length - 1 ? lastMessageRef : undefined}
                                    className={`flex gap-4 animate-fade-in-up group/msg ${message.role === 'user' ? 'justify-end' : 'justify-start'}`}
                                >
                                    {/* Assistant avatar */}
                                    {message.role === 'assistant' && (
                                        <div className="w-8 h-8 rounded-lg bg-gradient-to-br from-cyan-500 to-indigo-600 flex items-center justify-center flex-shrink-0 shadow-lg shadow-cyan-500/10 mt-1">
                                            <Sparkles className="w-4 h-4 text-white" />
                                        </div>
                                    )}

                                    <div className={`max-w-[90%] flex flex-col gap-4 ${message.role === 'user' ? 'items-end' : 'items-start'}`}>

                                        {/* Auto-mode pipeline */}
                                        {message.role === 'assistant' && isAutoModeMessage && executedIntents.length > 0 && (
                                            <AutoModePipeline
                                                executedIntents={executedIntents}
                                                primaryIntent={message.meta?.auto_mode?.primary_intent}
                                                planner={message.meta?.auto_mode?.planner}
                                                subintentCount={message.meta?.auto_mode?.subintent_count}
                                            />
                                        )}

                                        {/* Error card */}
                                        {message.role === 'assistant' && message.isError && (
                                            <ErrorCard
                                                message={message.content}
                                                onRetry={message.originalQuery
                                                    ? () => sendMessage(message.originalQuery!, { showUser: true, bypassPending: true })
                                                    : undefined}
                                            />
                                        )}

                                        {/* Investigation note summary */}
                                        {message.role === 'assistant' && !message.isError && noteSummary && (
                                            <div className="w-full max-w-3xl bg-emerald-500/10 border border-emerald-400/20 rounded-2xl overflow-hidden shadow-2xl shadow-emerald-500/10 backdrop-blur-md p-5">
                                                <div className="flex items-start justify-between gap-4">
                                                    <div>
                                                        <div className="text-xs uppercase tracking-[0.2em] text-emerald-200/80 font-semibold">Summary (suggested)</div>
                                                        <div className="text-[11px] text-emerald-200/60 mt-1">{noteSummary.disclaimer || 'Generated by LLM'}</div>
                                                    </div>
                                                    <div className="flex flex-col items-end gap-1">
                                                        <span className="text-[10px] font-semibold uppercase tracking-[0.2em] text-emerald-300/60">Snapshot</span>
                                                        {noteSummarySourceLabel && (
                                                            <span className="text-[10px] text-emerald-200/70 font-mono px-2 py-0.5 rounded bg-white/[0.04] border border-white/[0.08]">
                                                                {noteSummarySourceLabel}{noteSummaryModel ? ` · ${noteSummaryModel}` : ''}
                                                            </span>
                                                        )}
                                                    </div>
                                                </div>
                                                <ul className="mt-3 space-y-2 text-sm text-emerald-100">
                                                    {noteSummary.bullets.map((bullet, i) => (
                                                        <li key={`${message.id}-bullet-${i}`} className="flex items-start gap-2">
                                                            <span className="mt-1 w-1.5 h-1.5 rounded-full bg-emerald-300 flex-shrink-0" />
                                                            <span>{bullet}</span>
                                                        </li>
                                                    ))}
                                                </ul>
                                            </div>
                                        )}

                                        {/* Text content */}
                                        {!message.isError && !hideTextForDashboard && Boolean(message.content?.trim()) && (
                                            <div className={`p-5 rounded-3xl backdrop-blur-md transition-all duration-300 shadow-xl ${
                                                message.role === 'user'
                                                    ? 'bg-indigo-900/20 text-indigo-50 shadow-indigo-900/20 rounded-tr-sm border border-indigo-500/20'
                                                    : `bg-transparent text-slate-200 border border-transparent${isAutoModeMessage ? ' border-l-2 border-l-cyan-500/20' : ''}`
                                            }`}>
                                                <div className="text-[15px] leading-relaxed font-normal" onClick={handleMessageLinkClick}>
                                                    {message.role === 'user' ? (
                                                        message.content
                                                    ) : (
                                                        <ReactMarkdown
                                                            remarkPlugins={[remarkGfm]}
                                                            components={{
                                                                p: ({ children }) => <p className="mb-3 last:mb-0">{children}</p>,
                                                                strong: ({ children }) => <strong className="font-bold text-white">{children}</strong>,
                                                                ul: ({ children }) => <ul className="list-disc pl-4 mb-3 space-y-1 marker:text-cyan-400">{children}</ul>,
                                                                ol: ({ children }) => <ol className="list-decimal pl-4 mb-3 space-y-1 marker:text-cyan-400">{children}</ol>,
                                                                li: ({ children }) => <li className="text-slate-300 pl-1">{children}</li>,
                                                                h1: ({ children }) => <h1 className="text-2xl font-bold tracking-tight text-white mb-4 mt-2 pb-2 border-b border-white/[0.08] font-display">{children}</h1>,
                                                                a: ({ href, children }) => {
                                                                    if (href?.startsWith('actus://ask/')) {
                                                                        const decoded = decodeURIComponent(href.replace('actus://ask/', ''));
                                                                        return (
                                                                            <button
                                                                                type="button"
                                                                                onClick={e => { e.preventDefault(); e.stopPropagation(); sendMessage(decoded, { showUser: false, bypassPending: true, modeOverride: 'manual' }); }}
                                                                                className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-md bg-cyan-500/10 text-cyan-200 border border-cyan-500/20 hover:bg-cyan-500/20 transition-all text-xs font-semibold uppercase tracking-wide"
                                                                            >{children}</button>
                                                                        );
                                                                    }
                                                                    return <a href={href} className="text-cyan-400 hover:text-cyan-300 underline decoration-cyan-400/30 underline-offset-2 hover:decoration-cyan-300" target="_blank" rel="noreferrer">{children}</a>;
                                                                },
                                                                code: ({ children }) => {
                                                                    const text = Array.isArray(children) ? children.join('') : String(children);
                                                                    if (text.startsWith('ask:')) {
                                                                        const payload = text.slice(4);
                                                                        const divider = payload.lastIndexOf('|');
                                                                        const encoded = divider === -1 ? payload : payload.slice(0, divider);
                                                                        const label = divider === -1 ? 'Open' : payload.slice(divider + 1);
                                                                        return (
                                                                            <button
                                                                                type="button"
                                                                                onClick={() => sendMessage(decodeURIComponent(encoded).trim(), { showUser: false, bypassPending: true, modeOverride: 'manual' })}
                                                                                className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-md bg-cyan-500/10 text-cyan-200 border border-cyan-500/20 hover:bg-cyan-500/20 transition-all text-xs font-semibold uppercase tracking-wide"
                                                                            >{label?.trim() || 'Open'}</button>
                                                                        );
                                                                    }
                                                                    return <code className="bg-obsidian-950/50 rounded px-1.5 py-0.5 text-sm font-mono text-cyan-300 border border-white/5">{children}</code>;
                                                                },
                                                            }}
                                                        >
                                                            {noteDisplayContent}
                                                        </ReactMarkdown>
                                                    )}
                                                </div>
                                            </div>
                                        )}

                                        {/* Expand/collapse note */}
                                        {message.role === 'assistant' && shouldCollapseNote && (
                                            <button
                                                type="button"
                                                onClick={() => toggleNoteExpansion(message.id)}
                                                className="text-xs font-semibold uppercase tracking-[0.2em] text-cyan-200/80 hover:text-cyan-100 transition-colors"
                                            >
                                                {isNoteExpanded ? 'Hide full note' : 'View full note'}
                                            </button>
                                        )}

                                        {/* System updates batches */}
                                        {message.role === 'assistant' && systemUpdatesSummary && visibleSystemBatches.length > 0 && (
                                            <div className="w-full max-w-4xl bg-obsidian-950/40 border border-white/[0.08] rounded-3xl overflow-hidden shadow-2xl backdrop-blur-xl">
                                                <div className="px-6 py-4 border-b border-white/[0.04] flex items-center justify-between gap-4 bg-gradient-to-b from-white/[0.02] to-transparent">
                                                    <div>
                                                        <div className="text-[10px] font-bold text-cyan-400 uppercase tracking-[0.2em]">RTN Update Batches</div>
                                                        <div className="mt-1 text-sm text-slate-300">
                                                            {isSystemUpdatesExpanded
                                                                ? `Showing all ${systemUpdatesSummary.total_update_dates.toLocaleString()} update date${systemUpdatesSummary.total_update_dates === 1 ? '' : 's'}`
                                                                : `Showing ${visibleSystemBatches.length} most recent update date${visibleSystemBatches.length === 1 ? '' : 's'}`}
                                                        </div>
                                                    </div>
                                                    {systemUpdatesSummary.total_update_dates > visibleSystemBatches.length && (
                                                        <button
                                                            type="button"
                                                            onClick={() => toggleSystemUpdatesExpansion(message.id)}
                                                            className="inline-flex items-center gap-2 px-3 py-1.5 rounded-lg bg-cyan-500/10 hover:bg-cyan-500/20 border border-cyan-500/20 text-xs font-semibold text-cyan-300 transition-colors"
                                                        >
                                                            <ChevronDown className="w-3.5 h-3.5" />
                                                            Show all {systemUpdatesSummary.total_update_dates.toLocaleString()}
                                                        </button>
                                                    )}
                                                    {systemUpdatesSummary.total_update_dates <= visibleSystemBatches.length
                                                        && systemUpdatesSummary.total_update_dates > (systemUpdatesSummary.recent_limit || 3) && (
                                                        <button
                                                            type="button"
                                                            onClick={() => toggleSystemUpdatesExpansion(message.id)}
                                                            className="inline-flex items-center gap-2 px-3 py-1.5 rounded-lg bg-white/[0.04] hover:bg-white/[0.08] border border-white/[0.08] text-xs font-semibold text-slate-300 transition-colors"
                                                        >
                                                            <ChevronUp className="w-3.5 h-3.5" />
                                                            Show fewer
                                                        </button>
                                                    )}
                                                </div>
                                                <div className="px-6 py-4 space-y-2">
                                                    {visibleSystemBatches.map(batch => (
                                                        <div
                                                            key={`${message.id}-${batch.date}`}
                                                            className="flex items-center justify-between rounded-2xl border border-white/[0.05] bg-white/[0.02] px-4 py-3"
                                                        >
                                                            <div className="flex flex-col gap-1">
                                                                <span className="text-sm text-slate-300">Updated on {batch.date}</span>
                                                                <span className="text-xs text-slate-500">Total amount {batch.credit_total_display || formatCurrency(batch.credit_total)}</span>
                                                            </div>
                                                            <span className="text-sm font-semibold text-white">{batch.count.toLocaleString()}</span>
                                                        </div>
                                                    ))}
                                                </div>
                                            </div>
                                        )}

                                        {/* Credit trends dashboard */}
                                        {message.role === 'assistant' && !isAutoModeMessage && message.creditTrends && (
                                            <Mockup creditTrends={message.creditTrends} />
                                        )}

                                        {/* Credit amount chart */}
                                        {message.role === 'assistant' && !isAutoModeMessage && message.meta?.chart?.kind === 'credit_amount_trend' && (
                                            <div className="w-full max-w-6xl bg-obsidian-950/40 border border-white/[0.08] rounded-3xl overflow-hidden shadow-2xl backdrop-blur-xl p-6 hover:border-cyan-500/30 transition-all text-white">
                                                <div className="flex items-center gap-3 mb-4">
                                                    <div className="p-2 rounded-xl bg-gradient-to-br from-cyan-500/20 to-blue-600/20 border border-cyan-500/20">
                                                        <BarChart className="w-5 h-5 text-cyan-400" />
                                                    </div>
                                                    <h2 className="text-2xl font-bold font-display tracking-tight text-white drop-shadow-sm">Credit Amount Trend</h2>
                                                </div>
                                                <div className="text-xs text-slate-400 mb-4">
                                                    Window: {message.meta.chart.window} · Bucketing: {message.meta.chart.bucket}
                                                </div>
                                                <div className="h-[440px] min-w-[900px] w-full overflow-x-auto">
                                                    {renderCreditAmountPlot(message.meta.chart.data)}
                                                </div>
                                            </div>
                                        )}

                                        {/* Mixed lines summary */}
                                        {message.role === 'assistant' && !isAutoModeMessage && message.meta?.mixedLinesSummary && (
                                            <div className="w-full max-w-4xl bg-obsidian-950/40 border border-white/[0.08] rounded-3xl overflow-hidden shadow-2xl backdrop-blur-xl p-6 md:p-8 hover:border-emerald-500/30 transition-all text-white">
                                                <div className="flex items-center gap-3 mb-6">
                                                    <div className="p-2 rounded-xl bg-gradient-to-br from-emerald-500/20 to-teal-600/20 border border-emerald-500/20">
                                                        <BarChart className="w-5 h-5 text-emerald-400" />
                                                    </div>
                                                    <h2 className="text-2xl font-bold font-display tracking-tight text-white drop-shadow-sm">Mixed Lines Summary</h2>
                                                </div>
                                                <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-4">
                                                    {[
                                                        { label: 'Mixed Tickets', value: message.meta.mixedLinesSummary.mixedTicketCount.toLocaleString(), color: 'text-white border-white/[0.04]' },
                                                        { label: 'Without CR Count', value: message.meta.mixedLinesSummary.withoutCrCount.toLocaleString(), color: 'text-white border-white/[0.04]' },
                                                        { label: 'Total Credits', value: formatCurrency(message.meta.mixedLinesSummary.totalUsd), color: 'text-emerald-400 border-emerald-500/20' },
                                                    ].map(({ label, value, color }) => (
                                                        <div key={label} className={`bg-obsidian-900 border rounded-2xl p-5 ${color.includes('emerald') ? 'border-emerald-500/20 shadow-[0_0_20px_rgba(16,185,129,0.05)]' : 'border-white/[0.04]'}`}>
                                                            <div className={`text-[10px] ${color.includes('emerald') ? 'text-emerald-500/80' : 'text-slate-500'} uppercase tracking-[0.2em] font-bold`}>{label}</div>
                                                            <div className={`mt-2 text-3xl font-bold font-display tracking-tight ${color.split(' ')[0]}`}>{value}</div>
                                                        </div>
                                                    ))}
                                                </div>
                                                <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                                                    <div className="bg-white/[0.02] border border-cyan-500/20 rounded-2xl p-5">
                                                        <div className="text-[10px] text-cyan-500/80 uppercase tracking-[0.2em] font-bold">With CR Total</div>
                                                        <div className="mt-2 text-2xl font-bold text-cyan-400 font-display tracking-tight">{formatCurrency(message.meta.mixedLinesSummary.withCrUsd)}</div>
                                                    </div>
                                                    <div className="bg-white/[0.02] border border-amber-500/20 rounded-2xl p-5">
                                                        <div className="text-[10px] text-amber-500/80 uppercase tracking-[0.2em] font-bold">Without CR Total</div>
                                                        <div className="mt-2 text-2xl font-bold text-amber-400 font-display tracking-tight">{formatCurrency(message.meta.mixedLinesSummary.withoutCrUsd)}</div>
                                                    </div>
                                                </div>
                                            </div>
                                        )}

                                        {/* Ticket analysis */}
                                        {message.role === 'assistant' && !isAutoModeMessage && message.meta?.ticket_analysis && (
                                            <Analysis
                                                data={message.meta.ticket_analysis}
                                                suggestions={message.meta.suggestions}
                                                onSuggestionClick={q => sendMessage(q, { showUser: true, bypassPending: true, modeOverride: 'manual' })}
                                            />
                                        )}

                                        {/* Item analysis */}
                                        {message.role === 'assistant' && !isAutoModeMessage && message.meta?.item_analysis && (
                                            <ItemAnalysis
                                                data={message.meta.item_analysis}
                                                suggestions={message.meta.suggestions}
                                                onSuggestionClick={q => sendMessage(q, { showUser: true, bypassPending: true, modeOverride: 'manual' })}
                                            />
                                        )}

                                        {/* Customer analysis */}
                                        {message.role === 'assistant' && !isAutoModeMessage && message.meta?.customer_analysis && (
                                            <CustomerAnalysis
                                                data={message.meta.customer_analysis}
                                                suggestions={message.meta.suggestions}
                                                onSuggestionClick={q => sendMessage(q, { showUser: true, bypassPending: true, modeOverride: 'manual' })}
                                            />
                                        )}

                                        {/* Anomaly scan */}
                                        {message.role === 'assistant' && !isAutoModeMessage && isAnomalyMessage && (
                                            <AnomalyScan
                                                rows={Array.isArray(message.rows) ? message.rows : []}
                                                csvRows={Array.isArray(message.meta?.csv_rows) ? message.meta!.csv_rows! : []}
                                                onReviewTicket={ticketId => {
                                                    const t = String(ticketId || '').trim();
                                                    if (t) sendMessage(`ticket status ${t}`, { showUser: true, bypassPending: true, modeOverride: 'manual' });
                                                }}
                                            />
                                        )}

                                        {/* Root causes */}
                                        {message.role === 'assistant' && !isAutoModeMessage && message.meta?.rootCauses && (
                                            <RootCauses
                                                data={toRootCauseItems(message.meta.rootCauses)}
                                                period={message.meta.rootCauses.period}
                                            />
                                        )}

                                        {/* Data table */}
                                        {message.role === 'assistant' && !isAutoModeMessage
                                            && Array.isArray(message.rows) && message.rows.length > 0
                                            && message.meta?.show_table !== false && (
                                            <DataTable
                                                rows={message.rows}
                                                meta={message.meta}
                                                onDrillDown={(type, value) => {
                                                    const normalizedValue = String(value || '').trim();
                                                    const queries: Record<'ticket' | 'customer' | 'item', string> = {
                                                        ticket: `ticket status ${normalizedValue}`,
                                                        customer: `analyze account ${normalizeCustomerPrefix(normalizedValue)}`,
                                                        item: `analyze item ${normalizedValue}`,
                                                    };
                                                    sendMessage(queries[type], { showUser: true, bypassPending: true, modeOverride: 'manual' });
                                                }}
                                            />
                                        )}

                                        {/* Timestamp — appears on hover */}
                                        {message.timestamp && (
                                            <span className={`text-[10px] text-slate-800 group-hover/msg:text-slate-600 transition-colors duration-200 font-mono select-none ${message.role === 'user' ? 'self-end' : 'self-start ml-1'}`}>
                                                {fmtMsgTime(message.timestamp)}
                                            </span>
                                        )}
                                    </div>

                                    {/* No user avatar — right-alignment is sufficient signal */}
                                </div>
                            );
                        })}

                        {/* Typing indicator */}
                        {isTyping && <AutoModeLoading mode={askMode} />}

                        <div ref={messagesEndRef} className="h-4" />
                    </div>
                )}
            </div>

            {/* ── Input ──────────────────────────────────────────────────────── */}
            {activeView === 'chat' && (
                <ChatInput
                    value={input}
                    onChange={setInput}
                    onSend={handleSend}
                    isTyping={isTyping}
                    askMode={askMode}
                    onModeChange={setAskMode}
                    isSidebarOpen={isSidebarOpen}
                    pendingFollowup={pendingFollowup}
                    onCancelFollowup={cancelFollowup}
                />
            )}
        </div>
    );
}
