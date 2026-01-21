import React, { useState, useRef, useEffect } from 'react';
import { Send, TrendingUp, Sparkles, Zap, MessageSquare, BarChart, X, Menu, Download, Table as TableIcon, LogOut } from 'lucide-react';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import Mockup from './Mockup';
import RootCauses, { type RootCauseItem } from './RootCauses';
import { RagResults } from './RagResults';

type ActusChatProps = {
    userEmail?: string;
    onLogout?: () => void;
};

export default function ActusChat({ userEmail, onLogout }: ActusChatProps) {
    type TrendMetric = {
        label: string;
        current: string | number;
        previous: string | number;
        change: number;
        isCurrency?: boolean;
    };

    type RankedItem = {
        rank: number;
        name: string;
        value: string;
    };

    type CreditTrendsData = {
        period: string; // e.g., "Last 30 Days"
        metrics: TrendMetric[];
        topCustomers: RankedItem[];
        topItems: RankedItem[];
        topReps: RankedItem[];
        window?: {
            previous?: string;
            current?: string;
        };
        chartData?: Array<{
            date: string;
            withCr: number;
            withoutCr: number;
            trend: number;
        }>;
    };

    type MixedLinesSummary = {
        mixedTicketCount: number;
        withoutCrCount: number;
        totalUsd: number | null;
        withCrUsd: number | null;
        withoutCrUsd: number | null;
    };

    type NoteSummary = {
        bullets: string[];
        disclaimer?: string;
    };

    type Message = {
        id: string;
        role: 'user' | 'assistant';
        content: string;
        rows?: Record<string, unknown>[];
        creditTrends?: CreditTrendsData; // New field for trends dashboard
        meta?: {
            show_table?: boolean;
            csv_filename?: string;
            csv_rows?: Record<string, unknown>[];
            csv_row_count?: number;
            columns?: string[];
            creditTrends?: CreditTrendsData;
            mixedLinesSummary?: MixedLinesSummary;
            note_summary?: NoteSummary;
            rootCauses?: {
                period?: string;
                data: Array<{
                    root_cause: string;
                    credit_request_total: number;
                    record_count: number;
                }>;
            };
            chart?: {
                kind: 'credit_amount_trend';
                bucket: 'daily' | 'monthly';
                window: string;
                data: Array<{
                    bucket: string;
                    with_cr_usd: number;
                    without_cr_usd: number;
                    total_usd: number;
                    trend_usd: number;
                }>;
            };
        };
    };

    const createId = () => {
        if (typeof crypto !== 'undefined' && 'randomUUID' in crypto) {
            return crypto.randomUUID();
        }
        return `${Date.now()}-${Math.random().toString(16).slice(2)}`;
    };

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
    const messagesEndRef = useRef<HTMLDivElement>(null);
    const lastMessageRef = useRef<HTMLDivElement>(null);
    const scrollContainerRef = useRef<HTMLDivElement>(null);
    const inputRef = useRef<HTMLTextAreaElement>(null);
    const [expandedNoteIds, setExpandedNoteIds] = useState<Record<string, boolean>>({});

    const toggleSidebar = () => setIsSidebarOpen(!isSidebarOpen);
    const toggleNoteExpansion = (noteId: string) => {
        setExpandedNoteIds(prev => ({
            ...prev,
            [noteId]: !prev[noteId],
        }));
    };

    const splitInvestigationNote = (content: string) => {
        const marker = '\n\n**Note:**';
        const idx = content.indexOf(marker);
        if (idx === -1) {
            return { collapsed: content, full: content, hasCollapse: false };
        }
        const header = content.slice(0, idx + marker.length);
        const body = content.slice(idx + marker.length).trim();
        if (!body) {
            return { collapsed: header, full: content, hasCollapse: true };
        }
        const firstChunk = body.split(/\n{2,}/)[0] || '';
        const collapsed = firstChunk ? `${header}\n${firstChunk}` : header;
        return { collapsed, full: content, hasCollapse: true };
    };



    const suggestedIntents = [
        { label: 'Stalled tickets', scope: 'last 60 days' },
        { label: 'Credit operations snapshot', scope: 'last 60 days' },
        { label: 'Credit trends' },
        { label: 'Credit activity', scope: 'last 7 days' },
        { label: 'Mixed lines', scope: 'credit activity - last 7 days' },
    ];

    const [recentIntents, setRecentIntents] = useState<string[]>(() => {
        try {
            const raw = localStorage.getItem('actusRecentIntents');
            return raw ? JSON.parse(raw) : [];
        } catch {
            return [];
        }
    });

    const addRecentIntent = (intent: string) => {
        setRecentIntents(prev => {
            const next = [intent, ...prev].slice(0, 3);
            localStorage.setItem('actusRecentIntents', JSON.stringify(next));
            return next;
        });
    };

    const fallbackUserContext = {
        name: 'Sebastian Rosales',
        firstName: 'Sebastian',
        role: 'Staff Engineer',
        location: 'SF Office',
        lastLogin: '9:12 AM',
    };
    const [userContext, setUserContext] = useState({
        name: '',
        firstName: '',
        role: fallbackUserContext.role,
        location: fallbackUserContext.location,
        lastLogin: fallbackUserContext.lastLogin,
    });
    type RootCausePayload = NonNullable<Message['meta']>['rootCauses'];
    const resolvedUserEmail =
        userEmail ||
        localStorage.getItem('actusUserEmail') ||
        (import.meta.env.VITE_USER_EMAIL as string | undefined) ||
        '';

    const formatLoginTime = (value?: number | string) => {
        if (!value) {
            return fallbackUserContext.lastLogin;
        }
        const numeric = typeof value === 'number' ? value : Number(value);
        if (Number.isFinite(numeric)) {
            return new Intl.DateTimeFormat('en-US', { hour: 'numeric', minute: '2-digit' }).format(
                new Date(numeric)
            );
        }
        return String(value);
    };

    const formatRole = (value?: string) => {
        if (!value) {
            return fallbackUserContext.role;
        }
        return value
            .split(/[\s_-]+/)
            .filter(Boolean)
            .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
            .join(' ');
    };

    const nameFromEmail = (email: string) => {
        const base = email.split('@')[0] || '';
        if (!base) {
            return fallbackUserContext.name;
        }
        return base
            .split(/[._-]+/)
            .filter(Boolean)
            .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
            .join(' ');
    };

    useEffect(() => {
        if (!resolvedUserEmail) {
            return;
        }
        setUserContext((prev) => ({
            ...prev,
            name: '',
            firstName: '',
        }));
        const controller = new AbortController();
        const loadUserContext = async () => {
            try {
                const endpoint = apiBase
                    ? `${apiBase}/api/user-context?email=${encodeURIComponent(resolvedUserEmail)}`
                    : `/api/user-context?email=${encodeURIComponent(resolvedUserEmail)}`;
                const response = await fetch(endpoint, {
                    signal: controller.signal,
                });
                if (!response.ok) {
                    let detail = `user context failed ${response.status}`;
                    try {
                        const errorBody = await response.json() as { detail?: string };
                        if (errorBody?.detail) {
                            detail = errorBody.detail;
                        }
                    } catch {
                        // Best effort: keep default detail.
                    }
                    throw new Error(detail);
                }
                const payload = await response.json() as {
                    email?: string;
                    name?: string;
                    first_name?: string;
                    last_name?: string;
                    firstName?: string;
                    lastName?: string;
                    role?: string;
                    location?: string;
                    last_login?: number | string;
                };
                const inferredName = payload.name
                    || payload.first_name
                    || payload.firstName
                    || [payload.first_name, payload.last_name].filter(Boolean).join(' ')
                    || [payload.firstName, payload.lastName].filter(Boolean).join(' ');
                const resolvedName = inferredName || nameFromEmail(payload.email || resolvedUserEmail);
                setUserContext({
                    name: resolvedName,
                    firstName: payload.firstName
                        || payload.first_name
                        || resolvedName.split(' ')[0]
                        || fallbackUserContext.firstName,
                    role: formatRole(payload.role),
                    location: payload.location || 'Unknown location',
                    lastLogin: formatLoginTime(payload.last_login),
                });
            } catch (error) {
                if (error instanceof DOMException && error.name === 'AbortError') {
                    return;
                }
                console.warn('Failed to load user context', error);
                setUserContext((prev) => ({
                    ...prev,
                    name: '',
                    firstName: '',
                }));
            }
        };
        loadUserContext();
        return () => controller.abort();
    }, [apiBase, resolvedUserEmail]);

    // Default columns
    const tableColumns = [
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

    const resolveColumns = (meta?: { columns?: string[] }) => {
        if (!meta?.columns?.length) {
            // If no columns specified, try to infer from first row keys or fallback to all known
            return tableColumns;
        }
        const mapped = meta.columns.map(key => {
            const existing = tableColumns.find(col => col.key === key);
            return existing || { key, label: key };
        });
        return mapped;
    };

    const formatNumber = (value: unknown) => {
        const num = Number(value);
        if (!Number.isFinite(num)) {
            return String(value);
        }
        return new Intl.NumberFormat('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 }).format(num);
    };

    const formatZScore = (value: unknown) => {
        const num = Number(value);
        if (!Number.isFinite(num)) {
            return String(value);
        }
        return `${num >= 0 ? '+' : ''}${num.toFixed(2)} `;
    };

    const formatCompactCurrency = (value: number) => {
        return new Intl.NumberFormat('en-US', {
            style: 'currency',
            currency: 'USD',
            maximumFractionDigits: 0,
            notation: 'compact',
        }).format(value);
    };

    const formatCurrency = (value: number | null | undefined) => {
        if (value === null || value === undefined || !Number.isFinite(value)) {
            return 'N/A';
        }
        return new Intl.NumberFormat('en-US', {
            style: 'currency',
            currency: 'USD',
            minimumFractionDigits: 2,
            maximumFractionDigits: 2,
        }).format(value);
    };

    const renderChartPlaceholder = (label: string) => (
        <div className="flex items-center justify-center h-full text-sm text-slate-400 bg-slate-950/40 border border-white/5 rounded-lg">
            {label}
        </div>
    );

    const toRootCauseItems = (payload?: RootCausePayload): RootCauseItem[] => {
        if (!payload?.data?.length) {
            return [];
        }
        return payload.data.map((item: RootCausePayload['data'][number], index) => ({
            rank: index + 1,
            rootCause: item.root_cause || 'Unspecified',
            creditRequestTotal: Number(item.credit_request_total || 0),
            recordCount: Number(item.record_count || 0),
        }));
    };

    const renderCreditAmountPlot = (data: Array<any>) => {
        if (!chartLib) {
            const label = chartLibError ? 'Chart library unavailable. Run npm install recharts.' : 'Loading chart...';
            return renderChartPlaceholder(label);
        }
        const { ResponsiveContainer, ComposedChart, Bar, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend } = chartLib;
        return (
            <ResponsiveContainer width="100%" height="100%">
                <ComposedChart data={data}>
                    <CartesianGrid strokeDasharray="3 3" stroke="rgba(148, 163, 184, 0.2)" />
                    <XAxis dataKey="bucket" tick={{ fill: '#94a3b8', fontSize: 11 }} />
                    <YAxis
                        tick={{ fill: '#94a3b8', fontSize: 11 }}
                        tickFormatter={(v: number) => formatCompactCurrency(v)}
                    />
                    <Tooltip
                        cursor={{ fill: 'rgba(255,255,255,0.03)' }}
                        content={({ active, payload, label }: any) => {
                            if (active && payload && payload.length) {
                                return (
                                    <div className="bg-slate-900/90 border border-white/10 rounded-lg p-3 shadow-xl backdrop-blur-md">
                                        <p className="text-xs text-slate-400 mb-2 font-medium">{label}</p>
                                        <div className="space-y-1.5">
                                            {payload.map((entry: any, index: number) => {
                                                const labelText = entry.name === 'with_cr_usd'
                                                    ? 'With CR #'
                                                    : entry.name === 'without_cr_usd'
                                                        ? 'Without CR #'
                                                        : entry.name === 'trend_usd'
                                                            ? '3-Month Trend'
                                                            : 'Total';
                                                const labelColor = '#e2e8f0';
                                                return (
                                                    <div key={index} className="flex items-center gap-2 text-xs">
                                                        <span className="w-2 h-2 rounded-full" style={{ backgroundColor: entry.color }}></span>
                                                        <span style={{ color: labelColor }}>
                                                            {labelText}:
                                                        </span>
                                                        <span className="font-mono font-medium text-white">
                                                            {entry.name === 'trend_usd' ? '' : '$'}
                                                            {Number(entry.value || 0).toLocaleString()}
                                                        </span>
                                                    </div>
                                                );
                                            })}
                                        </div>
                                    </div>
                                );
                            }
                            return null;
                        }}
                    />
                    <Legend
                        formatter={(value: any) => {
                            const label = value === 'with_cr_usd'
                                ? 'With CR #'
                                : value === 'without_cr_usd'
                                    ? 'Without CR #'
                                    : value === 'trend_usd'
                                        ? '3-Month Trend'
                                        : 'Total';
                            const color = value === 'with_cr_usd' ? '#5B7DB1' : '#94a3b8';
                            return <span style={{ color }}>{label}</span>;
                        }}
                    />
                    <Bar dataKey="with_cr_usd" stackId="credits" fill="#0b2a4a" radius={[4, 4, 0, 0]} />
                    <Bar dataKey="without_cr_usd" stackId="credits" fill="#4f6d8a" radius={[4, 4, 0, 0]} />
                    <Line dataKey="trend_usd" stroke="#c1121f" strokeWidth={2.5} dot={{ r: 3 }} />
                </ComposedChart>
            </ResponsiveContainer>
        );
    };

    const INDY_TIMEZONE = 'America/Indiana/Indianapolis';

    const parseDateValue = (value: unknown) => {
        if (value instanceof Date) {
            return Number.isNaN(value.getTime()) ? null : value;
        }
        if (typeof value === 'number') {
            const asDate = new Date(value);
            return Number.isNaN(asDate.getTime()) ? null : asDate;
        }
        if (typeof value === 'string') {
            const asDate = new Date(value);
            return Number.isNaN(asDate.getTime()) ? null : asDate;
        }
        return null;
    };

    const formatIndyDate = (value: unknown) => {
        const asDate = parseDateValue(value);
        if (!asDate) return String(value);
        return new Intl.DateTimeFormat('en-CA', {
            timeZone: INDY_TIMEZONE,
            year: 'numeric',
            month: '2-digit',
            day: '2-digit',
        }).format(asDate);
    };

    const formatIndyDateTime = (value: unknown) => {
        const asDate = parseDateValue(value);
        if (!asDate) return String(value);
        const parts = new Intl.DateTimeFormat('en-CA', {
            timeZone: INDY_TIMEZONE,
            year: 'numeric',
            month: '2-digit',
            day: '2-digit',
            hour: '2-digit',
            minute: '2-digit',
            hour12: false,
        }).formatToParts(asDate);
        const lookup = Object.fromEntries(parts.map(part => [part.type, part.value]));
        return `${lookup.year}-${lookup.month}-${lookup.day} ${lookup.hour}:${lookup.minute}`;
    };

    const normalizeColumnKey = (key: string) => key.trim().toLowerCase().replace(/[\s-]+/g, '_');

    const dateOnlyKeys = new Set(['date']);
    const dateTimeKeys = new Set([
        'last_status_time',
        'last_status',
        'last_updated',
        'last_updated_at',
        'update_timestamp',
        'updated_at',
        'created_at',
    ]);

    const downloadCsv = (rows: Record<string, unknown>[], filename: string, meta?: { columns?: string[] }) => {
        const columns = resolveColumns(meta);
        const header = columns.map(col => col.label).join(',');
        const lines = rows.map(row => columns.map(col => {
            const value = row[col.key];
            if (value === null || value === undefined) {
                return '';
            }
            const text = String(value).replace(/"/g, '""').replace(/\n/g, ' ');
            return `"${text}"`;
        }).join(','));
        const csv = [header, ...lines].join('\n');
        const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' });
        const url = URL.createObjectURL(blob);
        const link = document.createElement('a');
        link.href = url;
        link.download = filename;
        link.click();
        URL.revokeObjectURL(url);
    };

    useEffect(() => {
        const lastMessage = messages[messages.length - 1];
        if (!lastMessage || lastMessage.role !== 'assistant') {
            return;
        }

        const container = scrollContainerRef.current;
        const messageEl = lastMessageRef.current;
        if (!container || !messageEl) {
            return;
        }

        const topOffset = 140;
        requestAnimationFrame(() => {
            const containerRect = container.getBoundingClientRect();
            const messageRect = messageEl.getBoundingClientRect();
            const offset = messageRect.top - containerRect.top;
            const targetTop = container.scrollTop + offset - topOffset;
            container.scrollTo({ top: Math.max(targetTop, 0), behavior: 'smooth' });
        });
    }, [messages]);

    useEffect(() => {
        let mounted = true;
        import(/* @vite-ignore */ 'recharts')
            .then((mod) => {
                if (mounted) {
                    setChartLib(mod);
                    setChartLibError(false);
                }
            })
            .catch(() => {
                if (mounted) {
                    setChartLib(null);
                    setChartLibError(true);
                }
            });
        return () => {
            mounted = false;
        };
    }, []);

    const [pendingFollowup, setPendingFollowup] = useState<{ intent: string; prefix: string } | null>(null);
    const [pendingChoices, setPendingChoices] = useState<Array<{ label: string; prefix: string }> | null>(null);

    const sendMessage = async (messageText: string, options?: { showUser?: boolean }) => {
        const trimmed = messageText.trim();
        if (!trimmed || isTyping) return;
        const shouldUseFollowup = pendingFollowup && !/^cancel|never mind|nevermind$/i.test(trimmed);
        const choiceMatch = pendingChoices && /^\d$/.test(trimmed) ? Number(trimmed) : null;
        const resolvedMessage = choiceMatch && pendingChoices && pendingChoices[choiceMatch - 1]
            ? pendingChoices[choiceMatch - 1].prefix
            : shouldUseFollowup
                ? `${pendingFollowup.prefix} ${trimmed}`
                : trimmed;
        if (shouldUseFollowup) {
            setPendingFollowup(null);
        }
        if (choiceMatch) {
            setPendingChoices(null);
        }

        const showUser = options?.showUser !== false;
        if (showUser) {
            const userMessage: Message = { id: createId(), role: 'user', content: trimmed };
            setMessages(prev => [...prev, userMessage]);
            setInput('');
        }
        setIsTyping(true);

        const t0 = performance.now();
        try {
            const endpoint = apiBase ? `${apiBase}/api/ask` : '/api/ask';
            const response = await fetch(endpoint, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ query: resolvedMessage }),
            });
            const t1 = performance.now();

            if (!response.ok) {
                throw new Error(`Request failed with ${response.status} `);
            }

            const data = await response.json() as {
                text?: string;
                rows?: Record<string, unknown>[];
                meta?: Message['meta'];
            };
            const t2 = performance.now();
            console.info('[ask] fetch=%dms json=%dms total=%dms', Math.round(t1 - t0), Math.round(t2 - t1), Math.round(t2 - t0));

            const assistantMessage: Message = {
                id: createId(),
                role: 'assistant',
                content: data.text || '',
                rows: Array.isArray(data.rows) ? data.rows : undefined,
                meta: data.meta,
            };
            if (data.meta?.follow_up?.intent && data.meta?.follow_up?.prefix) {
                setPendingFollowup({
                    intent: String(data.meta.follow_up.intent),
                    prefix: String(data.meta.follow_up.prefix),
                });
            }
            if (Array.isArray(data.meta?.suggestions)) {
                setPendingChoices(
                    data.meta.suggestions
                        .filter((item: { label?: string; prefix?: string }) => item?.label && item?.prefix)
                        .map((item: { label: string; prefix: string }) => ({
                            label: item.label,
                            prefix: item.prefix,
                        }))
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
                    ]
                };
            }

            setMessages(prev => [...prev, assistantMessage]);
            if (showUser) {
                addRecentIntent(trimmed);
            }
        } catch (error) {
            console.error(error); // Log error but allow UI to continue
            const tErr = performance.now();
            console.info('[ask] failed after %dms', Math.round(tErr - t0));
            // Optional: Show error message in chat if desired, but for now we'll just fail silently or mock if needed.
            // For this demo, let's keep the error feedback
            const message = error instanceof Error ? error.message : 'Unknown error';
            setMessages(prev => [...prev, {
                id: createId(),
                role: 'assistant',
                content: `Request failed: ${message} `,
            }]);
        } finally {
            setIsTyping(false);
        }
    };

    const handleSend = async () => {
        await sendMessage(input, { showUser: true });
    };

    const handleMessageLinkClick = (event: React.MouseEvent) => {
        const target = event.target as HTMLElement | null;
        const anchor = target?.closest('a');
        if (!anchor) return;
        const href = anchor.getAttribute('href') || '';
        if (href.startsWith('actus://ask/')) {
            event.preventDefault();
            const raw = href.replace('actus://ask/', '');
            const decoded = decodeURIComponent(raw);
            sendMessage(decoded, { showUser: false });
        }
    };

    const handleKeyPress = (e: React.KeyboardEvent) => {
        if (e.key === 'Enter' && !e.shiftKey) {
            e.preventDefault();
            handleSend();
        }
    };

    return (
        <div className="flex flex-col h-screen bg-obsidian-950 text-slate-100 overflow-hidden font-sans selection:bg-cyan-500/30">
            {/* Animated background elements */}
            <div className="fixed inset-0 overflow-hidden pointer-events-none">
                <div className="absolute top-[-10%] left-[-10%] w-[50%] h-[50%] bg-cyan-500/05 rounded-full blur-[120px] animate-pulse-slow"></div>
                <div className="absolute bottom-[-10%] right-[-10%] w-[50%] h-[50%] bg-indigo-500/05 rounded-full blur-[120px] animate-pulse-slow" style={{ animationDelay: '2s' }}></div>
                <div className="absolute top-[20%] left-[20%] w-[40%] h-[40%] bg-violet-500/05 rounded-full blur-[100px] animate-float opacity-50"></div>
            </div>

            {/* Header */}
            <header className="fixed top-0 inset-x-0 z-50 glass border-b-0">
                <div className="max-w-[90rem] mx-auto px-6 h-[88px] flex items-center justify-between">
                    <div className="flex items-center gap-4">
                        <div className="relative group">
                            <div className="w-10 h-10 rounded-xl bg-gradient-to-br from-cyan-400 to-indigo-600 flex items-center justify-center shadow-glow group-hover:shadow-glow-lg transition-all duration-300">
                                <Sparkles className="w-5 h-5 text-white" />
                            </div>
                            <div className="absolute -top-1 -right-1 w-2.5 h-2.5 bg-emerald-400 rounded-full border-2 border-obsidian-900 animate-pulse"></div>
                        </div>
                        <div className="flex flex-col">
                            <h1 className="text-xl font-bold font-display tracking-tight bg-gradient-to-r from-white via-slate-200 to-slate-400 bg-clip-text text-transparent drop-shadow-sm">
                                Actus
                            </h1>
                        </div>
                    </div>

                    <div className="flex items-center gap-4">
                        <div className="hidden md:flex items-center gap-2 px-3 py-1.5 rounded-full bg-white/[0.03] border border-white/[0.08] backdrop-blur-sm">
                            <span className="relative flex h-2 w-2">
                                <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-emerald-400 opacity-75"></span>
                                <span className="relative inline-flex rounded-full h-2 w-2 bg-emerald-500"></span>
                            </span>
                            <span className="text-xs font-medium text-slate-400 tracking-wide">SYSTEM ONLINE</span>
                        </div>
                        <div className="flex items-center gap-1 p-1 rounded-full bg-white/[0.04] border border-white/[0.08] backdrop-blur-sm">
                            <button
                                onClick={() => setActiveView('chat')}
                                className={`px-3 py-1.5 text-xs font-semibold rounded-full transition-colors ${activeView === 'chat' ? 'bg-white/10 text-white' : 'text-slate-400 hover:text-white'}`}
                            >
                                Chat
                            </button>
                            <button
                                onClick={() => setActiveView('rag')}
                                className={`px-3 py-1.5 text-xs font-semibold rounded-full transition-colors ${activeView === 'rag' ? 'bg-white/10 text-white' : 'text-slate-400 hover:text-white'}`}
                            >
                                RAG
                            </button>
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
                            onClick={toggleSidebar}
                            className="p-2.5 hover:bg-white/[0.05] rounded-xl transition-all duration-300 text-slate-400 hover:text-white border border-transparent hover:border-white/[0.05] group"
                        >
                            <Menu className="w-5 h-5 group-hover:scale-110 transition-transform" />
                        </button>
                    </div>
                </div>
            </header>

            {/* Sidebar/Drawer */}
            {/* Sidebar/Drawer */}
            <div className={`fixed inset-y-0 right-0 w-80 bg-obsidian-950/90 backdrop-blur-2xl border-l border-white/[0.08] shadow-2xl z-[60] transform transition-transform duration-500 cubic-bezier(0.2, 0.8, 0.2, 1) ${isSidebarOpen ? 'translate-x-0' : 'translate-x-full'} `}>
                <div className="absolute inset-y-0 left-0 w-[1px] bg-gradient-to-b from-transparent via-cyan-500/30 to-transparent"></div>
                <div className="h-full flex flex-col">
                    <div className="h-24 flex items-center justify-between px-6 border-b border-white/[0.06]">
                        <div className="flex flex-col gap-0.5">
                            <h2 className="text-lg font-bold font-display tracking-tight text-transparent bg-clip-text bg-gradient-to-r from-white to-slate-400">Settings & History</h2>
                            <span className="text-[10px] uppercase tracking-widest text-cyan-500/80 font-semibold">User Controls</span>
                        </div>
                        <button onClick={toggleSidebar} className="p-2 -mr-2 text-slate-400 hover:text-white hover:bg-white/5 rounded-lg transition-colors group">
                            <X className="w-5 h-5 group-hover:rotate-90 transition-transform duration-300" />
                        </button>
                    </div>

                    <div className="flex-1 overflow-y-auto p-4 space-y-8">
                        <div>
                            <div className="flex items-center gap-2 mb-4 px-2">
                                <div className="w-1 h-1 rounded-full bg-cyan-400"></div>
                                <h3 className="text-[10px] font-bold text-slate-500 uppercase tracking-widest">Suggested Actions</h3>
                            </div>
                            <div className="space-y-1">
                                {suggestedIntents.map((intent) => (
                                    <button
                                        key={intent.label}
                                        onClick={() => sendMessage(intent.scope ? `${intent.label} (${intent.scope})` : intent.label, { showUser: true })}
                                        className="w-full text-left p-3.5 rounded-xl hover:bg-gradient-to-r hover:from-white/[0.07] hover:to-transparent border border-transparent hover:border-white/[0.05] transition-all group flex items-start gap-3"
                                    >
                                        <Sparkles className="w-4 h-4 text-slate-600 group-hover:text-cyan-400 transition-colors mt-0.5" />
                                        <span className="flex flex-col">
                                            <span className="text-sm text-slate-300 group-hover:text-white transition-colors font-medium">
                                                {intent.label}
                                            </span>
                                            {intent.scope && (
                                                <span className="text-[10px] text-slate-500 group-hover:text-cyan-200/50 mt-0.5">{intent.scope}</span>
                                            )}
                                        </span>
                                    </button>
                                ))}
                            </div>
                        </div>

                        <div>
                            <div className="flex items-center gap-2 mb-4 px-2">
                                <div className="w-1 h-1 rounded-full bg-indigo-400"></div>
                                <h3 className="text-[10px] font-bold text-slate-500 uppercase tracking-widest">Recent Chats</h3>
                            </div>
                            <div className="space-y-1">
                                {recentIntents.length === 0 ? (
                                    <div className="px-3 py-8 text-center border border-dashed border-white/5 rounded-xl bg-white/[0.02]">
                                        <p className="text-xs text-slate-500">No chat history yet.</p>
                                    </div>
                                ) : (
                                    recentIntents.map((intent, index) => (
                                        <button
                                            key={`${intent}-${index}`}
                                            onClick={() => sendMessage(intent, { showUser: true })}
                                            className="w-full text-left p-3.5 rounded-xl hover:bg-gradient-to-r hover:from-white/[0.07] hover:to-transparent border border-transparent hover:border-white/[0.05] transition-all group flex items-center gap-3"
                                        >
                                            <div className="w-1.5 h-1.5 rounded-full bg-slate-700 group-hover:bg-indigo-400 transition-colors"></div>
                                            <span className="text-sm text-slate-400 group-hover:text-slate-200 transition-colors truncate">
                                                {intent}
                                            </span>
                                        </button>
                                    ))
                                )}
                            </div>
                        </div>
                    </div>

                    <div className="p-4 bg-gradient-to-t from-black/40 to-transparent">
                        <div className="p-4 rounded-2xl bg-white/[0.03] border border-white/[0.06] backdrop-blur-sm flex items-center gap-4 group hover:bg-white/[0.05] transition-colors cursor-default">
                            <div className="w-10 h-10 rounded-full bg-gradient-to-br from-cyan-500 to-blue-600 flex items-center justify-center text-sm font-bold text-white shadow-lg shadow-cyan-900/20">
                                {userContext.firstName ? userContext.firstName.charAt(0) : 'U'}
                            </div>
                            <div className="flex-1 min-w-0">
                                <div className="text-sm text-slate-200 font-medium truncate group-hover:text-white transition-colors">
                                    {userContext.name || 'User'}
                                </div>
                                <div className="text-xs text-slate-500 flex items-center gap-1.5 mt-0.5">
                                    <div className="w-1.5 h-1.5 rounded-full bg-emerald-500"></div>
                                    Online • {userContext.lastLogin}
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            </div>

            {/* Overlay for Sidebar */}
            {isSidebarOpen && (
                <div
                    className="fixed inset-0 bg-black/50 backdrop-blur-sm z-[55] transition-opacity duration-300"
                    onClick={toggleSidebar}
                ></div>
            )}

            {/* Messages */}
            <div ref={scrollContainerRef} className="flex-1 overflow-y-auto pt-[120px] pb-40 scroll-smooth">
                {/* WIDER CONTAINER: max-w-6xl */}
                {activeView === 'rag' ? (
                    <div className="max-w-6xl mx-auto px-6 pb-16">
                        <RagResults />
                    </div>
                ) : (
                    <div className="max-w-6xl mx-auto px-6 space-y-8 flex flex-col min-h-full">

                        {/* Hero Empty State */}
                        {messages.length === 0 && (
                            <div className="flex-1 flex flex-col items-center justify-center -mt-20 animate-fade-in z-10">
                                <div className="relative group mb-10">
                                    <div className="absolute -inset-1 bg-gradient-to-r from-cyan-500 to-indigo-600 rounded-full blur opacity-40 group-hover:opacity-75 transition duration-1000 group-hover:duration-200"></div>
                                    <div className="w-24 h-24 rounded-3xl bg-obsidian-900 border border-white/10 flex items-center justify-center shadow-2xl relative z-10 group-hover:scale-105 transition-transform duration-500">
                                        <Sparkles className="w-12 h-12 text-cyan-400 group-hover:text-white transition-colors duration-500" />
                                    </div>
                                    <div className="absolute -top-2 -right-2 w-6 h-6 bg-emerald-500 rounded-full border-4 border-obsidian-950 animate-pulse z-20"></div>
                                </div>

                                {(() => {
                                    const displayName = userContext.firstName || userContext.name;
                                    const timeGreeting = (() => {
                                        const now = new Date();
                                        const indyTime = new Date(now.toLocaleString('en-US', { timeZone: 'America/Indiana/Indianapolis' }));
                                        const hour = indyTime.getHours();
                                        if (hour < 5 || hour >= 18) return 'Good evening';
                                        if (hour < 12) return 'Good morning';
                                        return 'Good afternoon';
                                    })();

                                    return (
                                        <h1 className="text-5xl md:text-6xl font-bold font-display tracking-tight text-center mb-6 flex flex-col md:block items-center justify-center gap-2">
                                            <span className="bg-gradient-to-r from-white via-slate-200 to-slate-500 bg-clip-text text-transparent drop-shadow-sm">
                                                {timeGreeting}
                                            </span>
                                            {displayName ? (
                                                <span
                                                    key={displayName}
                                                    className="inline-block md:ml-3 bg-gradient-to-r from-cyan-200 via-cyan-400 to-indigo-400 bg-clip-text text-transparent opacity-0 animate-[fadeInUp_0.8s_ease-out_0.5s_forwards]"
                                                >
                                                    , {displayName}
                                                </span>
                                            ) : (
                                                <span className="inline-flex items-center ml-3 align-middle">
                                                    <span className="h-4 w-24 rounded-full bg-white/5 animate-pulse"></span>
                                                </span>
                                            )}
                                        </h1>
                                    );
                                })()}
                                <p className="text-slate-400 text-lg text-center mb-12 max-w-lg leading-relaxed font-light">
                                    Actus is ready to analyze real-time data trends and anomalies.
                                </p>

                                <div className="grid grid-cols-1 md:grid-cols-3 gap-5 w-full max-w-3xl">
                                    {[
                                        { label: 'Check Credit Trends', cmd: 'Show me the credit trends', icon: <TrendingUp className="w-5 h-5 text-cyan-400" />, desc: 'Analyze latest movements' },
                                        { label: 'Priority Tickets', cmd: 'Show priority tickets', icon: <Zap className="w-5 h-5 text-amber-400" />, desc: 'View urgent items' },
                                        { label: 'Help Functions', cmd: 'What can you do?', icon: <MessageSquare className="w-5 h-5 text-purple-400" />, desc: 'Explore capabilities' },
                                    ].map((item, idx) => (
                                        <button
                                            key={idx}
                                            onClick={() => sendMessage(item.cmd, { showUser: true })}
                                            className="flex flex-col items-start gap-4 p-5 rounded-2xl bg-white/[0.03] border border-white/[0.06] hover:bg-white/[0.08] hover:border-cyan-500/30 transition-all duration-300 group/card text-left relative overflow-hidden"
                                        >
                                            <div className="absolute top-0 right-0 p-4 opacity-0 group-hover/card:opacity-100 transition-opacity transform translate-x-2 group-hover/card:translate-x-0">
                                                <div className="w-16 h-16 bg-gradient-to-bl from-cyan-500/10 to-transparent rounded-full blur-xl"></div>
                                            </div>
                                            <div className="p-2.5 rounded-xl bg-obsidian-950 border border-white/10 group-hover/card:border-cyan-500/20 group-hover/card:shadow-glow-sm transition-all duration-300">
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

                        {messages.map((message, idx) => {
                            const noteSummary = message.meta?.note_summary;
                            const isNoteExpanded = expandedNoteIds[message.id] ?? false;
                            const noteContent = message.content ?? '';
                            const noteSplit = splitInvestigationNote(noteContent);
                            const shouldCollapseNote = Boolean(noteSummary && noteSplit.hasCollapse);
                            const noteDisplayContent = shouldCollapseNote && !isNoteExpanded ? noteSplit.collapsed : noteSplit.full;

                            return (
                                <div
                                    key={message.id}
                                    ref={idx === messages.length - 1 ? lastMessageRef : undefined}
                                    className={`flex gap-4 animate-fade-in-up group ${message.role === 'user' ? 'justify-end' : 'justify-start'} `}
                                >
                                {message.role === 'assistant' && (
                                    <div className="w-8 h-8 rounded-lg bg-gradient-to-br from-cyan-500 to-indigo-600 flex items-center justify-center flex-shrink-0 shadow-lg shadow-cyan-500/10 mt-1">
                                        <Sparkles className="w-4 h-4 text-white" />
                                    </div>
                                )}

                                <div className={`max-w-[90%] flex flex-col gap-4 ${message.role === 'user' ? 'items-end' : 'items-start'} `}>
                                    {/* INVESTIGATION NOTE SUMMARY */}
                                    {message.role === 'assistant' && noteSummary && (
                                        <div className="w-full max-w-3xl bg-emerald-500/10 border border-emerald-400/20 rounded-2xl overflow-hidden shadow-2xl shadow-emerald-500/10 backdrop-blur-md p-5">
                                            <div className="flex items-start justify-between gap-4">
                                                <div>
                                                    <div className="text-xs uppercase tracking-[0.2em] text-emerald-200/80 font-semibold">
                                                        Summary (suggested)
                                                    </div>
                                                    <div className="text-[11px] text-emerald-200/60 mt-1">
                                                        {noteSummary.disclaimer || 'Generated by LLM'}
                                                    </div>
                                                </div>
                                                <span className="text-[10px] font-semibold uppercase tracking-[0.2em] text-emerald-300/60">
                                                    Snapshot
                                                </span>
                                            </div>
                                            <ul className="mt-3 space-y-2 text-sm text-emerald-100">
                                                {noteSummary.bullets.map((bullet, index) => (
                                                    <li key={`${message.id}-summary-${index}`} className="flex items-start gap-2">
                                                        <span className="mt-1 w-1.5 h-1.5 rounded-full bg-emerald-300 flex-shrink-0" />
                                                        <span>{bullet}</span>
                                                    </li>
                                                ))}
                                            </ul>
                                        </div>
                                    )}

                                    {/* TEXT CONTENT */}
                                    {!(message.role === 'assistant' && (message.creditTrends || message.meta?.rootCauses)) && Boolean(message.content?.trim()) && (
                                        <div
                                            className={`p-5 rounded-2xl backdrop-blur-md transition-all duration-300 shadow-xl ${message.role === 'user'
                                                ? 'bg-gradient-to-br from-cyan-600 to-blue-600 text-white shadow-cyan-900/20 rounded-tr-sm border border-white/10'
                                                : 'glass-panel text-slate-200 rounded-tl-sm'
                                                } `}
                                        >
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
                                                            h1: ({ children }) => <h1 className="text-xl font-bold text-white mb-3 mt-1 pb-2 border-b border-white/10 font-display">{children}</h1>,
                                                            a: ({ href, children }) => {
                                                                if (href?.startsWith('actus://ask/')) {
                                                                    const raw = href.replace('actus://ask/', '');
                                                                    const decoded = decodeURIComponent(raw);
                                                                    return (
                                                                        <a
                                                                            href="#"
                                                                            role="button"
                                                                            onClick={(event) => {
                                                                                event.preventDefault();
                                                                                sendMessage(decoded, { showUser: false });
                                                                            }}
                                                                            className="inline-flex items-center gap-1.5 px-2 py-0.5 rounded bg-cyan-500/10 text-cyan-200 border border-cyan-500/20 hover:bg-cyan-500/20 transition-colors"
                                                                            title="Run this request"
                                                                        >
                                                                            {children}
                                                                        </a>
                                                                    );
                                                                }
                                                                return (
                                                                    <a href={href} className="text-cyan-400 hover:text-cyan-300 underline decoration-cyan-400/30 underline-offset-2 hover:decoration-cyan-300" target="_blank" rel="noreferrer">
                                                                        {children}
                                                                    </a>
                                                                );
                                                            },
                                                            code: ({ children }) => {
                                                                const text = Array.isArray(children) ? children.join('') : String(children);
                                                                if (text.startsWith('ask:')) {
                                                                    const payload = text.slice(4);
                                                                    const divider = payload.lastIndexOf('|');
                                                                    const encoded = divider === -1 ? payload : payload.slice(0, divider);
                                                                    const label = divider === -1 ? 'Open' : payload.slice(divider + 1);
                                                                    const command = decodeURIComponent(encoded);
                                                                    return (
                                                                        <button
                                                                            type="button"
                                                                            onClick={() => sendMessage(command.trim(), { showUser: false })}
                                                                            className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-md bg-cyan-500/10 text-cyan-200 border border-cyan-500/20 hover:bg-cyan-500/20 transition-all text-xs font-semibold uppercase tracking-wide"
                                                                            title="Open note"
                                                                        >
                                                                            {label?.trim() || 'Open'}
                                                                        </button>
                                                                    );
                                                                }
                                                                return (
                                                                    <code className="bg-obsidian-950/50 rounded px-1.5 py-0.5 text-sm font-mono text-cyan-300 border border-white/5">
                                                                        {children}
                                                                    </code>
                                                                );
                                                            },
                                                        }}
                                                    >
                                                        {noteDisplayContent}
                                                    </ReactMarkdown>
                                                )}
                                            </div>
                                        </div>
                                    )}
                                    {message.role === 'assistant' && shouldCollapseNote && (
                                        <button
                                            type="button"
                                            onClick={() => toggleNoteExpansion(message.id)}
                                            className="text-xs font-semibold uppercase tracking-[0.2em] text-cyan-200/80 hover:text-cyan-100 transition-colors"
                                        >
                                            {isNoteExpanded ? 'Hide full note' : 'View full note'}
                                        </button>
                                    )}

                                    {/* CREDIT TRENDS DASHBOARD */}
                                    {message.role === 'assistant' && message.creditTrends && (
                                        <Mockup
                                            creditTrends={message.creditTrends}
                                        />
                                    )}

                                    {/* CREDIT AMOUNT CHART */}
                                    {message.role === 'assistant' && message.meta?.chart?.kind === 'credit_amount_trend' && (
                                        <div className="w-full max-w-6xl bg-slate-900/50 border border-white/10 rounded-2xl overflow-hidden shadow-2xl backdrop-blur-md p-6">
                                            <div className="flex items-center gap-2 mb-4">
                                                <BarChart className="w-5 h-5 text-cyan-400" />
                                                <h2 className="text-lg font-bold text-white">Credit Amount Trend</h2>
                                            </div>
                                            <div className="text-xs text-slate-400 mb-4">
                                                Window: {message.meta.chart.window} • Bucketing: {message.meta.chart.bucket}
                                            </div>
                                            <div className="h-[440px] min-w-[900px] w-full overflow-x-auto">
                                                {renderCreditAmountPlot(message.meta.chart.data)}
                                            </div>
                                        </div>
                                    )}

                                    {/* MIXED LINES SUMMARY CARD */}
                                    {message.role === 'assistant' && message.meta?.mixedLinesSummary && (
                                        <div className="w-full max-w-4xl bg-slate-900/55 border border-white/10 rounded-2xl overflow-hidden shadow-2xl shadow-slate-900/40 backdrop-blur-md p-6">
                                            <div className="flex items-center gap-2 mb-5">
                                                <BarChart className="w-5 h-5 text-emerald-400" />
                                                <h2 className="text-lg font-bold text-white">Mixed Lines Summary</h2>
                                            </div>
                                            <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-4">
                                                <div className="bg-white/5 border border-white/10 rounded-xl p-4">
                                                    <div className="text-xs text-slate-400 uppercase tracking-wider font-semibold">Mixed Tickets</div>
                                                    <div className="mt-2 text-2xl font-bold text-white font-display">
                                                        {message.meta.mixedLinesSummary.mixedTicketCount.toLocaleString()}
                                                    </div>
                                                </div>
                                                <div className="bg-white/5 border border-white/10 rounded-xl p-4">
                                                    <div className="text-xs text-slate-400 uppercase tracking-wider font-semibold">Without CR Count</div>
                                                    <div className="mt-2 text-2xl font-bold text-white font-display">
                                                        {message.meta.mixedLinesSummary.withoutCrCount.toLocaleString()}
                                                    </div>
                                                </div>
                                                <div className="bg-white/5 border border-white/10 rounded-xl p-4">
                                                    <div className="text-xs text-slate-400 uppercase tracking-wider font-semibold">Total Credits</div>
                                                    <div className="mt-2 text-2xl font-bold text-emerald-400 font-display">
                                                        {formatCurrency(message.meta.mixedLinesSummary.totalUsd)}
                                                    </div>
                                                </div>
                                            </div>
                                            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                                                <div className="bg-white/5 border border-white/10 rounded-xl p-4">
                                                    <div className="text-xs text-slate-400 uppercase tracking-wider font-semibold">With CR Total</div>
                                                    <div className="mt-2 text-xl font-bold text-cyan-300 font-display">
                                                        {formatCurrency(message.meta.mixedLinesSummary.withCrUsd)}
                                                    </div>
                                                </div>
                                                <div className="bg-white/5 border border-white/10 rounded-xl p-4">
                                                    <div className="text-xs text-slate-400 uppercase tracking-wider font-semibold">Without CR Total</div>
                                                    <div className="mt-2 text-xl font-bold text-amber-300 font-display">
                                                        {formatCurrency(message.meta.mixedLinesSummary.withoutCrUsd)}
                                                    </div>
                                                </div>
                                            </div>
                                        </div>
                                    )}

                                    {/* ROOT CAUSES DASHBOARD */}
                                    {message.role === 'assistant' && message.meta?.rootCauses && (
                                        <RootCauses
                                            data={toRootCauseItems(message.meta.rootCauses)}
                                            period={message.meta.rootCauses.period}
                                        />
                                    )}

                                    {/* DATA CARD (Table + Actions) */}
                                    {message.role === 'assistant' && Array.isArray(message.rows) && message.rows.length > 0 && message.meta?.show_table !== false && (
                                        <div className="w-full max-w-4xl bg-slate-900/60 border border-white/10 rounded-2xl overflow-hidden shadow-2xl shadow-slate-900/40 backdrop-blur-md">
                                            {/* Header Bar */}
                                            <div className="px-5 py-3.5 border-b border-white/10 flex items-center justify-between bg-slate-900/70">
                                                <div className="flex items-center gap-2 text-sm font-semibold text-slate-100">
                                                    <TableIcon className="w-4 h-4 text-cyan-400" />
                                                    <span>Data Preview</span>
                                                    <span className="text-slate-500 font-normal ml-2">({message.rows.length} rows)</span>
                                                </div>

                                                {message.meta?.csv_filename && (
                                                    <button
                                                        onClick={() => downloadCsv(
                                                            Array.isArray(message.meta?.csv_rows) && message.meta.csv_rows.length > 0
                                                                ? message.meta.csv_rows
                                                                : (message.rows || []),
                                                            message.meta?.csv_filename || 'data.csv',
                                                            message.meta
                                                        )}
                                                        className="flex items-center gap-2 px-3 py-1.5 rounded-lg bg-cyan-500/10 hover:bg-cyan-500/20 border border-cyan-500/20 text-xs font-medium text-cyan-300 transition-colors"
                                                    >
                                                        <Download className="w-3.5 h-3.5" />
                                                        Download CSV{typeof message.meta?.csv_row_count === 'number' ? ` (${message.meta.csv_row_count} rows)` : ''}
                                                    </button>
                                                )}
                                            </div>

                                            {/* Scrollable Table */}
                                            <div className="overflow-x-auto overflow-y-auto max-h-[520px] max-w-full overscroll-x-contain">
                                                <table className="min-w-[1100px] w-max divide-y divide-white/10">
                                                    <thead className="bg-slate-950/80 sticky top-0 z-10">
                                                        <tr>
                                                            {resolveColumns(message.meta).map(col => (
                                                                <th key={col.key} className="px-4 py-3 text-left text-[11px] font-semibold text-slate-400 uppercase tracking-[0.12em] whitespace-nowrap">
                                                                    {col.label}
                                                                </th>
                                                            ))}
                                                        </tr>
                                                    </thead>
                                                    <tbody className="divide-y divide-white/5 bg-transparent">
                                                        {message.rows.map((row: Record<string, unknown>, rowIdx: number) => (
                                                            <tr key={rowIdx} className="hover:bg-white/5 transition-colors group/row">
                                                                {resolveColumns(message.meta).map(col => {
                                                                    const value = row[col.key];
                                                                    const normalizedKey = normalizeColumnKey(col.key);
                                                                    let content;

                                                                    if (value === null || value === undefined || value === '') {
                                                                        content = <span className="text-slate-600 italic">N/A</span>;
                                                                    } else if (dateOnlyKeys.has(normalizedKey)) {
                                                                        content = <span className="font-mono">{formatIndyDate(value)}</span>;
                                                                    } else if (dateTimeKeys.has(normalizedKey)) {
                                                                        content = <span className="font-mono">{formatIndyDateTime(value)}</span>;
                                                                    } else if (col.key === 'Amount' || col.key === 'Credit Request Total') {
                                                                        // Simple heuristic for currency styling
                                                                        const txt = formatNumber(value);
                                                                        content = <span className="text-emerald-400 font-medium font-mono">{txt}</span>;
                                                                    } else if (col.key === 'Z Score' || col.key === 'z_score') {
                                                                        const txt = formatZScore(value);
                                                                        const tone = String(value).startsWith('-') ? 'text-cyan-400' : 'text-rose-400';
                                                                        content = <span className={`font-mono ${tone} `}>{txt}</span>;
                                                                    } else if (col.key === 'Anomaly Flag' || col.key === 'anomaly_reason') {
                                                                        const flag = String(value).toLowerCase();
                                                                        let badge = 'bg-slate-500/20 text-slate-300 border-slate-500/30';
                                                                        if (flag.includes('hard')) {
                                                                            badge = 'bg-rose-500/20 text-rose-300 border-rose-500/30';
                                                                        } else if (flag.includes('stat')) {
                                                                            badge = 'bg-amber-500/20 text-amber-300 border-amber-500/30';
                                                                        } else if (flag.includes('both')) {
                                                                            badge = 'bg-purple-500/20 text-purple-300 border-purple-500/30';
                                                                        }
                                                                        content = (
                                                                            <span className={`inline-flex items-center px-2 py-0.5 rounded text-xs font-medium border ${badge} `}>
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
                                                                        content = String(value);
                                                                    }

                                                                    return (
                                                                        <td key={col.key} className="px-4 py-2.5 text-sm text-slate-300 whitespace-nowrap">
                                                                            {content}
                                                                        </td>
                                                                    );
                                                                })}
                                                            </tr>
                                                        ))}
                                                    </tbody>
                                                </table>
                                            </div>
                                        </div>
                                    )}
                                </div>

                                {
                                    message.role === 'user' && (
                                        <div className="w-8 h-8 rounded-lg bg-slate-800 border border-white/10 flex items-center justify-center flex-shrink-0 mt-1">
                                            <div className="w-4 h-4 bg-gradient-to-br from-slate-400 to-slate-600 rounded-sm"></div>
                                        </div>
                                    )
                                }
                                </div>
                            );
                        })}
                        {isTyping && (
                            <div className="flex gap-4 animate-fade-in-up">
                                <div className="w-8 h-8 rounded-lg bg-gradient-to-br from-cyan-500 to-indigo-600 flex items-center justify-center flex-shrink-0 shadow-lg shadow-cyan-500/10 mt-1">
                                    <Sparkles className="w-4 h-4 text-white animate-pulse" />
                                </div>
                                <div className="bg-slate-800/40 border border-white/5 rounded-2xl rounded-tl-sm p-4 shadow-lg flex items-center gap-2">
                                    <div className="w-2 h-2 rounded-full bg-cyan-400 animate-bounce" style={{ animationDelay: '0ms' }}></div>
                                    <div className="w-2 h-2 rounded-full bg-cyan-400 animate-bounce" style={{ animationDelay: '150ms' }}></div>
                                    <div className="w-2 h-2 rounded-full bg-cyan-400 animate-bounce" style={{ animationDelay: '300ms' }}></div>
                                </div>
                            </div>
                        )}
                        <div ref={messagesEndRef} className="h-4" />
                    </div>
                )}
            </div>

            {/* Input Area */}
            {activeView === 'chat' && (
                <div className="fixed bottom-0 inset-x-0 bg-transparent p-6 z-50">
                    <div className={`max-w-4xl mx-auto relative transition-all duration-300 ease-out ${isSidebarOpen ? 'mr-[340px]' : 'mx-auto'} `}>
                        <div className="relative group perspective-[1000px]">
                            <div className="absolute -inset-0.5 bg-gradient-to-r from-cyan-500/30 via-blue-500/30 to-purple-500/30 rounded-full blur-xl opacity-0 group-hover:opacity-100 transition duration-700 group-hover:duration-200"></div>
                            <div className="relative flex items-center bg-slate-900/40 backdrop-blur-2xl border border-white/10 rounded-full shadow-2xl overflow-hidden focus-within:ring-2 focus-within:ring-cyan-500/20 focus-within:border-cyan-500/40 transition-all duration-300 transform group-hover:translate-y-[-2px]">
                                <textarea
                                    ref={inputRef}
                                    value={input}
                                    onChange={(e) => setInput(e.target.value)}
                                    onKeyDown={handleKeyPress}
                                    placeholder="Ask Actus anything..."
                                    className="w-full bg-transparent text-white placeholder-slate-400 px-6 py-4 focus:outline-none resize-none h-[60px] leading-[28px] text-base"
                                    rows={1}
                                />
                                <div className="flex items-center gap-2 pr-3">
                                    <button
                                        onClick={handleSend}
                                        disabled={!input.trim() || isTyping}
                                        className="p-2.5 bg-gradient-to-r from-cyan-500 to-blue-600 hover:from-cyan-400 hover:to-blue-500 text-white rounded-xl transition-all disabled:opacity-50 disabled:cursor-not-allowed shadow-glow hover:shadow-glow-lg active:scale-95"
                                    >
                                        <Send className="w-5 h-5" />
                                    </button>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            )}

        </div>
    );
}
