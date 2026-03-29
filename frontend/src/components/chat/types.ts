import type { TicketAnalysisMeta, ItemAnalysisMeta, CustomerAnalysisMeta } from '../Analysis';

export type AskMode = 'manual' | 'auto';

export type TrendMetric = {
    label: string;
    current: string | number;
    previous: string | number;
    change: number;
    isCurrency?: boolean;
};

export type RankedItem = {
    rank: number;
    name: string;
    value: string;
};

export type CreditTrendsData = {
    period: string;
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

export type MixedLinesSummary = {
    mixedTicketCount: number;
    withoutCrCount: number;
    totalUsd: number | null;
    withCrUsd: number | null;
    withoutCrUsd: number | null;
};

export type NoteSummary = {
    bullets: string[];
    disclaimer?: string;
    source?: string;
    model?: string;
};

export type SystemUpdatesSummary = {
    total_records: number;
    total_update_dates: number;
    recent_limit: number;
    batches: Array<{
        date: string;
        count: number;
        credit_total: number;
        credit_total_display: string;
    }>;
};

export type MessageMeta = {
    intent_id?: string;
    intent?: string;
    follow_up?: {
        intent?: string;
        prefix?: string;
    };
    suggestions?: Array<{
        id?: string;
        label?: string;
        prefix?: string;
        confidence?: number;
    }>;
    show_table?: boolean;
    anomaly_scan?: boolean;
    csv_filename?: string;
    csv_rows?: Record<string, unknown>[];
    csv_row_count?: number;
    columns?: string[];
    creditTrends?: CreditTrendsData;
    mixedLinesSummary?: MixedLinesSummary;
    note_summary?: NoteSummary;
    system_updates_summary?: SystemUpdatesSummary;
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
    ticket_analysis?: TicketAnalysisMeta;
    item_analysis?: ItemAnalysisMeta;
    customer_analysis?: CustomerAnalysisMeta;
    auto_mode?: {
        enabled?: boolean;
        planner?: string;
        primary_intent?: string;
        subintent_count?: number;
        executed_intents?: Array<{
            id?: string;
            label?: string;
            status?: 'ok' | 'error';
        }>;
    };
};

export type Message = {
    id: string;
    role: 'user' | 'assistant';
    content: string;
    rows?: Record<string, unknown>[];
    creditTrends?: CreditTrendsData;
    isError?: boolean;
    originalQuery?: string;
    timestamp?: number;
    meta?: MessageMeta;
};

export type RootCausePayload = NonNullable<NonNullable<Message['meta']>['rootCauses']>;

export type UserContext = {
    name: string;
    firstName: string;
    role: string;
    location: string;
    lastLogin: string;
};

export type ContextualSuggestion = {
    label: string;
    query: string;
};

export function getContextualSuggestions(meta: MessageMeta | undefined): ContextualSuggestion[] {
    if (!meta) return [];

    // Server suggestions take priority — they're already shown as chips inline,
    // so show them in sidebar as quick-access shortcuts too
    if (meta.suggestions?.length) {
        return meta.suggestions
            .filter((s): s is { label: string; prefix: string } => Boolean(s?.label && s?.prefix))
            .slice(0, 4)
            .map(s => ({ label: s.label, query: s.prefix }));
    }

    const suggestions: ContextualSuggestion[] = [];

    if (meta.ticket_analysis) {
        const topCause = meta.ticket_analysis.primary_root_cause;
        if (topCause) {
            suggestions.push({ label: `More on: ${topCause}`, query: `credit activity for ${topCause}` });
        }
        suggestions.push({ label: 'Root cause analysis', query: 'root cause analysis last 60 days' });
    }

    if (meta.customer_analysis) {
        suggestions.push({ label: 'Credit trends', query: 'credit trends' });
        const firstTicket = meta.customer_analysis.tickets?.[0];
        if (firstTicket) {
            suggestions.push({ label: `Top ticket: ${firstTicket}`, query: `ticket status ${firstTicket}` });
        }
    }

    if (meta.item_analysis) {
        suggestions.push({ label: 'Credit trends', query: 'credit trends' });
        suggestions.push({ label: 'Root cause analysis', query: 'root cause analysis last 60 days' });
    }

    if (meta.rootCauses?.data?.[0]) {
        const topCause = meta.rootCauses.data[0].root_cause;
        suggestions.push({ label: `Tickets: ${topCause}`, query: `credit activity for ${topCause}` });
        suggestions.push({ label: 'Credit trends', query: 'credit trends' });
    }

    if (meta.anomaly_scan) {
        suggestions.push({ label: 'Credit trends', query: 'credit trends' });
        suggestions.push({ label: 'Root cause analysis', query: 'root cause analysis' });
    }

    // De-duplicate by label
    const seen = new Set<string>();
    return suggestions.filter(s => {
        if (seen.has(s.label)) return false;
        seen.add(s.label);
        return true;
    }).slice(0, 4);
}
