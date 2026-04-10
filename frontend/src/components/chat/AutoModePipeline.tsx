import { CheckCircle2, AlertCircle, Loader2, Zap, GitBranch } from 'lucide-react';
import { useState, useEffect } from 'react';

type ExecutedIntent = {
    id?: string;
    label?: string;
    status?: 'ok' | 'error';
};

type AutoModePipelineProps = {
    executedIntents: ExecutedIntent[];
    isActive?: boolean;
    primaryIntent?: string;
    planner?: string;
    subintentCount?: number;
};

export function AutoModePipeline({ executedIntents, isActive, primaryIntent, planner, subintentCount }: AutoModePipelineProps) {
    const total = subintentCount ?? executedIntents.length;
    const done = executedIntents.length;
    const allOk = executedIntents.every(i => i.status !== 'error');

    return (
        <div className="w-full max-w-4xl bg-obsidian-950/60 border border-cyan-500/15 rounded-2xl overflow-hidden shadow-lg backdrop-blur-xl">
            {/* Header row */}
            <div className="px-5 py-3.5 border-b border-white/[0.04] flex items-center justify-between gap-4 bg-gradient-to-r from-cyan-500/[0.05] to-transparent">
                <div className="flex items-center gap-2.5">
                    <div className="w-7 h-7 rounded-lg bg-cyan-500/10 border border-cyan-500/20 flex items-center justify-center">
                        <GitBranch className="w-3.5 h-3.5 text-cyan-400" />
                    </div>
                    <div>
                        <span className="text-[10px] font-bold uppercase tracking-[0.22em] text-cyan-400">
                            Auto · Orchestrated
                        </span>
                        {primaryIntent && (
                            <span className="ml-2 text-[10px] text-slate-500 font-mono">{primaryIntent}</span>
                        )}
                    </div>
                </div>

                <div className="flex items-center gap-2">
                    {isActive ? (
                        <span className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full bg-cyan-500/10 border border-cyan-500/20 text-[10px] font-semibold text-cyan-300">
                            <Loader2 className="w-3 h-3 animate-spin" />
                            Orchestrating…
                        </span>
                    ) : allOk ? (
                        <span className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full bg-emerald-500/10 border border-emerald-500/20 text-[10px] font-semibold text-emerald-300">
                            <CheckCircle2 className="w-3 h-3" />
                            {done} completed
                        </span>
                    ) : (
                        <span className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full bg-rose-500/10 border border-rose-500/20 text-[10px] font-semibold text-rose-300">
                            <AlertCircle className="w-3 h-3" />
                            {done - executedIntents.filter(i => i.status === 'error').length}/{done} ok
                        </span>
                    )}
                </div>
            </div>

            {/* Intent steps */}
            <div className="px-5 py-3 flex flex-wrap gap-2">
                {executedIntents.map((item, idx) => {
                    const isOk = item.status !== 'error';
                    return (
                        <div
                            key={`${item.id || idx}`}
                            className={`flex items-center gap-2 px-3 py-2 rounded-xl border text-xs font-medium transition-all
                                ${isOk
                                    ? 'bg-emerald-500/[0.07] border-emerald-500/20 text-emerald-200'
                                    : 'bg-rose-500/[0.07] border-rose-500/20 text-rose-200'
                                }`}
                        >
                            {/* Step number */}
                            <span className={`w-5 h-5 rounded-full flex items-center justify-center text-[10px] font-bold flex-shrink-0
                                ${isOk ? 'bg-emerald-500/20 text-emerald-400' : 'bg-rose-500/20 text-rose-400'}`}>
                                {idx + 1}
                            </span>

                            <span className="flex items-center gap-1.5">
                                {isOk
                                    ? <CheckCircle2 className="w-3.5 h-3.5 text-emerald-400 flex-shrink-0" />
                                    : <AlertCircle className="w-3.5 h-3.5 text-rose-400 flex-shrink-0" />
                                }
                                {item.label || item.id || 'Specialist'}
                            </span>

                            <span className={`text-[10px] uppercase tracking-[0.15em] opacity-60 font-bold`}>
                                {isOk ? 'ok' : 'err'}
                            </span>
                        </div>
                    );
                })}

                {/* Pending placeholder when still running */}
                {isActive && total > done && Array.from({ length: total - done }).map((_, i) => (
                    <div
                        key={`pending-${i}`}
                        className="flex items-center gap-2 px-3 py-2 rounded-xl border border-white/[0.06] bg-white/[0.02] text-xs font-medium text-slate-600"
                    >
                        <span className="w-5 h-5 rounded-full bg-white/[0.04] flex items-center justify-center text-[10px] font-bold">
                            {done + i + 1}
                        </span>
                        <Loader2 className="w-3.5 h-3.5 animate-spin opacity-50" />
                        <span className="opacity-50">Queued</span>
                    </div>
                ))}
            </div>

            {planner && (
                <div className="px-5 pb-3 text-[10px] text-slate-600 font-mono">
                    Planner: {planner}
                </div>
            )}
        </div>
    );
}

const AUTO_PHASES = [
    'Routing to specialists…',
    'Querying credit records…',
    'Running intent analysis…',
    'Aggregating results…',
    'Composing response…',
];

/* Compact loading indicator shown while auto-mode is actively running */
export function AutoModeLoading({ mode }: { mode: 'manual' | 'auto' }) {
    const [phaseIdx, setPhaseIdx] = useState(0);

    useEffect(() => {
        if (mode !== 'auto') return;
        const id = setInterval(() => {
            setPhaseIdx(p => (p + 1) % AUTO_PHASES.length);
        }, 2200);
        return () => clearInterval(id);
    }, [mode]);

    return (
        <div className="flex gap-4 animate-fade-in-up" style={{ animationDelay: '120ms', animationFillMode: 'both' }}>
            <div className="w-8 h-8 rounded-lg bg-gradient-to-br from-cyan-500 to-indigo-600 flex items-center justify-center flex-shrink-0 shadow-lg shadow-cyan-500/10 mt-1">
                <Zap className="w-4 h-4 text-white animate-pulse" />
            </div>
            <div className="bg-slate-800/40 border border-white/5 rounded-2xl rounded-tl-sm px-5 py-3.5 shadow-lg flex items-center gap-3 min-w-[220px]">
                <div className="flex items-center gap-1.5 flex-shrink-0">
                    <div className="w-2 h-2 rounded-full bg-cyan-400 animate-bounce" style={{ animationDelay: '0ms' }} />
                    <div className="w-2 h-2 rounded-full bg-cyan-400 animate-bounce" style={{ animationDelay: '150ms' }} />
                    <div className="w-2 h-2 rounded-full bg-cyan-400 animate-bounce" style={{ animationDelay: '300ms' }} />
                </div>
                <span key={phaseIdx} className="text-xs text-slate-400 font-medium animate-fade-in">
                    {mode === 'auto' ? AUTO_PHASES[phaseIdx] : 'Thinking…'}
                </span>
            </div>
        </div>
    );
}
