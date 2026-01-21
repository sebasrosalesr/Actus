import React from 'react';
import { TrendingUp, Users, Box, DollarSign, ArrowUpRight, ArrowDownRight } from 'lucide-react';

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
    period: string;
    window?: {
        previous?: string;
        current?: string;
    };
    metrics: TrendMetric[];
    topCustomers: RankedItem[];
    topItems: RankedItem[];
    topReps: RankedItem[];
    chartData?: Array<{
        date: string;
        withCr: number;
        withoutCr: number;
        trend: number;
    }>;
};

type MockupProps = {
    creditTrends: CreditTrendsData;
};

const Mockup: React.FC<MockupProps> = ({ creditTrends }) => {
    const formatMetricValue = (metric: TrendMetric) => {
        if (metric.isCurrency) {
            const num = Number(metric.current);
            if (Number.isFinite(num)) {
                return new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD', maximumFractionDigits: 2 }).format(num);
            }
        }
        return metric.current;
    };

    const formatMetricPrevious = (metric: TrendMetric) => {
        if (metric.isCurrency) {
            const num = Number(metric.previous);
            if (Number.isFinite(num)) {
                return new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD', maximumFractionDigits: 2 }).format(num);
            }
        }
        return metric.previous;
    };

    return (
        <div className="flex flex-col gap-6 w-full max-w-7xl mx-auto p-4 font-sans text-slate-200">
            {/* Context Bubble */}
            <div className="flex items-start gap-4 animate-fade-in-up">
                <div className="glass-panel p-5 rounded-2xl rounded-tl-none border border-white/10 max-w-2xl bg-slate-900/60 backdrop-blur-md shadow-xl ring-1 ring-white/5">
                    <p className="mb-3 text-[15px] leading-relaxed text-slate-200">
                        Here is the <strong className="text-white font-semibold">Credit Trends Analysis</strong> for the requested period.
                    </p>
                    <ul className="space-y-2 text-sm text-slate-400">
                        <li className="flex items-center gap-2.5">
                            <span className="w-1.5 h-1.5 rounded-full bg-cyan-500 shadow-[0_0_8px_rgba(6,182,212,0.6)]"></span>
                            <span>Previous 30 days: <span className="text-slate-200 font-mono bg-white/5 px-1.5 py-0.5 rounded border border-white/5">{creditTrends.window?.previous || 'N/A'}</span></span>
                        </li>
                        <li className="flex items-center gap-2.5">
                            <span className="w-1.5 h-1.5 rounded-full bg-indigo-500 shadow-[0_0_8px_rgba(99,102,241,0.6)]"></span>
                            <span>Last 30 days: <span className="text-slate-200 font-mono bg-white/5 px-1.5 py-0.5 rounded border border-white/5">{creditTrends.window?.current || 'N/A'}</span></span>
                        </li>
                    </ul>
                </div>
            </div>

            {/* Main Dashboard Card */}
            <div className="group relative p-[1px] rounded-[24px] bg-gradient-to-b from-white/10 to-transparent animate-scale-in">
                {/* Glow Effect */}
                <div className="absolute -inset-0.5 bg-gradient-to-b from-cyan-500/20 to-indigo-500/20 rounded-[24px] blur opacity-50 group-hover:opacity-75 transition duration-1000"></div>

                <div className="relative bg-obsidian-950/90 backdrop-blur-2xl rounded-[23px] p-8 shadow-2xl overflow-hidden">
                    {/* Background Ambience */}
                    <div className="absolute top-0 right-0 w-[500px] h-[500px] bg-indigo-900/20 rounded-full blur-[120px] -translate-y-1/2 translate-x-1/2 pointer-events-none"></div>
                    <div className="absolute bottom-0 left-0 w-[400px] h-[400px] bg-cyan-900/10 rounded-full blur-[100px] translate-y-1/2 -translate-x-1/4 pointer-events-none"></div>

                    {/* Header */}
                    <div className="relative flex items-center gap-3.5 mb-8 pb-6 border-b border-white/[0.06]">
                        <div className="p-2 rounded-lg bg-gradient-to-br from-cyan-500/20 to-blue-600/20 border border-white/10 shadow-lg shadow-cyan-900/20">
                            <TrendingUp className="w-5 h-5 text-cyan-400" />
                        </div>
                        <div>
                            <h2 className="text-xl font-bold text-white tracking-tight font-display bg-gradient-to-r from-white via-slate-100 to-slate-400 bg-clip-text text-transparent">
                                Analysis: {creditTrends.period}
                            </h2>
                        </div>
                    </div>

                    {/* Metrics Row */}
                    <div className="grid grid-cols-1 md:grid-cols-3 gap-5 mb-8">
                        {creditTrends.metrics.map((metric, idx) => {
                            const rising = metric.change >= 0;
                            return (
                                <div
                                    key={metric.label}
                                    className="relative group/card overflow-hidden rounded-2xl bg-slate-900/40 border border-white/[0.06] hover:border-white/[0.12] transition-all duration-500 hover:shadow-2xl hover:shadow-cyan-900/10 hover:-translate-y-1"
                                    style={{ animationDelay: `${idx * 100}ms` }}
                                >
                                    <div className="absolute inset-0 bg-gradient-to-br from-white/[0.02] to-transparent opacity-0 group-hover/card:opacity-100 transition-opacity duration-500"></div>
                                    <div className="relative p-6 z-10">
                                        <div className="flex items-center justify-between mb-4">
                                            <div className="text-xs font-bold text-slate-500 uppercase tracking-widest">{metric.label}</div>
                                            {idx === 0 && <div className="w-1.5 h-1.5 rounded-full bg-cyan-400 shadow-[0_0_8px_rgba(34,211,238,0.5)] animate-pulse"></div>}
                                        </div>

                                        <div className="flex items-end gap-3 mb-4">
                                            <span className="text-4xl font-bold text-white font-display tracking-tight leading-none bg-gradient-to-b from-white to-slate-300 bg-clip-text text-transparent">
                                                {formatMetricValue(metric)}
                                            </span>
                                        </div>

                                        <div className="flex items-center justify-between pt-3 border-t border-dashed border-white/10">
                                            <div className="flex flex-col">
                                                <span className="text-[10px] text-slate-500 font-semibold uppercase tracking-wider mb-0.5">Change</span>
                                                <span className={`inline-flex items-center gap-1 text-sm font-bold ${rising ? 'text-emerald-400 drop-shadow-[0_0_8px_rgba(52,211,153,0.3)]' : 'text-rose-400 drop-shadow-[0_0_8px_rgba(251,113,133,0.3)]'}`}>
                                                    {rising ? <ArrowUpRight className="w-4 h-4" /> : <ArrowDownRight className="w-4 h-4" />}
                                                    {Math.abs(metric.change)}%
                                                </span>
                                            </div>
                                            <div className="flex flex-col items-end">
                                                <span className="text-[10px] text-slate-500 font-semibold uppercase tracking-wider mb-0.5">Previous</span>
                                                <span className="font-mono text-xs text-slate-400 bg-white/[0.03] px-1.5 py-0.5 rounded border border-white/[0.05]">
                                                    {formatMetricPrevious(metric)}
                                                </span>
                                            </div>
                                        </div>
                                    </div>
                                </div>
                            );
                        })}
                    </div>

                    {/* Ranks Grid */}
                    <div className="grid grid-cols-1 md:grid-cols-3 gap-5">
                        {/* Top Customers */}
                        <div className="bg-slate-900/40 rounded-2xl p-6 border border-white/[0.06] hover:border-white/[0.1] transition-all duration-300 group/list">
                            <div className="flex items-center gap-2.5 mb-6">
                                <div className="p-1.5 rounded bg-emerald-500/10 border border-emerald-500/20 text-emerald-400">
                                    <Users className="w-4 h-4" />
                                </div>
                                <h3 className="text-xs font-bold text-emerald-400 uppercase tracking-widest text-glow-emerald">Top Customers</h3>
                            </div>
                            <div className="space-y-3">
                                {creditTrends.topCustomers.map((item) => (
                                    <div key={item.rank} className="flex items-center justify-between group/item p-2 rounded-lg hover:bg-white/[0.03] transition-colors border border-transparent hover:border-white/[0.05]">
                                        <div className="flex items-center gap-3">
                                            <span className={`w-6 h-6 flex items-center justify-center rounded-lg text-[10px] font-bold shadow-lg transition-transform group-hover/item:scale-110 ${item.rank === 1 ? 'bg-emerald-500 text-emerald-950 shadow-emerald-500/20' : 'bg-slate-800 text-slate-400 border border-white/5'
                                                }`}>
                                                {item.rank}
                                            </span>
                                            <span className="text-sm font-medium text-slate-300 group-hover/item:text-white transition-colors">{item.name}</span>
                                        </div>
                                        <span className="text-sm font-mono text-emerald-400/90 font-semibold">{item.value}</span>
                                    </div>
                                ))}
                            </div>
                        </div>

                        {/* Top Items */}
                        <div className="bg-slate-900/40 rounded-2xl p-6 border border-white/[0.06] hover:border-white/[0.1] transition-all duration-300 group/list">
                            <div className="flex items-center gap-2.5 mb-6">
                                <div className="p-1.5 rounded bg-cyan-500/10 border border-cyan-500/20 text-cyan-400">
                                    <Box className="w-4 h-4" />
                                </div>
                                <h3 className="text-xs font-bold text-cyan-400 uppercase tracking-widest text-glow-cyan">Top Items</h3>
                            </div>
                            <div className="space-y-3">
                                {creditTrends.topItems.map((item) => (
                                    <div key={item.rank} className="flex items-center justify-between group/item p-2 rounded-lg hover:bg-white/[0.03] transition-colors border border-transparent hover:border-white/[0.05]">
                                        <div className="flex items-center gap-3">
                                            <span className={`w-6 h-6 flex items-center justify-center rounded-lg text-[10px] font-bold shadow-lg transition-transform group-hover/item:scale-110 ${item.rank === 1 ? 'bg-cyan-500 text-cyan-950 shadow-cyan-500/20' : 'bg-slate-800 text-slate-400 border border-white/5'
                                                }`}>
                                                {item.rank}
                                            </span>
                                            <span className="text-sm font-medium text-slate-300 group-hover/item:text-white transition-colors">{item.name}</span>
                                        </div>
                                        <span className="text-sm font-mono text-cyan-400/90 font-semibold">{item.value}</span>
                                    </div>
                                ))}
                            </div>
                        </div>

                        {/* Top Sales Reps */}
                        <div className="bg-slate-900/40 rounded-2xl p-6 border border-white/[0.06] hover:border-white/[0.1] transition-all duration-300 group/list">
                            <div className="flex items-center gap-2.5 mb-6">
                                <div className="p-1.5 rounded bg-violet-500/10 border border-violet-500/20 text-violet-400">
                                    <DollarSign className="w-4 h-4" />
                                </div>
                                <h3 className="text-xs font-bold text-violet-400 uppercase tracking-widest text-glow-violet">Top Sales Reps</h3>
                            </div>
                            <div className="space-y-3">
                                {creditTrends.topReps.map((item) => (
                                    <div key={item.rank} className="flex items-center justify-between group/item p-2 rounded-lg hover:bg-white/[0.03] transition-colors border border-transparent hover:border-white/[0.05]">
                                        <div className="flex items-center gap-3">
                                            <span className={`w-6 h-6 flex items-center justify-center rounded-lg text-[10px] font-bold shadow-lg transition-transform group-hover/item:scale-110 ${item.rank === 1 ? 'bg-violet-500 text-violet-950 shadow-violet-500/20' : 'bg-slate-800 text-slate-400 border border-white/5'
                                                }`}>
                                                {item.rank}
                                            </span>
                                            <span className="text-sm font-medium text-slate-300 truncate max-w-[110px] group-hover/item:text-white transition-colors" title={item.name}>{item.name}</span>
                                        </div>
                                        <span className="text-sm font-mono text-violet-400/90 font-semibold">{item.value}</span>
                                    </div>
                                ))}
                            </div>
                        </div>

                    </div>
                </div>
            </div>
        </div>
    );
};

export default Mockup;
