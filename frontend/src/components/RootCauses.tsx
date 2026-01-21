import React from 'react';
import { AlertCircle, DollarSign, Hash } from 'lucide-react';

export type RootCauseItem = {
    rank: number;
    rootCause: string;
    creditRequestTotal: number;
    recordCount: number;
};

type RootCausesProps = {
    data: RootCauseItem[];
    period?: string;
};

const RootCauses: React.FC<RootCausesProps> = ({ data, period = "Last 30 Days" }) => {
    const formatCurrency = (value: number) => {
        return new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD', maximumFractionDigits: 0 }).format(value);
    };

    const formatNumber = (value: number) => {
        return new Intl.NumberFormat('en-US').format(value);
    };

    return (
        <div className="flex flex-col gap-6 w-full max-w-7xl mx-auto p-4 font-sans text-slate-200">
            {/* Context Bubble (Optional, keeping consistent with Mockup.tsx) */}
            <div className="flex items-start gap-4 animate-fade-in-up">
                <div className="glass-panel p-5 rounded-2xl rounded-tl-none border border-white/10 max-w-2xl bg-slate-900/60 backdrop-blur-md shadow-xl ring-1 ring-white/5">
                    <p className="mb-3 text-[15px] leading-relaxed text-slate-200">
                        Here is the <strong className="text-white font-semibold">Root Cause Analysis</strong> breakdown.
                    </p>
                </div>
            </div>

            {/* Main Dashboard Card */}
            <div className="group relative p-[1px] rounded-[24px] bg-gradient-to-b from-white/10 to-transparent animate-scale-in">
                {/* Glow Effect */}
                <div className="absolute -inset-0.5 bg-gradient-to-b from-rose-500/20 to-orange-500/20 rounded-[24px] blur opacity-50 group-hover:opacity-75 transition duration-1000"></div>

                <div className="relative bg-obsidian-950/90 backdrop-blur-2xl rounded-[23px] p-8 shadow-2xl overflow-hidden min-h-[500px]">
                    {/* Background Ambience */}
                    <div className="absolute top-0 right-0 w-[500px] h-[500px] bg-rose-900/20 rounded-full blur-[120px] -translate-y-1/2 translate-x-1/2 pointer-events-none"></div>
                    <div className="absolute bottom-0 left-0 w-[400px] h-[400px] bg-orange-900/10 rounded-full blur-[100px] translate-y-1/2 -translate-x-1/4 pointer-events-none"></div>

                    {/* Header */}
                    <div className="relative flex items-center gap-3.5 mb-8 pb-6 border-b border-white/[0.06]">
                        <div className="p-2 rounded-lg bg-gradient-to-br from-rose-500/20 to-orange-600/20 border border-white/10 shadow-lg shadow-rose-900/20">
                            <AlertCircle className="w-5 h-5 text-rose-400" />
                        </div>
                        <div>
                            <h2 className="text-xl font-bold text-white tracking-tight font-display bg-gradient-to-r from-white via-slate-100 to-slate-400 bg-clip-text text-transparent">
                                Root Cause Analysis
                            </h2>
                            <p className="text-xs text-slate-400 font-medium tracking-wide upppercase mt-1">
                                {period}
                            </p>
                        </div>
                    </div>

                    {/* Table / List View */}
                    <div className="bg-slate-900/40 rounded-2xl border border-white/[0.06] overflow-hidden">
                        {/* Table Header */}
                        <div className="grid grid-cols-12 gap-4 p-4 border-b border-white/[0.06] bg-white/[0.02] text-xs font-bold text-slate-500 uppercase tracking-widest">
                            <div className="col-span-1 text-center">#</div>
                            <div className="col-span-5">Root Cause</div>
                            <div className="col-span-3 text-right">Credit Request Total</div>
                            <div className="col-span-3 text-right">Record Count</div>
                        </div>

                        {/* Table Body */}
                        <div className="divide-y divide-white/[0.06]">
                            {data.map((item, idx) => (
                                <div
                                    key={item.rootCause}
                                    className="grid grid-cols-12 gap-4 p-4 items-center group/row hover:bg-white/[0.03] transition-colors duration-200"
                                    style={{ animationDelay: `${idx * 50}ms` }}
                                >
                                    {/* Rank */}
                                    <div className="col-span-1 flex justify-center">
                                        <span className={`w-6 h-6 flex items-center justify-center rounded-lg text-[10px] font-bold shadow-lg transition-transform group-hover/row:scale-110 ${idx === 0 ? 'bg-rose-500 text-rose-950 shadow-rose-500/20' :
                                                idx === 1 ? 'bg-orange-500 text-orange-950 shadow-orange-500/20' :
                                                    idx === 2 ? 'bg-amber-500 text-amber-950 shadow-amber-500/20' :
                                                        'bg-slate-800 text-slate-400 border border-white/5'
                                            }`}>
                                            {item.rank || idx + 1}
                                        </span>
                                    </div>

                                    {/* Root Cause Name */}
                                    <div className="col-span-5">
                                        <div className="flex items-center gap-3">
                                            <span className="text-sm font-medium text-slate-300 group-hover/row:text-white transition-colors">
                                                {item.rootCause}
                                            </span>
                                        </div>
                                    </div>

                                    {/* Credit Request Total */}
                                    <div className="col-span-3 text-right">
                                        <div className="flex items-center justify-end gap-2 text-emerald-400">
                                            <DollarSign className="w-3.5 h-3.5 opacity-70" />
                                            <span className="text-sm font-mono font-semibold tracking-tight">
                                                {formatCurrency(item.creditRequestTotal).replace('$', '')}
                                            </span>
                                        </div>
                                    </div>

                                    {/* Record Count */}
                                    <div className="col-span-3 text-right">
                                        <div className="flex items-center justify-end gap-2 text-cyan-400">
                                            <Hash className="w-3.5 h-3.5 opacity-70" />
                                            <span className="text-sm font-mono font-semibold tracking-tight">
                                                {formatNumber(item.recordCount)}
                                            </span>
                                        </div>
                                    </div>
                                </div>
                            ))}
                        </div>
                    </div>
                </div>
            </div>
        </div>
    );
};

export default RootCauses;
