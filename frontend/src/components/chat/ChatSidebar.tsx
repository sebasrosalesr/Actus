import { X, Sparkles, Trash2, TrendingUp, ArrowRight } from 'lucide-react';
import type { UserContext, ContextualSuggestion, AskMode } from './types';

type SuggestedIntent = {
    label: string;
    query?: string;
    scope?: string;
    mode?: AskMode;
};

type ChatSidebarProps = {
    isOpen: boolean;
    onClose: () => void;
    suggestedIntents: SuggestedIntent[];
    recentIntents: string[];
    userContext: UserContext;
    contextualSuggestions: ContextualSuggestion[];
    onSendMessage: (query: string, opts?: { bypassPending?: boolean; modeOverride?: AskMode }) => void;
    onClearMessages: () => void;
    hasMessages: boolean;
};

export function ChatSidebar({
    isOpen,
    onClose,
    suggestedIntents,
    recentIntents,
    userContext,
    contextualSuggestions,
    onSendMessage,
    onClearMessages,
    hasMessages,
}: ChatSidebarProps) {
    return (
        <>
            {/* Backdrop */}
            {isOpen && (
                <div
                    className="fixed inset-0 bg-black/40 z-[55] transition-opacity duration-300"
                    onClick={onClose}
                />
            )}

            {/* Drawer — slides in from right on ≥sm, slides up from bottom on mobile */}
            <div
                className={[
                    'fixed z-[60] bg-obsidian-950/90 backdrop-blur-2xl shadow-2xl',
                    'transition-transform duration-500',
                    // Mobile: bottom sheet
                    'bottom-0 left-0 right-0 max-h-[78vh] rounded-t-2xl border-t border-white/[0.08]',
                    // Desktop: right side drawer
                    'sm:top-0 sm:bottom-0 sm:left-auto sm:right-0 sm:w-80 sm:max-h-none sm:rounded-none sm:rounded-l-none sm:border-t-0 sm:border-l sm:border-white/[0.08]',
                    // Open/closed states
                    isOpen
                        ? 'translate-y-0 sm:translate-x-0'
                        : 'translate-y-full sm:translate-x-full sm:translate-y-0',
                ].join(' ')}
            >
                {/* Accent line (desktop only) */}
                <div className="hidden sm:block absolute inset-y-0 left-0 w-[1px] bg-gradient-to-b from-transparent via-cyan-500/30 to-transparent" />

                <div className="h-full flex flex-col overflow-hidden">
                    {/* Header */}
                    <div className="flex items-center justify-between px-6 py-4 sm:h-24 sm:py-0 border-b border-white/[0.06] flex-shrink-0">
                        <div className="flex flex-col gap-0.5">
                            <h2 className="text-lg font-bold font-display tracking-tight text-transparent bg-clip-text bg-gradient-to-r from-white to-slate-400">
                                Settings &amp; History
                            </h2>
                            <span className="text-[10px] uppercase tracking-widest text-cyan-500/80 font-semibold">User Controls</span>
                        </div>
                        <button
                            onClick={onClose}
                            className="p-2 -mr-1 text-slate-400 hover:text-white hover:bg-white/5 rounded-lg transition-colors group"
                        >
                            <X className="w-5 h-5 group-hover:rotate-90 transition-transform duration-300" />
                        </button>
                    </div>

                    {/* Scrollable body */}
                    <div className="flex-1 overflow-y-auto p-4 space-y-7 overscroll-contain">

                        {/* ── Contextual follow-up (dynamic, only when available) ── */}
                        {contextualSuggestions.length > 0 && (
                            <div>
                                <div className="flex items-center gap-2 mb-3 px-2">
                                    <div className="w-1 h-1 rounded-full bg-amber-400" />
                                    <h3 className="text-[10px] font-bold text-slate-500 uppercase tracking-widest">Follow-up</h3>
                                </div>
                                <div className="space-y-1">
                                    {contextualSuggestions.map((s, i) => (
                                        <button
                                            key={i}
                                            onClick={() => { onSendMessage(s.query, { bypassPending: true }); onClose(); }}
                                            className="w-full text-left p-3.5 rounded-xl hover:bg-gradient-to-r hover:from-amber-500/[0.07] hover:to-transparent border border-transparent hover:border-amber-500/[0.1] transition-all group flex items-center gap-3"
                                        >
                                            <TrendingUp className="w-4 h-4 text-slate-600 group-hover:text-amber-400 transition-colors flex-shrink-0" />
                                            <span className="text-sm text-slate-300 group-hover:text-white transition-colors font-medium truncate">
                                                {s.label}
                                            </span>
                                            <ArrowRight className="w-3.5 h-3.5 text-slate-700 group-hover:text-amber-400 opacity-0 group-hover:opacity-100 ml-auto transition-all flex-shrink-0" />
                                        </button>
                                    ))}
                                </div>
                            </div>
                        )}

                        {/* ── Static suggestions ── */}
                        <div>
                            <div className="flex items-center gap-2 mb-3 px-2">
                                <div className="w-1 h-1 rounded-full bg-cyan-400" />
                                <h3 className="text-[10px] font-bold text-slate-500 uppercase tracking-widest">Suggested Actions</h3>
                            </div>
                            <div className="space-y-1">
                                {suggestedIntents.map(intent => (
                                    <button
                                        key={intent.label}
                                        onClick={() => {
                                            onSendMessage(intent.query || (intent.scope ? `${intent.label} (${intent.scope})` : intent.label), {
                                                bypassPending: true,
                                                modeOverride: intent.mode,
                                            });
                                            onClose();
                                        }}
                                        className="w-full text-left p-3.5 rounded-xl hover:bg-gradient-to-r hover:from-white/[0.07] hover:to-transparent border border-transparent hover:border-white/[0.05] transition-all group flex items-start gap-3"
                                    >
                                        <Sparkles className="w-4 h-4 text-slate-600 group-hover:text-cyan-400 transition-colors mt-0.5 flex-shrink-0" />
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

                        {/* ── Recent chats ── */}
                        <div>
                            <div className="flex items-center justify-between mb-3 px-2">
                                <div className="flex items-center gap-2">
                                    <div className="w-1 h-1 rounded-full bg-indigo-400" />
                                    <h3 className="text-[10px] font-bold text-slate-500 uppercase tracking-widest">Recent Chats</h3>
                                </div>
                                {hasMessages && (
                                    <button
                                        onClick={() => { onClearMessages(); onClose(); }}
                                        className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-lg text-[10px] font-semibold text-slate-500 hover:text-rose-400 hover:bg-rose-500/[0.07] border border-transparent hover:border-rose-500/20 transition-all uppercase tracking-wide"
                                        title="Clear conversation"
                                    >
                                        <Trash2 className="w-3 h-3" />
                                        Clear
                                    </button>
                                )}
                            </div>
                            <div className="space-y-1">
                                {recentIntents.length === 0 ? (
                                    <div className="px-3 py-8 text-center border border-dashed border-white/5 rounded-xl bg-white/[0.02]">
                                        <p className="text-xs text-slate-500">No chat history yet.</p>
                                    </div>
                                ) : (
                                    recentIntents.map((intent, i) => (
                                        <button
                                            key={`${intent}-${i}`}
                                            onClick={() => { onSendMessage(intent, { bypassPending: true }); onClose(); }}
                                            className="w-full text-left p-3.5 rounded-xl hover:bg-gradient-to-r hover:from-white/[0.07] hover:to-transparent border border-transparent hover:border-white/[0.05] transition-all group flex items-center gap-3"
                                        >
                                            <div className="w-1.5 h-1.5 rounded-full bg-slate-700 group-hover:bg-indigo-400 transition-colors flex-shrink-0" />
                                            <span className="text-sm text-slate-400 group-hover:text-slate-200 transition-colors truncate">
                                                {intent}
                                            </span>
                                        </button>
                                    ))
                                )}
                            </div>
                        </div>
                    </div>

                    {/* User info footer */}
                    <div className="p-4 bg-gradient-to-t from-black/40 to-transparent flex-shrink-0">
                        <div className="p-4 rounded-2xl bg-white/[0.03] border border-white/[0.06] backdrop-blur-sm flex items-center gap-4 group cursor-default">
                            <div className="w-10 h-10 rounded-full bg-gradient-to-br from-cyan-500 to-blue-600 flex items-center justify-center text-sm font-bold text-white shadow-lg shadow-cyan-900/20 flex-shrink-0">
                                {userContext.firstName ? userContext.firstName.charAt(0) : 'U'}
                            </div>
                            <div className="flex-1 min-w-0">
                                <div className="text-sm text-slate-200 font-medium truncate group-hover:text-white transition-colors">
                                    {userContext.name || 'User'}
                                </div>
                                <div className="text-xs text-slate-500 flex items-center gap-1.5 mt-0.5">
                                    <div className="w-1.5 h-1.5 rounded-full bg-emerald-500 flex-shrink-0" />
                                    Online · {userContext.lastLogin}
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </>
    );
}
