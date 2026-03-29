import React, { useRef, useEffect } from 'react';
import { Send, X } from 'lucide-react';
import type { AskMode } from './types';

type ChatInputProps = {
    value: string;
    onChange: (value: string) => void;
    onSend: () => void;
    isTyping: boolean;
    askMode: AskMode;
    onModeChange: (mode: AskMode) => void;
    isSidebarOpen: boolean;
    pendingFollowup?: { intent: string; prefix: string } | null;
    onCancelFollowup?: () => void;
};

export function ChatInput({
    value, onChange, onSend, isTyping, askMode, onModeChange, isSidebarOpen,
    pendingFollowup, onCancelFollowup,
}: ChatInputProps) {
    const inputRef = useRef<HTMLTextAreaElement>(null);

    const handleKeyDown = (e: React.KeyboardEvent) => {
        if (e.key === 'Enter' && !e.shiftKey) {
            e.preventDefault();
            onSend();
        }
    };

    useEffect(() => {
        inputRef.current?.focus();
    }, []);

    return (
        <div className="fixed bottom-0 inset-x-0 bg-transparent p-6 z-50">
            <div
                className={`max-w-4xl relative transition-all duration-300 ease-out ${isSidebarOpen ? 'sm:mr-[340px]' : ''} mx-auto`}
            >
                {/* Pending followup banner */}
                {pendingFollowup && (
                    <div className="flex items-center gap-2 px-3 pb-3 animate-fade-in">
                        <div className="flex-1 flex items-center gap-2.5 px-3.5 py-2 rounded-xl bg-amber-500/[0.06] border border-amber-500/[0.18] backdrop-blur-xl">
                            <div className="w-1.5 h-1.5 rounded-full bg-amber-400 animate-pulse flex-shrink-0" />
                            <span className="text-xs text-amber-200/70 font-medium truncate">
                                Follow-up context active:{' '}
                                <span className="text-amber-300 font-mono">{pendingFollowup.intent}</span>
                            </span>
                        </div>
                        <button
                            type="button"
                            onClick={onCancelFollowup}
                            title="Cancel follow-up context"
                            className="p-1.5 rounded-lg text-slate-500 hover:text-slate-300 hover:bg-white/[0.05] transition-colors flex-shrink-0"
                        >
                            <X className="w-3.5 h-3.5" />
                        </button>
                    </div>
                )}

                {/* Mode toggle + hint */}
                <div className="flex items-center justify-between px-3 pb-3">
                    <div className="inline-flex items-center gap-1 p-1 rounded-full bg-obsidian-950/70 border border-white/[0.08] backdrop-blur-xl">
                        <button
                            type="button"
                            onClick={() => onModeChange('manual')}
                            className={`px-4 py-1.5 rounded-full text-xs font-semibold transition-colors ${askMode === 'manual' ? 'bg-white/10 text-white' : 'text-slate-400 hover:text-white'}`}
                        >
                            Manual
                        </button>
                        <button
                            type="button"
                            onClick={() => onModeChange('auto')}
                            className={`px-4 py-1.5 rounded-full text-xs font-semibold transition-colors ${askMode === 'auto' ? 'bg-cyan-500/15 text-cyan-200 border border-cyan-500/20' : 'text-slate-400 hover:text-white'}`}
                        >
                            Auto
                        </button>
                    </div>
                    <div className="flex items-center gap-3">
                        <span className="text-[10px] uppercase tracking-[0.22em] text-slate-500 font-semibold">
                            {askMode === 'auto' ? 'Orchestrated Specialists' : 'Single Intent'}
                        </span>
                        <span className="hidden sm:block text-[10px] text-slate-700 font-mono">
                            ↵ send · ⇧↵ newline
                        </span>
                    </div>
                </div>

                {/* Input pill */}
                <div className="relative group z-20">
                    <div className={`absolute -inset-1 rounded-full blur-md opacity-40 transition duration-700 bg-gradient-to-r ${
                        isTyping
                            ? 'from-cyan-500/30 via-blue-500/20 to-cyan-500/30 opacity-60 animate-pulse'
                            : 'from-cyan-500/20 via-blue-500/20 to-purple-500/20 group-hover:opacity-100'
                    }`} />
                    <div className={`relative flex items-center bg-obsidian-950/80 backdrop-blur-2xl border rounded-full shadow-2xl overflow-hidden transition-all duration-300 ${
                        isTyping
                            ? 'border-cyan-500/20 ring-1 ring-cyan-500/10'
                            : 'border-white/[0.08] group-hover:border-white/[0.15] focus-within:ring-1 focus-within:ring-cyan-500/30 focus-within:border-cyan-500/50 focus-within:shadow-[0_0_45px_-10px_rgba(6,182,212,0.4)] group-hover:-translate-y-0.5 transform'
                    }`}>
                        <textarea
                            ref={inputRef}
                            value={value}
                            onChange={e => onChange(e.target.value)}
                            onKeyDown={handleKeyDown}
                            placeholder={isTyping ? 'Waiting for response…' : 'Ask Actus anything…'}
                            disabled={isTyping}
                            className="w-full bg-transparent text-slate-100 placeholder-slate-500 px-6 py-4 focus:outline-none resize-none h-[60px] leading-[28px] text-base font-sans disabled:cursor-default"
                            rows={1}
                        />
                        <div className="flex items-center gap-2 pr-2">
                            <button
                                onClick={onSend}
                                disabled={!value.trim() || isTyping}
                                className="p-3 bg-gradient-to-r from-cyan-500 to-blue-600 hover:from-cyan-400 hover:to-blue-500 text-white rounded-full transition-all disabled:opacity-50 disabled:cursor-not-allowed shadow-[0_0_20px_-5px_rgba(6,182,212,0.4)] hover:shadow-[0_0_30px_-5px_rgba(6,182,212,0.6)] active:scale-95"
                            >
                                <Send className="w-5 h-5 ml-[1px]" />
                            </button>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    );
}
