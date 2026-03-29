import { AlertTriangle, RefreshCw } from 'lucide-react';

type ErrorCardProps = {
    message: string;
    onRetry?: () => void;
};

export function ErrorCard({ message, onRetry }: ErrorCardProps) {
    return (
        <div className="w-full max-w-xl bg-rose-500/[0.07] border border-rose-500/20 rounded-2xl p-5 shadow-lg backdrop-blur-md">
            <div className="flex items-start gap-3">
                <div className="w-8 h-8 rounded-lg bg-rose-500/10 border border-rose-500/20 flex items-center justify-center flex-shrink-0 mt-0.5">
                    <AlertTriangle className="w-4 h-4 text-rose-400" />
                </div>
                <div className="flex-1 min-w-0">
                    <p className="text-sm font-semibold text-rose-300 mb-1">Request failed</p>
                    <p className="text-xs text-rose-200/60 font-mono break-words">{message}</p>
                </div>
            </div>
            {onRetry && (
                <div className="mt-4 flex justify-end">
                    <button
                        onClick={onRetry}
                        className="inline-flex items-center gap-2 px-4 py-2 rounded-xl bg-rose-500/10 hover:bg-rose-500/20 border border-rose-500/20 text-xs font-semibold text-rose-300 transition-all active:scale-95"
                    >
                        <RefreshCw className="w-3.5 h-3.5" />
                        Retry
                    </button>
                </div>
            )}
        </div>
    );
}
