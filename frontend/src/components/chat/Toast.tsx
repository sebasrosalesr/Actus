import { createContext, useContext, useReducer, useCallback, useEffect, useRef } from 'react';
import { CheckCircle2, AlertCircle, Info, X } from 'lucide-react';

// ─── Types ────────────────────────────────────────────────────────────────────

type ToastType = 'success' | 'error' | 'info';

type Toast = {
    id: string;
    message: string;
    type: ToastType;
};

type ToastAction =
    | { type: 'ADD'; toast: Toast }
    | { type: 'REMOVE'; id: string };

// ─── Context ──────────────────────────────────────────────────────────────────

type ToastContextValue = {
    addToast: (message: string, type?: ToastType) => void;
};

const ToastContext = createContext<ToastContextValue>({ addToast: () => {} });

export function useToast() {
    return useContext(ToastContext);
}

// ─── Reducer ──────────────────────────────────────────────────────────────────

function reducer(state: Toast[], action: ToastAction): Toast[] {
    switch (action.type) {
        case 'ADD': return [...state, action.toast];
        case 'REMOVE': return state.filter(t => t.id !== action.id);
        default: return state;
    }
}

// ─── Individual toast ─────────────────────────────────────────────────────────

function ToastItem({ toast, onRemove }: { toast: Toast; onRemove: (id: string) => void }) {
    const timerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

    useEffect(() => {
        timerRef.current = setTimeout(() => onRemove(toast.id), 3500);
        return () => { if (timerRef.current) clearTimeout(timerRef.current); };
    }, [toast.id, onRemove]);

    const icons = {
        success: <CheckCircle2 className="w-4 h-4 text-emerald-400 flex-shrink-0" />,
        error: <AlertCircle className="w-4 h-4 text-rose-400 flex-shrink-0" />,
        info: <Info className="w-4 h-4 text-cyan-400 flex-shrink-0" />,
    };

    const borders = {
        success: 'border-emerald-500/20',
        error: 'border-rose-500/20',
        info: 'border-cyan-500/20',
    };

    return (
        <div
            className={`flex items-center gap-3 px-4 py-3 rounded-xl bg-obsidian-950/95 border ${borders[toast.type]} shadow-2xl backdrop-blur-xl animate-fade-in-up text-sm text-slate-200 max-w-sm`}
            role="status"
            aria-live="polite"
        >
            {icons[toast.type]}
            <span className="flex-1">{toast.message}</span>
            <button
                onClick={() => onRemove(toast.id)}
                aria-label="Dismiss notification"
                className="text-slate-500 hover:text-slate-300 transition-colors ml-1"
            >
                <X className="w-3.5 h-3.5" />
            </button>
        </div>
    );
}

// ─── Provider + Container ─────────────────────────────────────────────────────

export function ToastProvider({ children }: { children: React.ReactNode }) {
    const [toasts, dispatch] = useReducer(reducer, []);

    const addToast = useCallback((message: string, type: ToastType = 'info') => {
        const id = `${Date.now()}-${Math.random().toString(16).slice(2)}`;
        dispatch({ type: 'ADD', toast: { id, message, type } });
    }, []);

    const removeToast = useCallback((id: string) => {
        dispatch({ type: 'REMOVE', id });
    }, []);

    return (
        <ToastContext.Provider value={{ addToast }}>
            {children}
            {/* Toast container — fixed bottom-right */}
            <div
                className="fixed bottom-6 right-6 z-[200] flex flex-col gap-2 items-end pointer-events-none"
                aria-label="Notifications"
            >
                {toasts.map(t => (
                    <div key={t.id} className="pointer-events-auto">
                        <ToastItem toast={t} onRemove={removeToast} />
                    </div>
                ))}
            </div>
        </ToastContext.Provider>
    );
}
