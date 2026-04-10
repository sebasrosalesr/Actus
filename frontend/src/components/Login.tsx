import React, { useState } from 'react';
import { User, Lock, ArrowRight, Sparkles } from 'lucide-react';

type LoginProps = {
    onLogin: (email: string) => void;
};

const Login: React.FC<LoginProps> = ({ onLogin }) => {
    const apiBase = (
        (import.meta.env.VITE_API_BASE_URL as string | undefined)
        || (import.meta.env.VITE_API_BASE as string | undefined)
    )?.replace(/\/$/, '') ?? '';
    const [email, setEmail] = useState('');
    const [password, setPassword] = useState('');
    const [isLoading, setIsLoading] = useState(false);
    const [isSuccess, setIsSuccess] = useState(false);
    const [showGreeting, setShowGreeting] = useState(false);
    const [greetingName, setGreetingName] = useState('');
    const [formError, setFormError] = useState('');
    const envFirstName =
        (import.meta.env.VITE_USER_FIRST_NAME as string | undefined)
        || (import.meta.env.VITE_USER_NAME as string | undefined)
        || '';

    const nameFromEmail = (value: string) => {
        const base = value.split('@')[0] || '';
        if (!base) {
            return 'there';
        }
        return base
            .split(/[._-]+/)
            .filter(Boolean)
            .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
            .join(' ');
    };

    const getTimeGreeting = () => {
        const hour = new Date().getHours();
        if (hour < 5 || hour >= 18) return 'Good evening';
        if (hour < 12) return 'Good morning';
        return 'Good afternoon';
    };

    const handleSubmit = async (e: React.FormEvent) => {
        e.preventDefault();
        const trimmedEmail = email.trim();
        const trimmedPassword = password.trim();

        if (!trimmedEmail) { setFormError('Email is required.'); return; }
        if (!trimmedPassword) { setFormError('Password is required.'); return; }
        setFormError('');
        setIsLoading(true);
        let resolvedFirstName = '';
        try {
            const endpoint = apiBase
                ? `${apiBase}/api/user-context?email=${encodeURIComponent(trimmedEmail)}`
                : `/api/user-context?email=${encodeURIComponent(trimmedEmail)}`;
            const response = await fetch(endpoint);
            if (response.ok) {
                const payload = await response.json() as {
                    firstName?: string;
                    first_name?: string;
                    name?: string;
                    email?: string;
                };
                resolvedFirstName =
                    payload.firstName
                    || payload.first_name
                    || (payload.name ? payload.name.split(' ')[0] : '')
                    || '';
            }
        } catch {
            // Fallback to email-derived name if lookup fails.
        }

        setGreetingName(resolvedFirstName || envFirstName || nameFromEmail(trimmedEmail));
        setTimeout(() => {
            setIsLoading(false);
            setIsSuccess(true);

            // Sequence the greeting
            setTimeout(() => setShowGreeting(true), 100);

            // Complete login after animation
            setTimeout(() => {
                onLogin(trimmedEmail);
            }, 3500);
        }, 800);
    };

    if (isSuccess) {
        return (
            <div className="min-h-screen flex items-center justify-center p-4 relative overflow-hidden bg-obsidian-950">
                {/* Background Ambience - slightly different for success state */}
                <div className="absolute inset-0 overflow-hidden pointer-events-none">
                    <div className="absolute top-1/3 left-1/4 w-[600px] h-[600px] bg-cyan-900/10 rounded-full blur-[120px] animate-pulse-slow"></div>
                    <div className="absolute bottom-1/3 right-1/4 w-[600px] h-[600px] bg-indigo-900/10 rounded-full blur-[120px] animate-pulse-slow" style={{ animationDelay: '1s' }}></div>
                </div>

                <div className="z-10 text-center space-y-4">
                    <div className={`transition-all duration-1000 ease-out transform ${showGreeting ? 'opacity-100 translate-y-0' : 'opacity-0 translate-y-8'}`}>
                        <h1 className="text-4xl md:text-5xl font-display font-medium text-slate-300">
                            {getTimeGreeting()},
                        </h1>
                    </div>
                    <div className={`transition-all duration-1000 ease-out delay-500 transform ${showGreeting ? 'opacity-100 translate-y-0' : 'opacity-0 translate-y-8'}`}>
                        <h1 className="text-5xl md:text-6xl font-display font-bold text-transparent bg-clip-text bg-gradient-to-r from-cyan-200 via-cyan-400 to-indigo-400 pb-2">
                            {greetingName}
                        </h1>
                    </div>
                </div>
            </div>
        );
    }

    return (
        <div className="min-h-screen flex items-center justify-center p-4 relative overflow-hidden">
            {/* Background Ambience */}
            <div className="absolute inset-0 overflow-hidden pointer-events-none">
                <div className="absolute top-0 left-1/4 w-[500px] h-[500px] bg-indigo-900/20 rounded-full blur-[120px] -translate-y-1/2 animate-pulse-slow"></div>
                <div className="absolute bottom-0 right-1/4 w-[500px] h-[500px] bg-cyan-900/10 rounded-full blur-[100px] translate-y-1/2 animate-float"></div>
            </div>

            <div className={`w-full max-w-md relative z-10 transition-all duration-700 ${isSuccess ? 'opacity-0 scale-95' : 'animate-fade-in-up'}`}>
                {/* Card */}
                <div className="relative group">
                    {/* Glow Effect */}
                    <div className="absolute -inset-0.5 bg-gradient-to-b from-cyan-500/20 to-indigo-500/20 rounded-2xl blur opacity-50 group-hover:opacity-100 transition duration-1000"></div>

                    <div className="relative bg-obsidian-950/80 backdrop-blur-xl rounded-2xl border border-white/10 p-8 shadow-2xl">

                        {/* Header */}
                        <div className="text-center mb-10">
                            <div className="flex justify-center mb-4">
                                <div className="p-3 rounded-xl bg-gradient-to-br from-cyan-500/20 to-blue-600/20 border border-white/10 shadow-lg shadow-cyan-900/20 animate-float">
                                    <Sparkles className="w-6 h-6 text-cyan-400" />
                                </div>
                            </div>
                            <h2 className="text-3xl font-bold font-display bg-gradient-to-br from-white via-slate-100 to-slate-400 bg-clip-text text-transparent mb-2">
                                Welcome Back
                            </h2>
                            <p className="text-slate-400 font-sans">Enter your credentials to access the portal</p>
                        </div>

                        {/* Form */}
                        <form onSubmit={handleSubmit} className="space-y-6" noValidate>
                            <div className="space-y-2">
                                <label className="text-xs font-bold text-slate-500 uppercase tracking-widest pl-1">Email Address</label>
                                <div className="relative group/input">
                                    <div className="absolute left-3 top-1/2 -translate-y-1/2 text-slate-500 group-focus-within/input:text-cyan-400 transition-colors">
                                        <User className="w-5 h-5" />
                                    </div>
                                    <input
                                        type="email"
                                        value={email}
                                        onChange={(e) => setEmail(e.target.value)}
                                        className="w-full bg-slate-900/50 border border-white/10 rounded-xl py-3 pl-10 pr-4 text-slate-200 placeholder-slate-600 focus:outline-none focus:border-cyan-500/50 focus:ring-1 focus:ring-cyan-500/50 transition-all duration-300 font-sans"
                                        placeholder="name@company.com"
                                    />
                                </div>
                            </div>

                            <div className="space-y-2">
                                <label className="text-xs font-bold text-slate-500 uppercase tracking-widest pl-1">Password</label>
                                <div className="relative group/input">
                                    <div className="absolute left-3 top-1/2 -translate-y-1/2 text-slate-500 group-focus-within/input:text-cyan-400 transition-colors">
                                        <Lock className="w-5 h-5" />
                                    </div>
                                    <input
                                        type="password"
                                        value={password}
                                        onChange={(e) => setPassword(e.target.value)}
                                        className="w-full bg-slate-900/50 border border-white/10 rounded-xl py-3 pl-10 pr-4 text-slate-200 placeholder-slate-600 focus:outline-none focus:border-cyan-500/50 focus:ring-1 focus:ring-cyan-500/50 transition-all duration-300 font-sans"
                                        placeholder="••••••••"
                                    />
                                </div>
                            </div>

                            <div className="flex items-center justify-between text-sm font-sans">
                                <label className="flex items-center gap-2 cursor-pointer group/check">
                                    <input type="checkbox" className="w-4 h-4 rounded bg-slate-900 border-white/10 text-cyan-500 focus:ring-offset-0 focus:ring-cyan-500/20 transition-colors" />
                                    <span className="text-slate-400 group-hover/check:text-slate-300 transition-colors">Remember me</span>
                                </label>
                                <a href="#" className="text-cyan-400 hover:text-cyan-300 transition-colors hover:underline">Forgot password?</a>
                            </div>

                            {formError && (
                                <div role="alert" className="flex items-center gap-2 px-3 py-2 rounded-lg bg-rose-500/10 border border-rose-500/20 text-xs text-rose-300 -mt-2">
                                    <span className="w-1.5 h-1.5 rounded-full bg-rose-400 flex-shrink-0" />
                                    {formError}
                                </div>
                            )}

                            <button
                                type="submit"
                                disabled={isLoading}
                                className="w-full group relative overflow-hidden rounded-xl bg-gradient-to-r from-cyan-600 to-blue-600 p-[1px] transition-all hover:shadow-[0_0_20px_rgba(6,182,212,0.4)] disabled:opacity-70 mt-2"
                            >
                                <div className="relative flex items-center justify-center gap-2 bg-obsidian-900/50 hover:bg-transparent h-full w-full py-3.5 transition-all duration-300 rounded-[11px]">
                                    <span className="font-semibold text-white tracking-wide font-sans">
                                        {isLoading ? 'Signing In...' : 'Sign In'}
                                    </span>
                                    {!isLoading && <ArrowRight className="w-4 h-4 text-cyan-100 group-hover:translate-x-1 transition-transform" />}
                                </div>
                            </button>
                        </form>

                        {/* Footer */}
                        <div className="mt-8 text-center">
                            <p className="text-slate-500 text-sm font-sans">
                                Don't have an account?{' '}
                                <a href="#" className="text-slate-300 hover:text-white transition-colors font-medium">Contact Admin</a>
                            </p>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    );
};

export default Login;
