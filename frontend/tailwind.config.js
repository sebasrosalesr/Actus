/** @type {import('tailwindcss').Config} */
export default {
    content: [
        "./index.html",
        "./src/**/*.{js,ts,jsx,tsx}",
    ],
    theme: {
        extend: {
            fontFamily: {
                sans: ['Inter', 'sans-serif'],
                display: ['Space Grotesk', 'sans-serif'],
                outfit: ['Outfit', 'sans-serif'],
            },
            colors: {
                // Custom rich black scale for deeper backgrounds
                obsidian: {
                    DEFAULT: '#0f172a', // slate-900 (much lighter than previous black)
                    50: '#f8fafc',
                    100: '#f1f5f9',
                    200: '#e2e8f0',
                    300: '#cbd5e1',
                    400: '#94a3b8',
                    800: '#1e293b', // slate-800
                    900: '#0f172a', // slate-900 (main bg base)
                    950: '#020617', // slate-950 (deepest, but not pure black)
                },
                primary: {
                    DEFAULT: '#06b6d4', // cyan-500 equivalent
                    glow: 'rgba(6, 182, 212, 0.5)',
                }
            },
            backgroundImage: {
                'gradient-radial': 'radial-gradient(var(--tw-gradient-stops))',
                'glass-gradient': 'linear-gradient(135deg, rgba(255, 255, 255, 0.03) 0%, rgba(255, 255, 255, 0.01) 100%)',
                'shine': 'linear-gradient(45deg, transparent 25%, rgba(255,255,255,0.1) 50%, transparent 75%)',
            },
            animation: {
                'pulse-slow': 'pulse 4s cubic-bezier(0.4, 0, 0.6, 1) infinite',
                'fade-in': 'fadeIn 0.6s ease-out forwards',
                'fade-in-up': 'fadeInUp 0.6s cubic-bezier(0.2, 0.8, 0.2, 1) forwards',
                'scale-in': 'scaleIn 0.4s cubic-bezier(0.2, 0.8, 0.2, 1) forwards',
                'float': 'float 6s ease-in-out infinite',
                'slide-in-right': 'slideInRight 0.4s cubic-bezier(0.2, 0.8, 0.2, 1) forwards',
                'shimmer': 'shimmer 2s linear infinite',
            },
            keyframes: {
                fadeIn: {
                    '0%': { opacity: '0' },
                    '100%': { opacity: '1' },
                },
                fadeInUp: {
                    '0%': { opacity: '0', transform: 'translateY(20px) scale(0.98)' },
                    '100%': { opacity: '1', transform: 'translateY(0) scale(1)' },
                },
                scaleIn: {
                    '0%': { opacity: '0', transform: 'scale(0.95)' },
                    '100%': { opacity: '1', transform: 'scale(1)' },
                },
                float: {
                    '0%, 100%': { transform: 'translateY(0)' },
                    '50%': { transform: 'translateY(-15px)' },
                },
                slideInRight: {
                    '0%': { transform: 'translateX(100%)', opacity: '0' },
                    '100%': { transform: 'translateX(0)', opacity: '1' },
                },
                shimmer: {
                    '0%': { backgroundPosition: '200% 0' },
                    '100%': { backgroundPosition: '-200% 0' },
                }
            },
            boxShadow: {
                'glow-sm': '0 0 10px rgba(6, 182, 212, 0.15)',
                'glow': '0 0 20px rgba(6, 182, 212, 0.25)',
                'glow-lg': '0 0 40px rgba(6, 182, 212, 0.4)',
            }
        },
    },
    plugins: [],
}
