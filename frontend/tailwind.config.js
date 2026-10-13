/** @type {import('tailwindcss').Config} */
export default {
  content: [
    './index.html',
    './src/**/*.{js,ts,jsx,tsx}',
  ],
  theme: {
    extend: {
      fontFamily: {
        sans: ['Inter', 'ui-sans-serif', 'system-ui', 'sans-serif'],
        mono: ['JetBrains Mono', 'ui-monospace', 'SFMono-Regular', 'monospace'],
      },
      colors: {
        brand: {
          light:   '#a5b4fc', // indigo-300 (WCAG AA compliant on dark background)
          DEFAULT: '#6366f1', // indigo-500
          dark:    '#4f46e5', // indigo-600
          subtle:  'rgba(99, 102, 241, 0.12)',
        },
        surface: {
          base:    '#0f172a', // slate-900
          raised:  '#1e293b', // slate-800
          overlay: '#1e293b',
          border:  '#334155', // slate-700
        },
        status: {
          success: '#34d399',
          warning: '#fbbf24',
          error:   '#fca5a5', // rose-300 (WCAG AA compliant on dark background)
          info:    '#60a5fa',
          neutral: '#94a3b8',
        },
      },
      spacing: {
        sidebar:     '16rem',   // w-64 expanded
        'sidebar-sm': '3.5rem', // w-14 collapsed (icon-only)
        topbar:      '4rem',    // h-16
      },
      transitionDuration: {
        fast:  '120ms',
        base:  '200ms',
        slow:  '350ms',
      },
      keyframes: {
        'fade-in': {
          from: { opacity: '0', transform: 'translateY(4px)' },
          to:   { opacity: '1', transform: 'translateY(0)' },
        },
        shimmer: {
          '0%, 100%': { opacity: '0.4' },
          '50%':      { opacity: '1' },
        },
      },
      animation: {
        'fade-in': 'fade-in 200ms ease-out both',
        shimmer:   'shimmer 1.5s ease-in-out infinite',
      },
    },
  },
  plugins: [],
};
