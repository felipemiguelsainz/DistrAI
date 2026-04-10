/** @type {import('tailwindcss').Config} */
export default {
  content: ['./index.html', './src/**/*.{js,jsx}'],
  theme: {
    extend: {
      fontFamily: {
        mono: ['"Space Mono"', 'ui-monospace', 'SFMono-Regular', 'monospace'],
        sans: ['"Space Mono"', 'ui-monospace', 'SFMono-Regular', 'monospace'],
      },
      colors: {
        nothing: {
          red:    '#ff3c00',
          bg:     '#080808',
          card:   '#0f0f0f',
          border: '#1f1f1f',
          muted:  '#3a3a3a',
          dim:    '#6b6b6b',
          light:  '#a8a8a8',
          white:  '#f0f0f0',
        },
      },
      borderRadius: {
        DEFAULT: '0px',
        sm:  '0px',
        md:  '0px',
        lg:  '2px',
        xl:  '2px',
        '2xl': '2px',
        full: '9999px',
      },
      animation: {
        'blink': 'blink 1s step-end infinite',
        'fade-in': 'fadeIn 0.15s ease-out both',
        'scan': 'scan 3s linear infinite',
      },
      keyframes: {
        blink: {
          '0%, 100%': { opacity: '1' },
          '50%':      { opacity: '0' },
        },
        fadeIn: {
          from: { opacity: '0', transform: 'translateY(4px)' },
          to:   { opacity: '1', transform: 'translateY(0)' },
        },
        scan: {
          from: { transform: 'translateY(-100%)' },
          to:   { transform: 'translateY(100vh)' },
        },
      },
    },
  },
  plugins: [],
};
