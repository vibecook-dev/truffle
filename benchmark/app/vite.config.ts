import { readdirSync, readFileSync, statSync } from 'node:fs';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { defineConfig, type Plugin } from 'vite';
import react from '@vitejs/plugin-react';

/**
 * Dev server only: `GET /__dev-reports` answers every `*.json` report under
 * benchmark/app/dev-reports/ (gitignored) as one array, for the browser mock's `?real=1`, which
 * then serves real engine reports to History, Run detail, Compare and Overview; `?dir=<name>`
 * (the mock's `?real_dir=`) serves one subfolder's only. It is a
 * `configureServer` hook (`apply: 'serve'`), so a build never sees the folder.
 */
function devReports(): Plugin {
  const root = fileURLToPath(new URL('./dev-reports', import.meta.url));
  const walk = (dir: string): string[] =>
    readdirSync(dir).flatMap((name) => {
      const p = join(dir, name);
      return statSync(p).isDirectory() ? walk(p) : name.endsWith('.json') ? [p] : [];
    });
  return {
    name: 'truffle-bench-dev-reports',
    apply: 'serve',
    configureServer(server) {
      server.middlewares.use('/__dev-reports', (req, res) => {
        let reports: unknown[] = [];
        try {
          // ?dir=<subfolder>: one folder's reports only (a plain name, never a path out)
          const dir = new URL(req.url ?? '/', 'http://x').searchParams.get('dir');
          const from = dir && /^[\w.-]+$/.test(dir) && dir !== '..' ? join(root, dir) : root;
          reports = walk(from).map((f) => JSON.parse(readFileSync(f, 'utf8')) as unknown);
        } catch {
          // no folder, or a file that isn't JSON: serve what there is
        }
        res.setHeader('content-type', 'application/json');
        res.end(JSON.stringify(reports));
      });
    },
  };
}

// The Tauri shell loads this server in development (tauri.conf.json build.devUrl) and the
// built `dist/` in a bundle. Fonts must be emitted as files, never inlined: the app's CSP is
// `font-src 'self'`, so a data: font would be blocked.
//
// The dev server binds 127.0.0.1:47183 with a strict port (devUrl http://127.0.0.1:47183), not
// the RFC's localhost:5173: other projects' Vite servers hold 5173 and 5183 on this machine, and a
// strict port on `localhost` then quietly binds [::1] instead, so the app could load the wrong
// page. 127.0.0.1 makes a clash fail loudly.
export default defineConfig({
  plugins: [react(), devReports()],
  clearScreen: false,
  envPrefix: ['VITE_', 'TAURI_ENV_'],
  server: {
    host: '127.0.0.1',
    port: 47183,
    strictPort: true,
    watch: { ignored: ['**/src-tauri/**', '**/dev-reports/**'] },
  },
  build: {
    outDir: 'dist',
    target: 'safari16',
    assetsInlineLimit: 0,
    sourcemap: false,
  },
});
