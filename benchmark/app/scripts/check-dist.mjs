// Fails the build when vite emitted no .woff2 files. tokens.css loads Jost and DM Mono through
// url(../fonts/…); if that path ever stops resolving, vite only warns and the app quietly falls
// back to system fonts. This check turns that into a build error (RFC 027 §4, §11.5).
import { readdirSync, existsSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const assets = join(dirname(fileURLToPath(import.meta.url)), '..', 'dist', 'assets');
if (!existsSync(assets)) {
  console.error(`check-dist: ${assets} does not exist. Run vite build first.`);
  process.exit(1);
}
const fonts = readdirSync(assets).filter((f) => f.endsWith('.woff2'));
if (fonts.length === 0) {
  console.error('check-dist: dist/assets holds no .woff2 files. The brand fonts were not emitted.');
  process.exit(1);
}
console.log(`check-dist: ${fonts.length} font files in dist/assets`);
