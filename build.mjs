import { build } from 'esbuild';
import { mkdir, rm } from 'fs/promises';
import { dirname, resolve } from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const outdir = resolve(__dirname, 'dist');

await rm(outdir, { recursive: true, force: true });
await mkdir(outdir, { recursive: true });

await build({
  entryPoints: [resolve(__dirname, 'src', 'worker.js')],
  outfile: resolve(outdir, 'worker.js'),
  bundle: true,
  format: 'esm',
  target: 'es2022',
  platform: 'neutral',
  sourcemap: true,
  minify: true,
});

console.log('✓ Build completed: dist/worker.js');
