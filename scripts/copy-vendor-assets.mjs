import { cp, mkdir } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const repoRoot = path.resolve(__dirname, '..');
const sourceDir = path.join(repoRoot, 'nodes', 'ServiceFusion', 'vendor');
const targetDir = path.join(repoRoot, 'dist', 'nodes', 'ServiceFusion', 'vendor');

await mkdir(targetDir, { recursive: true });
await cp(sourceDir, targetDir, { recursive: true });
