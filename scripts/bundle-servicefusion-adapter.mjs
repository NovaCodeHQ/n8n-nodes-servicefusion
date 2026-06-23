import { mkdir, readFile, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { createRequire } from 'node:module';
import { build } from 'esbuild';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const repoRoot = path.resolve(__dirname, '..');
const vendorDir = path.join(repoRoot, 'nodes', 'ServiceFusion', 'vendor');
const require = createRequire(import.meta.url);

const adapterEntry = require.resolve('@pmip/servicefusion-adapter');
const adapterLicensePath = path.join(path.dirname(adapterEntry), '..', 'LICENSE');
const bundlePath = path.join(vendorDir, 'servicefusion-adapter.bundle.js');
const licenseTargetPath = path.join(vendorDir, 'servicefusion-adapter.LICENSE');

await mkdir(vendorDir, { recursive: true });

await build({
	entryPoints: [adapterEntry],
	outfile: bundlePath,
	bundle: true,
	platform: 'node',
	format: 'cjs',
	target: ['node18'],
	legalComments: 'inline',
});

const licenseText = await readFile(adapterLicensePath, 'utf8');
await writeFile(
	licenseTargetPath,
	[
		'Bundled third-party component: @pmip/servicefusion-adapter',
		'Source repository: https://github.com/rashidazarang/servicefusion-adapter',
		'',
		licenseText,
	].join('\n'),
);
