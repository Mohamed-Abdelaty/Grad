import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { execFileSync } from 'child_process';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

export default async function globalSetup() {
  const rootDir = path.resolve(__dirname, '..', '..');
  const generatorScript = path.resolve(rootDir, 'scripts', 'generate_sales_data.py');

  const datasets = [
    {
      file: path.resolve(rootDir, 'backend', 'sample-data', 'sales_large.csv'),
      expectedMinBytes: 55_000_000,
      rows: 1_000_000,
      relativeName: 'backend/sample-data/sales_large.csv',
    },
    {
      file: path.resolve(rootDir, 'backend', 'sample-data', 'sales_300k.csv'),
      expectedMinBytes: 15_000_000,
      rows: 300_000,
      relativeName: 'backend/sample-data/sales_300k.csv',
    },
  ];

  for (const ds of datasets) {
    let exists = false;
    let size = 0;
    try {
      const stat = fs.statSync(ds.file);
      exists = true;
      size = stat.size;
    } catch {
      exists = false;
    }

    if (exists && size >= ds.expectedMinBytes) {
      console.log(`[global-setup] ${ds.relativeName}: present (${size} bytes)`);
    } else {
      let pyCmd: string | null = null;
      for (const cmd of ['python', 'py', 'python3']) {
        try {
          execFileSync(cmd, ['--version'], { stdio: 'ignore' });
          pyCmd = cmd;
          break;
        } catch {
          // ignore
        }
      }

      if (!pyCmd) {
        throw new Error(
          `Python is missing. Run this command manually to generate the dataset:\npython scripts/generate_sales_data.py --rows ${ds.rows} --out ${ds.file}`
        );
      }

      execFileSync(
        pyCmd,
        [generatorScript, '--rows', String(ds.rows), '--out', ds.file],
        { timeout: 180_000, stdio: 'inherit' }
      );

      const newStat = fs.statSync(ds.file);
      console.log(`[global-setup] ${ds.relativeName}: generated (${newStat.size} bytes)`);
    }
  }
}
