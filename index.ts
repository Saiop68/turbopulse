import * as path from 'path';
import { TurboPulse } from './src_ts/engine';
import { EngineConfig, EngineMode } from './src_ts/types';

import * as fs from 'fs';

// Dynamically locate and load the native .node binary
let nativeBinding: any;

try {
  const rootIndex = require('./index.js');
  if (rootIndex && (rootIndex.NativeEngine || rootIndex.nativeBinding)) {
    nativeBinding = rootIndex.nativeBinding || rootIndex;
  }
} catch {}

if (!nativeBinding) {
  const candidateDirs = [
    __dirname,
    path.resolve(__dirname, '..'),
    path.resolve(__dirname, 'build', 'Release'),
    process.cwd(),
  ];

  for (const dir of candidateDirs) {
    try {
      if (fs.existsSync(dir)) {
        const files = fs.readdirSync(dir);
        for (const file of files) {
          if (file.startsWith('turbopulse') && file.endsWith('.node')) {
            try {
              const loaded = require(path.join(dir, file));
              if (loaded && (loaded.NativeEngine || loaded.NativeRateLimiter)) {
                nativeBinding = loaded;
                break;
              }
            } catch {}
          }
        }
      }
    } catch {}
    if (nativeBinding) break;
  }
}

if (!nativeBinding) {
  try {
    nativeBinding = require('./turbopulse.node');
  } catch (err: any) {
    throw new Error(
      `Could not load native turbopulse binary. Details: ${err.message}`
    );
  }
}

export * from './src_ts';

/**
 * Initializes a TurboPulse instance asynchronously (supports InMemory and Redis modes)
 */
export async function createTurboPulse(
  config: EngineConfig = { mode: EngineMode.InMemory }
): Promise<TurboPulse> {
  return TurboPulse.create(nativeBinding.NativeEngine, config);
}

/**
 * Initializes a TurboPulse instance synchronously for local In-Memory mode
 */
export function createInMemoryTurboPulse(): TurboPulse {
  return TurboPulse.createInMemory(nativeBinding.NativeEngine);
}

export { nativeBinding };
