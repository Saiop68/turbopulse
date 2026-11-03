import * as path from 'path';
import { TurboPulse } from './src_ts/engine';
import { EngineConfig, EngineMode } from './src_ts/types';

// Dynamically locate and load the native .node binary
let nativeBinding: any;

const candidatePaths = [
  path.resolve(__dirname, 'turbopulse.node'),
  path.resolve(__dirname, '..', 'turbopulse.node'),
  path.resolve(__dirname, 'build', 'Release', 'turbopulse.node'),
  path.resolve(process.cwd(), 'turbopulse.node'),
];

for (const candidate of candidatePaths) {
  try {
    nativeBinding = require(candidate);
    break;
  } catch {
    // continue
  }
}

if (!nativeBinding) {
  try {
    nativeBinding = require('./turbopulse.node');
  } catch (err: any) {
    throw new Error(
      `Could not load native turbopulse binary. Searched paths: ${candidatePaths.join(', ')}. Details: ${err.message}`
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
