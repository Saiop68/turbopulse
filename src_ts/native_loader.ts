import * as path from 'path';

let cachedBinding: any = null;

export function getNativeBinding(): any {
  if (cachedBinding) {
    return cachedBinding;
  }

  const candidatePaths = [
    path.resolve(__dirname, 'turbopulse.node'),
    path.resolve(__dirname, '..', 'turbopulse.node'),
    path.resolve(__dirname, '..', '..', 'turbopulse.node'),
    path.resolve(process.cwd(), 'turbopulse.node'),
  ];

  for (const candidate of candidatePaths) {
    try {
      cachedBinding = require(candidate);
      return cachedBinding;
    } catch {}
  }

  try {
    cachedBinding = require('./turbopulse.node');
    return cachedBinding;
  } catch (err: any) {
    throw new Error(
      `Failed to load native turbopulse binary. Searched: ${candidatePaths.join(', ')}. Error: ${err.message}`
    );
  }
}

export function getNativeEngine(): any {
  const binding = getNativeBinding();
  return binding.NativeEngine;
}
