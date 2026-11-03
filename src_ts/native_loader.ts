import * as path from 'path';
import * as fs from 'fs';

let cachedBinding: any = null;

export function getNativeBinding(): any {
  if (cachedBinding) {
    return cachedBinding;
  }

  // 1. Try loading through NAPI-RS auto-generated root index.js
  try {
    const rootIndex = require('../index.js');
    if (rootIndex && (rootIndex.NativeEngine || rootIndex.nativeBinding)) {
      cachedBinding = rootIndex.nativeBinding || rootIndex;
      return cachedBinding;
    }
  } catch {}

  // 2. Scan candidate directories for platform-specific turbopulse.*.node
  const candidateDirs = [
    path.resolve(__dirname, '..'),
    path.resolve(__dirname),
    path.resolve(process.cwd()),
    path.resolve(process.cwd(), 'build', 'Release'),
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
                cachedBinding = loaded;
                return cachedBinding;
              }
            } catch {}
          }
        }
      }
    } catch {}
  }

  // 3. Direct candidate paths
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

  throw new Error(
    `Failed to load native turbopulse binary. Searched directories: ${candidateDirs.join(', ')}`
  );
}

export function getNativeEngine(): any {
  const binding = getNativeBinding();
  return binding.NativeEngine;
}
