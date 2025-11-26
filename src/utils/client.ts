import { buildClient, LogLevel } from '@datocms/cma-client-browser';
import type { CMAClient } from '../types';

/**
 * Creates a CMA client using the current user's access token
 */
export function createClient(apiToken: string): CMAClient {
  return buildClient({
    apiToken,
    // Use default rate limiting
    autoRetry: true,
    // Log requests in development
    logLevel: (import.meta.env.DEV ? 'BODY' : 'NONE') as unknown as LogLevel,
  });
}

/**
 * Helper to add delay between API calls for rate limiting
 */
export function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Process items in batches with delay between batches
 */
export async function processBatch<T, R>(
  items: T[],
  batchSize: number,
  processor: (item: T) => Promise<R>,
  delayMs: number = 100
): Promise<R[]> {
  const results: R[] = [];

  for (let i = 0; i < items.length; i += batchSize) {
    const batch = items.slice(i, i + batchSize);
    const batchResults = await Promise.all(batch.map(processor));
    results.push(...batchResults);

    // Add delay between batches to avoid rate limiting
    if (i + batchSize < items.length) {
      await delay(delayMs);
    }
  }

  return results;
}

