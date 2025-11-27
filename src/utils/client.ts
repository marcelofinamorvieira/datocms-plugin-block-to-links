/**
 * DatoCMS CMA Client Utilities
 * 
 * Provides functions for creating and working with the DatoCMS
 * Content Management API (CMA) client, including rate limiting helpers.
 * 
 * @module utils/client
 */

import { buildClient, LogLevel } from '@datocms/cma-client-browser';
import type { CMAClient } from '../types';

// =============================================================================
// Client Creation
// =============================================================================

/**
 * Creates a DatoCMS CMA client configured for browser use.
 * 
 * Features:
 * - Automatic retry on rate limit errors
 * - Verbose logging in development mode
 * 
 * @param apiToken - The user's API access token
 * @returns Configured CMA client instance
 * 
 * @example
 * const client = createClient(ctx.currentUserAccessToken);
 * const models = await client.itemTypes.list();
 */
export function createClient(apiToken: string): CMAClient {
  return buildClient({
    apiToken,
    autoRetry: true,
    logLevel: (import.meta.env.DEV ? 'BODY' : 'NONE') as unknown as LogLevel,
  });
}

// =============================================================================
// Rate Limiting Helpers
// =============================================================================

/**
 * Creates a promise that resolves after a specified delay.
 * Useful for rate limiting API calls.
 * 
 * @param ms - Delay duration in milliseconds
 * @returns Promise that resolves after the delay
 */
export function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Processes an array of items in batches with a delay between batches.
 * Helps avoid rate limiting when making many API calls.
 * 
 * @param items - Array of items to process
 * @param batchSize - Number of items to process in parallel per batch
 * @param processor - Async function to process each item
 * @param delayMs - Delay between batches in milliseconds (default: 100)
 * @returns Array of results from all processed items
 * 
 * @example
 * const results = await processBatch(
 *   blockInstances,
 *   10,
 *   async (block) => await client.items.create({ ...block }),
 *   200
 * );
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

