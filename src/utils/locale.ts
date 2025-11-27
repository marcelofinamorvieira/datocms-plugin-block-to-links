/**
 * Locale Handling Utilities
 * 
 * Utilities for working with localized field values in DatoCMS.
 * Handles wrapping, unwrapping, and ensuring complete locale coverage
 * for field values during migration operations.
 * 
 * @module utils/locale
 */

// =============================================================================
// Types
// =============================================================================

/** A localized field value - object with locale codes as keys */
export type LocalizedValue<T = unknown> = Record<string, T>;

/** Options for locale processing functions */
export interface LocaleProcessingOptions {
  /** List of all available locales in the project */
  availableLocales: string[];
  /** Fallback locale to use when a locale has no value (defaults to 'en' or first available) */
  fallbackLocale?: string;
}

// =============================================================================
// Locale Value Wrapping
// =============================================================================

/**
 * Wraps non-localized field values in a localized hash.
 * Duplicates the value across all specified locales.
 * 
 * This is used when a model was created with localized fields but the
 * source data is non-localized (e.g., converting a block from a
 * non-localized context to a model with localized fields).
 * 
 * @param data - The non-localized field data to wrap
 * @param availableLocales - List of all locales to include
 * @param sanitizeFn - Optional function to sanitize values before wrapping
 * @returns Object with field keys mapping to localized value hashes
 * 
 * @example
 * const wrapped = wrapFieldsInLocalizedHash(
 *   { title: 'Hello', count: 5 },
 *   ['en', 'es', 'fr']
 * );
 * // Result: { title: { en: 'Hello', es: 'Hello', fr: 'Hello' }, count: { en: 5, es: 5, fr: 5 } }
 */
export function wrapFieldsInLocalizedHash(
  data: Record<string, unknown>,
  availableLocales: string[],
  sanitizeFn?: (value: unknown) => unknown
): Record<string, LocalizedValue> {
  const result: Record<string, LocalizedValue> = {};

  for (const [fieldKey, value] of Object.entries(data)) {
    // Create a localized hash with the same value for all locales
    const localizedValue: LocalizedValue = {};
    
    for (const locale of availableLocales) {
      // Deep clone arrays and objects to avoid reference issues
      if (Array.isArray(value)) {
        localizedValue[locale] = value.map((item) => {
          if (item && typeof item === 'object') {
            return sanitizeFn ? sanitizeFn({ ...item }) : { ...item };
          }
          return item;
        });
      } else if (value && typeof value === 'object') {
        localizedValue[locale] = sanitizeFn ? sanitizeFn({ ...value }) : { ...value };
      } else {
        localizedValue[locale] = value;
      }
    }
    
    result[fieldKey] = localizedValue;
  }

  return result;
}

// =============================================================================
// Locale Completeness
// =============================================================================

/**
 * Ensures all available locales are present in a localized value.
 * Missing locales are filled with null or a fallback value.
 * 
 * This is critical when updating records to avoid DatoCMS interpreting
 * missing locales as "removed" locales.
 * 
 * @param localizedValue - The localized value to complete
 * @param availableLocales - List of all locales that should be present
 * @param fallbackValue - Value to use for missing locales (default: null)
 * @returns Localized value with all locales present
 * 
 * @example
 * const complete = ensureAllLocalesPresent(
 *   { en: 'Hello' },
 *   ['en', 'es', 'fr']
 * );
 * // Result: { en: 'Hello', es: null, fr: null }
 */
export function ensureAllLocalesPresent<T>(
  localizedValue: LocalizedValue<T>,
  availableLocales: string[],
  fallbackValue: T | null = null
): LocalizedValue<T | null> {
  const result: LocalizedValue<T | null> = {};
  
  for (const locale of availableLocales) {
    if (locale in localizedValue) {
      result[locale] = localizedValue[locale];
    } else {
      result[locale] = fallbackValue;
    }
  }
  
  return result;
}

/**
 * Ensures all locales are present in an update object for a localized field.
 * Uses original values for locales that weren't updated.
 * 
 * @param newValue - The new localized value (may have missing locales)
 * @param originalValue - The original localized value to fall back to
 * @param availableLocales - List of all locales that should be present
 * @returns Complete localized value suitable for API update
 */
export function completeLocalizedUpdate<T>(
  newValue: LocalizedValue<T>,
  originalValue: LocalizedValue<T> | undefined,
  availableLocales: string[]
): LocalizedValue<T | null> {
  const result: LocalizedValue<T | null> = {};
  
  for (const locale of availableLocales) {
    if (locale in newValue) {
      result[locale] = newValue[locale];
    } else if (originalValue && locale in originalValue) {
      result[locale] = originalValue[locale];
    } else {
      result[locale] = null;
    }
  }
  
  return result;
}

// =============================================================================
// Locale Processing
// =============================================================================

/**
 * Processes a potentially localized field value.
 * Determines if the value is localized and processes accordingly.
 * 
 * @param fieldValue - The field value (may be localized or not)
 * @param isLocalized - Whether the field is expected to be localized
 * @param processor - Function to process individual values
 * @returns Processed value (maintains localization structure)
 */
export function processLocalizedValue<T, R>(
  fieldValue: T | LocalizedValue<T>,
  isLocalized: boolean,
  processor: (value: T, locale: string | null) => R
): R | LocalizedValue<R> {
  if (isLocalized && typeof fieldValue === 'object' && fieldValue !== null && !Array.isArray(fieldValue)) {
    // Localized field - process each locale
    const localizedResult: LocalizedValue<R> = {};
    
    for (const [locale, localeValue] of Object.entries(fieldValue as LocalizedValue<T>)) {
      localizedResult[locale] = processor(localeValue, locale);
    }
    
    return localizedResult;
  }
  
  // Non-localized field
  return processor(fieldValue as T, null);
}

/**
 * Merges locale data from multiple sources into a single localized value.
 * Used when combining block data from different locales into one record.
 * 
 * @param localeData - Object mapping locale codes to field data
 * @param fieldKeys - Set of all field keys to include
 * @param availableLocales - List of all locales to include
 * @param fallbackLocale - Locale to use as fallback for missing data
 * @returns Merged localized field data
 */
export function mergeLocaleData(
  localeData: Record<string, Record<string, unknown>>,
  fieldKeys: Set<string>,
  availableLocales: string[],
  fallbackLocale?: string
): Record<string, LocalizedValue> {
  const result: Record<string, LocalizedValue> = {};
  
  // Determine fallback locale
  const localesWithData = Object.keys(localeData).filter(k => k !== '__default__');
  const effectiveFallback = fallbackLocale || 
    (localesWithData.includes('en') ? 'en' : localesWithData[0]);
  const fallbackData = effectiveFallback ? localeData[effectiveFallback] : null;
  
  // Check for __default__ data (non-localized context marked as localized)
  const defaultData = localeData['__default__'] || null;
  
  for (const fieldKey of fieldKeys) {
    const localizedValue: LocalizedValue = {};
    
    for (const locale of availableLocales) {
      // Try to get value from this locale's data
      let localeBlockData = localeData[locale];
      
      // If no locale-specific data, fall back to __default__ data
      if (!localeBlockData && defaultData) {
        localeBlockData = defaultData;
      }
      
      if (localeBlockData && localeBlockData[fieldKey] !== undefined) {
        localizedValue[locale] = localeBlockData[fieldKey];
      } else if (fallbackData && fallbackData[fieldKey] !== undefined) {
        // For missing locales, use the fallback locale's value
        // This prevents 422 errors when fields have required validators
        localizedValue[locale] = fallbackData[fieldKey];
      } else {
        // Last resort: set to null
        localizedValue[locale] = null;
      }
    }
    
    result[fieldKey] = localizedValue;
  }
  
  return result;
}

/**
 * Determines if a field value looks like a localized value.
 * A localized value is an object where keys are locale codes.
 * 
 * @param value - The value to check
 * @param isExpectedLocalized - Whether the field is expected to be localized
 * @returns True if the value appears to be localized
 */
export function isLocalizedValue(
  value: unknown,
  isExpectedLocalized: boolean = true
): value is LocalizedValue {
  if (!isExpectedLocalized) return false;
  if (!value || typeof value !== 'object' || Array.isArray(value)) return false;
  
  // Check if it looks like a locale hash (object with string keys, not a special object)
  const obj = value as Record<string, unknown>;
  
  // Exclude objects that look like blocks or structured text
  if ('type' in obj || 'document' in obj || 'blocks' in obj || 
      'relationships' in obj || '__itemTypeId' in obj || 'item_type' in obj) {
    return false;
  }
  
  return true;
}

