import type {
  CMAClient,
  BlockAnalysis,
  ConversionResult,
  ProgressCallback,
  BlockMigrationMapping,
  ModularContentFieldInfo,
  NestedBlockPath,
  DebugOptions,
  GroupedBlockInstance,
} from '../types';
import { DEFAULT_DEBUG_OPTIONS } from '../types';
import {
  analyzeBlock,
  buildNestedPathsToRootModels,
  getAllBlockInstancesNested,
  getGroupedBlockInstances,
  getBlockTypeId,
  getBlockId,
} from './analyzer';
import { delay, processBatch } from './client';
import {
  isStructuredTextValue,
  transformDastBlocksToLinks,
  extractLinksFromStructuredText,
} from './dast';

/**
 * Debug logging utility - only logs when debug mode is enabled
 */
function debugLog(debug: DebugOptions, category: string, message: string, data?: unknown): void {
  if (!debug.enabled || !debug.verboseLogging) return;
  
  const timestamp = new Date().toISOString().split('T')[1].slice(0, 12);
  const prefix = `[DEBUG ${timestamp}] [${category}]`;
  
  if (data !== undefined) {
    console.log(`${prefix} ${message}`, data);
  } else {
    console.log(`${prefix} ${message}`);
  }
}

/**
 * Logs the start of a major operation
 */
function debugLogSection(debug: DebugOptions, title: string): void {
  if (!debug.enabled || !debug.verboseLogging) return;
  console.log(`\n${'='.repeat(60)}`);
  console.log(`[DEBUG] ${title}`);
  console.log(`${'='.repeat(60)}`);
}

/**
 * Logs a sub-section within an operation
 */
function debugLogSubSection(debug: DebugOptions, title: string): void {
  if (!debug.enabled || !debug.verboseLogging) return;
  console.log(`\n${'-'.repeat(40)}`);
  console.log(`[DEBUG] ${title}`);
  console.log(`${'-'.repeat(40)}`);
}

/**
 * Recursively sanitizes block data to remove properties that shouldn't be included
 * when creating new records (like `id`, `item_type`, `relationships`, etc.)
 */
function sanitizeBlockDataForCreation(data: unknown): unknown {
  if (data === null || data === undefined) {
    return data;
  }

  if (Array.isArray(data)) {
    return data.map(sanitizeBlockDataForCreation);
  }

  if (typeof data === 'object') {
    const obj = data as Record<string, unknown>;
    
    // Check if this looks like a block object (has item_type or relationships.item_type)
    const isBlock = 
      obj.__itemTypeId !== undefined ||
      obj.item_type !== undefined ||
      (obj.relationships && typeof obj.relationships === 'object' && 
        (obj.relationships as Record<string, unknown>).item_type !== undefined);

    if (isBlock) {
      // This is a block - remove id and other read-only properties, but keep the block type and attributes
      const sanitized: Record<string, unknown> = {};
      
      // Get the item_type ID
      let itemTypeId: string | undefined;
      if (typeof obj.__itemTypeId === 'string') {
        itemTypeId = obj.__itemTypeId;
      } else if (obj.relationships) {
        const rel = obj.relationships as Record<string, unknown>;
        const itemTypeRel = rel.item_type as Record<string, unknown> | undefined;
        if (itemTypeRel?.data) {
          const data = itemTypeRel.data as Record<string, unknown>;
          itemTypeId = data.id as string;
        }
      } else if (typeof obj.item_type === 'string') {
        itemTypeId = obj.item_type;
      } else if (obj.item_type && typeof obj.item_type === 'object') {
        itemTypeId = (obj.item_type as Record<string, unknown>).id as string;
      }

      // Set the item_type reference for the new block
      if (itemTypeId) {
        sanitized.type = 'item';
        sanitized.attributes = {};
        sanitized.relationships = {
          item_type: {
            data: {
              type: 'item_type',
              id: itemTypeId,
            },
          },
        };
      }

      // Get the attributes (field values) and sanitize them recursively
      const attributes = obj.attributes as Record<string, unknown> | undefined;
      if (attributes) {
        for (const [key, value] of Object.entries(attributes)) {
          (sanitized.attributes as Record<string, unknown>)[key] = sanitizeBlockDataForCreation(value);
        }
      } else {
        // If there's no attributes wrapper, check for field values directly in the object
        // (this can happen with different CMA client versions)
        const skipKeys = new Set(['id', 'item_type', '__itemTypeId', 'relationships', 'type', 'meta', 'creator', 'attributes']);
        for (const [key, value] of Object.entries(obj)) {
          if (!skipKeys.has(key)) {
            (sanitized.attributes as Record<string, unknown>)[key] = sanitizeBlockDataForCreation(value);
          }
        }
      }

      return sanitized;
    }

    // Not a block - recursively sanitize nested values
    const sanitized: Record<string, unknown> = {};
    for (const [key, value] of Object.entries(obj)) {
      sanitized[key] = sanitizeBlockDataForCreation(value);
    }
    return sanitized;
  }

  // Primitive value - return as-is
  return data;
}

/**
 * Sanitizes field values for creating a new top-level record.
 * This is simpler than block sanitization - we just need to sanitize any nested blocks.
 */
function sanitizeFieldValuesForCreation(data: Record<string, unknown>): Record<string, unknown> {
  const result: Record<string, unknown> = {};
  
  for (const [key, value] of Object.entries(data)) {
    if (Array.isArray(value)) {
      // Could be an array of blocks (modular content field)
      result[key] = value.map((item) => {
        if (item && typeof item === 'object') {
          const obj = item as Record<string, unknown>;
          // Check if it looks like a block
          const isBlock = 
            obj.__itemTypeId !== undefined ||
            obj.item_type !== undefined ||
            (obj.relationships && typeof obj.relationships === 'object');
          
          if (isBlock) {
            return sanitizeBlockDataForCreation(item);
          }
        }
        return sanitizeBlockDataForCreation(item);
      });
    } else if (value && typeof value === 'object') {
      // Could be a single block, structured text, or other complex value
      const obj = value as Record<string, unknown>;
      
      // Check for structured text (has document and possibly blocks)
      if ('document' in obj || 'blocks' in obj) {
        result[key] = {
          ...obj,
          blocks: obj.blocks ? (obj.blocks as unknown[]).map(b => sanitizeBlockDataForCreation(b)) : undefined,
        };
      } else {
        // Regular object or single block
        result[key] = sanitizeBlockDataForCreation(value);
      }
    } else {
      result[key] = value;
    }
  }
  
  return result;
}

/**
 * Wraps non-localized field values in a localized hash, duplicating the value across all locales.
 * This is used when a model was created with localized fields but the data source is non-localized.
 */
function wrapFieldsInLocalizedHash(
  data: Record<string, unknown>,
  availableLocales: string[]
): Record<string, Record<string, unknown>> {
  const result: Record<string, Record<string, unknown>> = {};

  for (const [fieldKey, value] of Object.entries(data)) {
    // Create a localized hash with the same value for all locales
    const localizedValue: Record<string, unknown> = {};
    for (const locale of availableLocales) {
      // Deep clone arrays and objects to avoid reference issues
      if (Array.isArray(value)) {
        localizedValue[locale] = value.map((item) => {
          if (item && typeof item === 'object') {
            return sanitizeBlockDataForCreation({ ...item });
          }
          return item;
        });
      } else if (value && typeof value === 'object') {
        localizedValue[locale] = sanitizeBlockDataForCreation({ ...value });
      } else {
        localizedValue[locale] = value;
      }
    }
    result[fieldKey] = localizedValue;
  }

  return result;
}

/**
 * Finds the nested path that corresponds to a specific modular content field.
 * Used to get the full path from root model to the field for nested migration.
 */
function findNestedPathForField(
  nestedPaths: NestedBlockPath[],
  mcField: ModularContentFieldInfo
): NestedBlockPath | undefined {
  // Find a path where the last step's fieldApiKey matches and the parent matches
  return nestedPaths.find(path => {
    const lastStep = path.path[path.path.length - 1];
    return lastStep.fieldApiKey === mcField.apiKey && 
           path.fieldInfo.parentModelId === mcField.parentModelId;
  });
}

/**
 * Extracts blocks from a field value based on field type.
 * Handles rich_text (array), single_block (object), and structured_text (object with blocks).
 */
function extractBlocksFromValue(
  fieldValue: unknown,
  fieldType: 'rich_text' | 'structured_text' | 'single_block'
): unknown[] {
  if (!fieldValue) return [];

  if (fieldType === 'rich_text') {
    // Rich text (modular content) - value is directly an array of blocks
    if (Array.isArray(fieldValue)) {
      return fieldValue;
    }
    return [];
  } else if (fieldType === 'structured_text') {
    // Structured text - with nested: true, blocks are inlined in the DAST document children
    // The blocks have type: 'block' and their data is in item: {...}
    if (typeof fieldValue === 'object' && fieldValue !== null) {
      const stValue = fieldValue as Record<string, unknown>;
      
      // First try the traditional blocks array (for backwards compatibility)
      const blocksArray = stValue.blocks;
      if (Array.isArray(blocksArray) && blocksArray.length > 0) {
        return blocksArray;
      }
      
      // With nested: true, blocks are inlined in document.children
      // We need to extract them from the DAST structure
      const document = stValue.document as Record<string, unknown> | undefined;
      if (document && typeof document === 'object') {
        const children = document.children as unknown[] | undefined;
        if (Array.isArray(children)) {
          // Extract block nodes (type === 'block') and return their item (the block data)
          const inlinedBlocks: unknown[] = [];
          for (const child of children) {
            if (child && typeof child === 'object') {
              const childObj = child as Record<string, unknown>;
              if (childObj.type === 'block' && childObj.item !== undefined) {
                // The item contains the actual block data when using nested: true
                inlinedBlocks.push(childObj.item);
              }
            }
          }
          if (inlinedBlocks.length > 0) {
            return inlinedBlocks;
          }
        }
      }
    }
    return [];
  } else if (fieldType === 'single_block') {
    // Single block - value is a single block object (not an array)
    if (typeof fieldValue === 'object' && fieldValue !== null && !Array.isArray(fieldValue)) {
      return [fieldValue]; // Wrap in array for consistent processing
    }
    return [];
  }

  return [];
}

/**
 * Recursively traverses and updates nested block structures.
 * This is the core recursive function that navigates through arbitrary nesting depths.
 * 
 * @param fieldValue - The current field value to process
 * @param path - The path segments to follow
 * @param pathIndex - Current index in the path
 * @param updateFn - Function to call when we reach the target block to update it
 * @param debug - Debug options for logging
 * @returns Updated field value and whether any updates were made
 */
function traverseAndUpdateNestedBlocks(
  fieldValue: unknown,
  path: NestedBlockPath['path'],
  pathIndex: number,
  updateFn: (blockData: Record<string, unknown>, locale: string | null) => Record<string, unknown>,
  debug: DebugOptions = DEFAULT_DEBUG_OPTIONS
): { updated: boolean; newValue: unknown } {
  if (pathIndex >= path.length || !fieldValue) {
    return { updated: false, newValue: fieldValue };
  }

  const currentStep = path[pathIndex];
  const isLastStep = pathIndex === path.length - 1;

  // Helper to process blocks at this level
  const processBlocksArray = (
    blocks: unknown[],
    locale: string | null
  ): { updated: boolean; newBlocks: unknown[] } => {
    let updated = false;
    const newBlocks: unknown[] = [];

    for (const block of blocks) {
      if (!block || typeof block !== 'object') {
        newBlocks.push(block);
        continue;
      }

      const blockObj = block as Record<string, unknown>;
      const blockTypeId = getBlockTypeId(blockObj);

      // Check if this block matches the expected type at this path level
      if (blockTypeId === currentStep.expectedBlockTypeId) {
        if (isLastStep) {
          // This is the target block - apply the update function
          const updatedBlock = updateFn(blockObj, locale);
          newBlocks.push(updatedBlock);
          updated = true;
          debugLog(debug, 'TRAVERSE', `Updated block at final level`, { locale });
        } else {
          // Need to go deeper - get the next field value from this block
          const nextStep = path[pathIndex + 1];
          const nestedFieldValue = getNestedFieldValueFromBlock(blockObj, nextStep.fieldApiKey);

          if (nestedFieldValue !== undefined) {
            // Recurse into the nested field
            const result = traverseAndUpdateNestedBlocks(
              nestedFieldValue,
              path,
              pathIndex + 1,
              updateFn,
              debug
            );

            if (result.updated) {
              // Update the block with the new nested field value
              const updatedBlock = setNestedFieldValueInBlock(
                blockObj,
                nextStep.fieldApiKey,
                result.newValue
              );
              newBlocks.push(updatedBlock);
              updated = true;
            } else {
              newBlocks.push(block);
            }
          } else {
            newBlocks.push(block);
          }
        }
      } else {
        // Block type doesn't match - keep as is
        newBlocks.push(block);
      }
    }

    return { updated, newBlocks };
  };

  // Handle localized vs non-localized fields
  if (currentStep.localized && typeof fieldValue === 'object' && !Array.isArray(fieldValue)) {
    // Localized field - process each locale
    let anyUpdated = false;
    const newLocalizedValue: Record<string, unknown> = {};

    for (const [locale, localeValue] of Object.entries(fieldValue as Record<string, unknown>)) {
      const blocks = extractBlocksFromValue(localeValue, currentStep.fieldType);
      
      if (blocks.length > 0) {
        const result = processBlocksArray(blocks, locale);
        if (result.updated) {
          anyUpdated = true;
          // Reconstruct the value based on field type
          if (currentStep.fieldType === 'single_block') {
            newLocalizedValue[locale] = result.newBlocks[0] || null;
          } else if (currentStep.fieldType === 'structured_text') {
            // For structured text with inlined blocks, update the blocks in the DAST document
            newLocalizedValue[locale] = reconstructStructuredTextWithUpdatedBlocks(
              localeValue as Record<string, unknown>,
              result.newBlocks
            );
          } else {
            newLocalizedValue[locale] = result.newBlocks;
          }
        } else {
          newLocalizedValue[locale] = localeValue;
        }
      } else {
        newLocalizedValue[locale] = localeValue;
      }
    }

    return { updated: anyUpdated, newValue: newLocalizedValue };
  } else {
    // Non-localized field
    const blocks = extractBlocksFromValue(fieldValue, currentStep.fieldType);
    
    if (blocks.length > 0) {
      const result = processBlocksArray(blocks, null);
      if (result.updated) {
        // Reconstruct the value based on field type
        if (currentStep.fieldType === 'single_block') {
          return { updated: true, newValue: result.newBlocks[0] || null };
        } else if (currentStep.fieldType === 'structured_text') {
          // For structured text with inlined blocks, update the blocks in the DAST document
          return {
            updated: true,
            newValue: reconstructStructuredTextWithUpdatedBlocks(
              fieldValue as Record<string, unknown>,
              result.newBlocks
            ),
          };
        } else {
          return { updated: true, newValue: result.newBlocks };
        }
      }
    }

    return { updated: false, newValue: fieldValue };
  }
}

/**
 * Reconstructs a structured text value with updated block data.
 * Handles both the traditional format (blocks in blocks array) and 
 * the nested: true format (blocks inlined in document.children).
 */
function reconstructStructuredTextWithUpdatedBlocks(
  originalValue: Record<string, unknown>,
  updatedBlocks: unknown[]
): Record<string, unknown> {
  const result = { ...originalValue };
  
  // Check if we have inlined blocks in document.children
  const document = originalValue.document as Record<string, unknown> | undefined;
  if (document && typeof document === 'object') {
    const children = document.children as unknown[] | undefined;
    if (Array.isArray(children)) {
      // Find block nodes in children and check if they match the structure of inlined blocks
      const hasInlinedBlocks = children.some(child => {
        if (child && typeof child === 'object') {
          const childObj = child as Record<string, unknown>;
          return childObj.type === 'block' && childObj.item !== undefined && typeof childObj.item === 'object';
        }
        return false;
      });
      
      if (hasInlinedBlocks) {
        // Update inlined blocks in document.children
        let blockIndex = 0;
        const newChildren = children.map(child => {
          if (child && typeof child === 'object') {
            const childObj = child as Record<string, unknown>;
            if (childObj.type === 'block' && childObj.item !== undefined) {
              // Replace with updated block data
              if (blockIndex < updatedBlocks.length) {
                const updatedChild = {
                  ...childObj,
                  item: updatedBlocks[blockIndex],
                };
                blockIndex++;
                return updatedChild;
              }
            }
          }
          return child;
        });
        
        result.document = {
          ...document,
          children: newChildren,
        };
        return result;
      }
    }
  }
  
  // Fallback: use traditional blocks array
  result.blocks = updatedBlocks;
  return result;
}

/**
 * Gets a field value from a block object, handling both direct properties and attributes
 */
function getNestedFieldValueFromBlock(block: Record<string, unknown>, fieldApiKey: string): unknown {
  // Check directly on block first
  if (block[fieldApiKey] !== undefined) {
    return block[fieldApiKey];
  }
  // Check in attributes (CMA client may return block data in attributes)
  const attributes = block.attributes as Record<string, unknown> | undefined;
  if (attributes && attributes[fieldApiKey] !== undefined) {
    return attributes[fieldApiKey];
  }
  return undefined;
}

/**
 * Sets a field value in a block object, handling both direct properties and attributes
 */
function setNestedFieldValueInBlock(
  block: Record<string, unknown>,
  fieldApiKey: string,
  value: unknown
): Record<string, unknown> {
  const clonedBlock = JSON.parse(JSON.stringify(block));

  // Check if field exists at top level or in attributes
  if (block[fieldApiKey] !== undefined) {
    clonedBlock[fieldApiKey] = value;
  } else {
    const attributes = block.attributes as Record<string, unknown> | undefined;
    if (attributes && attributes[fieldApiKey] !== undefined) {
      clonedBlock.attributes = { ...attributes, [fieldApiKey]: value };
    } else {
      // Default to setting at top level
      clonedBlock[fieldApiKey] = value;
    }
  }

  return clonedBlock;
}

/**
 * Migrates data for fields inside nested blocks.
 * This function queries records from the ROOT model and navigates into nested block structures.
 */
async function migrateNestedBlockFieldData(
  client: CMAClient,
  nestedPath: NestedBlockPath,
  oldFieldApiKey: string,
  newFieldApiKey: string,
  targetBlockId: string,
  mapping: BlockMigrationMapping,
  isSingleValue: boolean,
  availableLocales: string[],
  debug: DebugOptions = DEFAULT_DEBUG_OPTIONS
): Promise<void> {
  debugLog(debug, 'NESTED_MIGRATE', `Migrating nested field data`, {
    rootModel: nestedPath.rootModelName,
    path: nestedPath.path.map(p => p.fieldApiKey).join(' → '),
    oldField: oldFieldApiKey,
    newField: newFieldApiKey,
  });

  let recordCount = 0;
  let updatedCount = 0;

  // The path goes from root model TO the target blocks (e.g., sections → socials → Social Media Icons)
  // But we need to update the PARENT block (e.g., Hero Section) with the new link field
  // So we use a path that stops one level before the final block level
  
  // For a single-step path (field is directly in a root block), we update at that level
  // For multi-step paths, we need to stop at the parent block level
  const pathToParentBlock = nestedPath.path.slice(0, -1);
  const fieldStep = nestedPath.path[nestedPath.path.length - 1];
  
  debugLog(debug, 'NESTED_MIGRATE', `Path to parent: ${pathToParentBlock.map(p => p.fieldApiKey).join(' → ') || '(root)'}`);
  debugLog(debug, 'NESTED_MIGRATE', `Field step: ${fieldStep.fieldApiKey} → ${newFieldApiKey}`);

  // Query records from the ROOT model (not the block)
  for await (const record of client.items.listPagedIterator({
    filter: { type: nestedPath.rootModelId },
    nested: true,
  })) {
    recordCount++;

    // Get the root field value (first step in the path)
    const rootFieldApiKey = nestedPath.path[0].fieldApiKey;
    const rootFieldValue = record[rootFieldApiKey];

    if (!rootFieldValue) {
      debugLog(debug, 'NESTED_MIGRATE', `Record ${record.id}: No value in root field "${rootFieldApiKey}", skipping`);
      continue;
    }

    // Create the update function that will be called for PARENT blocks
    // This function reads the old field, extracts block IDs, maps them to record IDs, and sets the new field
    const updateBlockFn = (blockData: Record<string, unknown>, locale: string | null): Record<string, unknown> => {
      // Get the old field value from the parent block (e.g., Hero Section's "socials" field)
      const oldValue = getNestedFieldValueFromBlock(blockData, oldFieldApiKey);
      
      if (!oldValue) {
        debugLog(debug, 'NESTED_MIGRATE', `No value in field "${oldFieldApiKey}", skipping`);
        return blockData;
      }

      // Extract link IDs from the old value (the array of blocks in the old field)
      const newValue = extractLinksFromValue(oldValue, targetBlockId, mapping, isSingleValue);

      debugLog(debug, 'NESTED_MIGRATE', `Extracted links for parent block field "${oldFieldApiKey}"`, { 
        oldValueType: typeof oldValue,
        oldValueIsArray: Array.isArray(oldValue),
        newValue,
        locale 
      });

      // Set the new links field value in the parent block
      return setNestedFieldValueInBlock(blockData, newFieldApiKey, newValue);
    };

    let result: { updated: boolean; newValue: unknown };

    if (pathToParentBlock.length === 0) {
      // Single-step path: the field is directly in a block at the root level
      // We need to process the blocks in the root field
      result = traverseAndUpdateNestedBlocksAtLevel(
        rootFieldValue,
        nestedPath.path[0], // The first (and only) step - contains the parent blocks
        updateBlockFn,
        debug
      );
    } else {
      // Multi-step path: we need to traverse to the parent block level first
      result = traverseAndUpdateNestedBlocks(
        rootFieldValue,
        pathToParentBlock,
        0,
        updateBlockFn,
        debug
      );
    }

    if (result.updated) {
      try {
        // Ensure all locales are present in the update to avoid INVALID_LOCALES errors
        let updateValue = result.newValue;
        
        // If the root field is localized, ensure all locales are present
        if (nestedPath.path[0].localized && typeof result.newValue === 'object' && !Array.isArray(result.newValue)) {
          const localizedValue = result.newValue as Record<string, unknown>;
          const completeLocalizedValue: Record<string, unknown> = {};
          
          for (const locale of availableLocales) {
            if (locale in localizedValue) {
              completeLocalizedValue[locale] = localizedValue[locale];
            } else {
              // Use original value if exists, otherwise null
              const originalLocaleValue = (rootFieldValue as Record<string, unknown>)?.[locale];
              completeLocalizedValue[locale] = originalLocaleValue !== undefined ? originalLocaleValue : null;
            }
          }
          
          updateValue = completeLocalizedValue;
        }
        
        debugLog(debug, 'NESTED_MIGRATE', `Record ${record.id}: Updating with migrated nested data`);
        await client.items.update(record.id, {
          [rootFieldApiKey]: updateValue,
        });
        updatedCount++;
      } catch (error) {
        console.error(`Failed to update record ${record.id} with nested data:`, error);
        debugLog(debug, 'ERROR', `Record ${record.id}: Failed to update`, error);
        throw error;
      }
    }
  }

  debugLog(debug, 'NESTED_MIGRATE', `Nested data migration complete: ${updatedCount}/${recordCount} records updated`);
}

/**
 * Traverses blocks at a single level and applies the update function
 * Used when we need to update blocks at the first level of nesting
 */
function traverseAndUpdateNestedBlocksAtLevel(
  fieldValue: unknown,
  step: NestedBlockPath['path'][0],
  updateFn: (blockData: Record<string, unknown>, locale: string | null) => Record<string, unknown>,
  debug: DebugOptions = DEFAULT_DEBUG_OPTIONS
): { updated: boolean; newValue: unknown } {
  if (!fieldValue) {
    return { updated: false, newValue: fieldValue };
  }

  // Helper to process blocks at this level
  const processBlocksArray = (
    blocks: unknown[],
    locale: string | null
  ): { updated: boolean; newBlocks: unknown[] } => {
    let updated = false;
    const newBlocks: unknown[] = [];

    for (const block of blocks) {
      if (!block || typeof block !== 'object') {
        newBlocks.push(block);
        continue;
      }

      const blockObj = block as Record<string, unknown>;
      const blockTypeId = getBlockTypeId(blockObj);

      // Check if this block matches the expected type
      if (blockTypeId === step.expectedBlockTypeId) {
        // Apply the update function to this block
        const updatedBlock = updateFn(blockObj, locale);
        newBlocks.push(updatedBlock);
        updated = true;
        debugLog(debug, 'TRAVERSE_LEVEL', `Updated block at level`, { blockTypeId, locale });
      } else {
        // Block type doesn't match - keep as is
        newBlocks.push(block);
      }
    }

    return { updated, newBlocks };
  };

  // Handle localized vs non-localized fields
  if (step.localized && typeof fieldValue === 'object' && !Array.isArray(fieldValue)) {
    // Localized field - process each locale
    let anyUpdated = false;
    const newLocalizedValue: Record<string, unknown> = {};

    for (const [locale, localeValue] of Object.entries(fieldValue as Record<string, unknown>)) {
      const blocks = extractBlocksFromValue(localeValue, step.fieldType);
      
      if (blocks.length > 0) {
        const result = processBlocksArray(blocks, locale);
        if (result.updated) {
          anyUpdated = true;
          // Reconstruct the value based on field type
          if (step.fieldType === 'single_block') {
            newLocalizedValue[locale] = result.newBlocks[0] || null;
          } else if (step.fieldType === 'structured_text') {
            // For structured text with inlined blocks, update the blocks in the DAST document
            newLocalizedValue[locale] = reconstructStructuredTextWithUpdatedBlocks(
              localeValue as Record<string, unknown>,
              result.newBlocks
            );
          } else {
            newLocalizedValue[locale] = result.newBlocks;
          }
        } else {
          newLocalizedValue[locale] = localeValue;
        }
      } else {
        newLocalizedValue[locale] = localeValue;
      }
    }

    return { updated: anyUpdated, newValue: newLocalizedValue };
  } else {
    // Non-localized field
    const blocks = extractBlocksFromValue(fieldValue, step.fieldType);
    
    if (blocks.length > 0) {
      const result = processBlocksArray(blocks, null);
      if (result.updated) {
        // Reconstruct the value based on field type
        if (step.fieldType === 'single_block') {
          return { updated: true, newValue: result.newBlocks[0] || null };
        } else if (step.fieldType === 'structured_text') {
          // For structured text with inlined blocks, update the blocks in the DAST document
          return {
            updated: true,
            newValue: reconstructStructuredTextWithUpdatedBlocks(
              fieldValue as Record<string, unknown>,
              result.newBlocks
            ),
          };
        } else {
          return { updated: true, newValue: result.newBlocks };
        }
      }
    }

    return { updated: false, newValue: fieldValue };
  }
}

/**
 * Main conversion function that orchestrates the entire block-to-model conversion
 */
export async function convertBlockToModel(
  client: CMAClient,
  blockId: string,
  onProgress: ProgressCallback,
  debugOptions: DebugOptions = DEFAULT_DEBUG_OPTIONS
): Promise<ConversionResult> {
  const totalSteps = 6;
  let migratedRecordsCount = 0;
  let convertedFieldsCount = 0;

  // Log debug mode status
  if (debugOptions.enabled) {
    debugLogSection(debugOptions, 'DEBUG MODE ENABLED');
    debugLog(debugOptions, 'CONFIG', 'Debug options:', {
      suffix: debugOptions.suffix,
      skipDeletions: debugOptions.skipDeletions,
      verboseLogging: debugOptions.verboseLogging,
    });
    console.log('[DEBUG] ⚠️  Running in DEBUG MODE - no deletions will occur, all created items will have suffix:', debugOptions.suffix);
  }

  try {
    // Step 1: Analyze the block
    onProgress({
      currentStep: 1,
      totalSteps,
      stepDescription: 'Analyzing block structure...',
      percentage: 5,
    });

    debugLogSubSection(debugOptions, 'Step 1: Analyzing Block');
    debugLog(debugOptions, 'ANALYSIS', `Starting analysis for block ID: ${blockId}`);

    const analysis = await analyzeBlock(client, blockId);

    debugLog(debugOptions, 'ANALYSIS', 'Block info:', {
      name: analysis.block.name,
      apiKey: analysis.block.apiKey,
      fieldsCount: analysis.fields.length,
    });
    debugLog(debugOptions, 'ANALYSIS', 'Fields:', analysis.fields.map(f => ({
      label: f.label,
      apiKey: f.apiKey,
      type: f.fieldType,
      localized: f.localized,
    })));
    debugLog(debugOptions, 'ANALYSIS', `Modular content fields using this block: ${analysis.modularContentFields.length}`);
    debugLog(debugOptions, 'ANALYSIS', 'Modular content fields:', analysis.modularContentFields.map(f => ({
      parentModel: f.parentModelName,
      field: f.apiKey,
      isNestedInBlock: f.parentIsBlock,
      allowedBlocks: f.allowedBlockIds.length,
    })));
    debugLog(debugOptions, 'ANALYSIS', `Total affected records: ${analysis.totalAffectedRecords}`);

    if (analysis.modularContentFields.length === 0) {
      debugLog(debugOptions, 'ERROR', 'Block is not used in any modular content fields');
      return {
        success: false,
        migratedRecordsCount: 0,
        convertedFieldsCount: 0,
        error: 'This block is not used in any modular content fields',
      };
    }

    // Build nested paths for all fields
    const nestedPaths = await buildNestedPathsToRootModels(
      client,
      analysis.modularContentFields,
      blockId
    );

    debugLog(debugOptions, 'ANALYSIS', `Built ${nestedPaths.length} nested paths to root models`);
    nestedPaths.forEach((path, i) => {
      debugLog(debugOptions, 'ANALYSIS', `Path ${i + 1}: ${path.rootModelName} → ${path.path.map(p => p.fieldApiKey).join(' → ')} (localized context: ${path.isInLocalizedContext})`);
    });

    // Determine if any path is in a localized context
    const shouldLocalizeFields = nestedPaths.some(p => p.isInLocalizedContext);
    debugLog(debugOptions, 'ANALYSIS', `Should localize fields: ${shouldLocalizeFields}`);

    // Fetch available locales from site settings
    // Always needed for structured text updates to avoid "removing locales" issues
    const site = await client.site.find();
    const availableLocales = site.locales;
    debugLog(debugOptions, 'ANALYSIS', `Available locales: ${availableLocales.join(', ')}`)

    // Step 2: Create new model with same fields
    onProgress({
      currentStep: 2,
      totalSteps,
      stepDescription: `Creating new model "${analysis.block.name}"${shouldLocalizeFields ? ' (with localized fields)' : ''}...`,
      percentage: 15,
      details: `Copying ${analysis.fields.length} fields${shouldLocalizeFields ? ' as localized' : ''}`,
    });

    debugLogSubSection(debugOptions, 'Step 2: Creating New Model');
    if (shouldLocalizeFields) {
      debugLog(debugOptions, 'MODEL', 'Block is used in localized context - all fields will be created as localized');
    }

    const newModel = await createNewModelFromBlock(client, analysis, debugOptions, shouldLocalizeFields);

    debugLog(debugOptions, 'MODEL', 'New model created:', {
      id: newModel.id,
      apiKey: newModel.api_key,
    });

    // Step 3: Migrate block content to new records
    onProgress({
      currentStep: 3,
      totalSteps,
      stepDescription: `Migrating block content to new records${shouldLocalizeFields ? ' (grouped by locale)' : ''}...`,
      percentage: 30,
      details: `Processing ${nestedPaths.length} nested paths`,
    });

    debugLogSubSection(debugOptions, 'Step 3: Migrating Block Content to Records');

    // For each nested path, migrate blocks and create mapping
    const globalMapping: BlockMigrationMapping = {};

    for (let i = 0; i < nestedPaths.length; i++) {
      const nestedPath = nestedPaths[i];
      onProgress({
        currentStep: 3,
        totalSteps,
        stepDescription: `Migrating blocks from "${nestedPath.rootModelName}" → ${nestedPath.path.map(p => p.fieldApiKey).join(' → ')}...`,
        percentage: 30 + (20 * i) / nestedPaths.length,
      });

      debugLog(debugOptions, 'MIGRATE', `Processing path ${i + 1}/${nestedPaths.length}: ${nestedPath.rootModelName} (localized: ${nestedPath.isInLocalizedContext})`);

      let mapping: BlockMigrationMapping;

      if (nestedPath.isInLocalizedContext) {
        // Use grouped migration for localized contexts - merges locale data into single records
        debugLog(debugOptions, 'MIGRATE', 'Using grouped migration for localized context');
        
        const groupedInstances = await getGroupedBlockInstances(client, nestedPath, blockId);
        debugLog(debugOptions, 'MIGRATE', `Found ${groupedInstances.length} grouped block instances`);
        
        mapping = await migrateGroupedBlocksToRecords(
          client,
          groupedInstances,
          newModel.id,
          availableLocales,
          globalMapping,
          (count) => {
            migratedRecordsCount = count;
          },
          debugOptions
        );
      } else {
        // Use standard migration for non-localized contexts
        // BUT if shouldLocalizeFields is true, we need to wrap values in localized hashes
        // because the model was created with all fields localized
        debugLog(debugOptions, 'MIGRATE', `Using standard migration for non-localized context (forceLocalized: ${shouldLocalizeFields})`);
        
        mapping = await migrateBlocksToRecordsNested(
          client,
          nestedPath,
          blockId,
          newModel.id,
          globalMapping,
          (count) => {
            migratedRecordsCount = count;
          },
          debugOptions,
          shouldLocalizeFields,  // Pass forceLocalizedFields flag
          availableLocales       // Pass available locales
        );
      }

      Object.assign(globalMapping, mapping);
      debugLog(debugOptions, 'MIGRATE', `Mapping after path ${i + 1}:`, Object.keys(mapping).length + ' new mappings');
    }

    debugLog(debugOptions, 'MIGRATE', 'Global mapping complete:', {
      totalMappings: Object.keys(globalMapping).length,
      sample: Object.entries(globalMapping).slice(0, 5).map(([k, v]) => `${k} → ${v}`),
    });

    // Step 4: Convert modular content fields to links fields (includes data migration)
    onProgress({
      currentStep: 4,
      totalSteps,
      stepDescription: 'Converting field types and migrating data...',
      percentage: 55,
    });

    debugLogSubSection(debugOptions, 'Step 4: Converting Fields');

    for (let i = 0; i < analysis.modularContentFields.length; i++) {
      const mcField = analysis.modularContentFields[i];
      onProgress({
        currentStep: 4,
        totalSteps,
        stepDescription: `Converting "${mcField.parentModelName}.${mcField.apiKey}" to links field...`,
        percentage: 55 + (15 * i) / analysis.modularContentFields.length,
      });

      debugLog(debugOptions, 'FIELD', `Converting field ${i + 1}/${analysis.modularContentFields.length}:`, {
        parent: mcField.parentModelName,
        field: mcField.apiKey,
        type: mcField.fieldType,
      });

      await convertModularContentToLinksField(
        client,
        mcField,
        newModel.id,
        blockId,
        globalMapping,
        nestedPaths,
        availableLocales,
        debugOptions
      );
      convertedFieldsCount++;

      debugLog(debugOptions, 'FIELD', `Field converted successfully`);
    }

    // Step 5: Cleanup nested block references (non-debug mode only)
    // In debug mode, we skip this step entirely since:
    // 1. Data has already been migrated to the new links field in Step 4
    // 2. We want to preserve the original field and its blocks
    // In non-debug mode, this step removes converted blocks from original fields
    // when using partial replacement (keeping both modular content and links fields)
    onProgress({
      currentStep: 5,
      totalSteps,
      stepDescription: debugOptions.skipDeletions 
        ? 'Skipping nested cleanup (debug mode)...' 
        : 'Cleaning up nested block references...',
      percentage: 75,
    });

    debugLogSubSection(debugOptions, 'Step 5: Nested Block Cleanup');

    if (debugOptions.skipDeletions) {
      debugLog(debugOptions, 'NESTED', 'Skipping nested block cleanup in debug mode - original blocks preserved');
    } else {
      // In non-debug mode, we may need to remove converted blocks from original fields
      // This is only needed when there are remaining block types in the field
      
      // Group paths by root model to avoid updating the same records multiple times
      const pathsByRootModel = new Map<string, NestedBlockPath[]>();
      for (const path of nestedPaths) {
        // Only process paths with more than 1 step (nested blocks)
        // AND where there are remaining block types (partial replacement scenario)
        if (path.path.length > 1) {
          const mcField = path.fieldInfo;
          const remainingBlockIds = mcField.allowedBlockIds.filter((id) => id !== blockId);
          if (remainingBlockIds.length > 0) {
            const existing = pathsByRootModel.get(path.rootModelId) || [];
            existing.push(path);
            pathsByRootModel.set(path.rootModelId, existing);
          }
        }
      }

      debugLog(debugOptions, 'NESTED', `Found ${pathsByRootModel.size} root models needing nested cleanup`);

      let rootModelIndex = 0;
      for (const [rootModelId, paths] of pathsByRootModel) {
        const rootModelName = paths[0].rootModelName;
        onProgress({
          currentStep: 5,
          totalSteps,
          stepDescription: `Cleaning up nested blocks in "${rootModelName}"...`,
          percentage: 75 + (15 * rootModelIndex) / pathsByRootModel.size,
        });

        debugLog(debugOptions, 'NESTED', `Cleaning up nested blocks in "${rootModelName}" (${paths.length} paths)`);

        await cleanupNestedBlocksFromOriginalField(
          client,
          rootModelId,
          paths,
          blockId,
          debugOptions
        );
        rootModelIndex++;
      }
    }

    // Step 6: Done
    onProgress({
      currentStep: 6,
      totalSteps,
      stepDescription: 'Conversion complete!',
      percentage: 100,
      details: `Created model "${newModel.api_key}" with ${migratedRecordsCount} records`,
    });

    debugLogSection(debugOptions, 'CONVERSION COMPLETE');
    debugLog(debugOptions, 'RESULT', 'Conversion summary:', {
      newModelId: newModel.id,
      newModelApiKey: newModel.api_key,
      migratedRecords: migratedRecordsCount,
      convertedFields: convertedFieldsCount,
      debugMode: debugOptions.enabled,
      suffix: debugOptions.suffix,
    });

    if (debugOptions.enabled) {
      console.log('[DEBUG] ✅ Conversion completed in DEBUG MODE');
      console.log('[DEBUG] 📋 Original fields/blocks were NOT deleted');
      console.log('[DEBUG] 🏷️  All created items have suffix:', debugOptions.suffix);
    }

    return {
      success: true,
      newModelId: newModel.id,
      newModelApiKey: newModel.api_key,
      migratedRecordsCount,
      convertedFieldsCount,
      originalBlockName: analysis.block.name,
      originalBlockApiKey: analysis.block.apiKey,
    };
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    
    debugLogSection(debugOptions, 'CONVERSION FAILED');
    debugLog(debugOptions, 'ERROR', 'Error details:', {
      message: errorMessage,
      stack: error instanceof Error ? error.stack : undefined,
    });

    return {
      success: false,
      migratedRecordsCount,
      convertedFieldsCount,
      error: errorMessage,
    };
  }
}

/**
 * Sanitizes the appearance object by removing properties that are not allowed
 */
function sanitizeAppearance(
  appearance: Record<string, unknown> | undefined
): Record<string, unknown> | undefined {
  if (!appearance) return undefined;
  // Remove 'type' property which is not allowed in field creation
  const { type: _type, ...sanitized } = appearance as Record<string, unknown> & { type?: unknown };
  return sanitized;
}

/**
 * Updates field ID references in validators when copying fields to a new model.
 * This is necessary for validators like `slug_title_field` that reference other fields by ID.
 */
function updateValidatorFieldReferences(
  validators: Record<string, unknown>,
  fieldIdMapping: Record<string, string>,
  debug: DebugOptions
): Record<string, unknown> {
  const updatedValidators = { ...validators };
  
  // Handle slug_title_field validator - used by slug fields to reference their title field
  if (updatedValidators.slug_title_field && typeof updatedValidators.slug_title_field === 'object') {
    const slugValidator = updatedValidators.slug_title_field as Record<string, unknown>;
    const oldTitleFieldId = slugValidator.title_field_id as string | undefined;
    
    if (oldTitleFieldId && fieldIdMapping[oldTitleFieldId]) {
      const newTitleFieldId = fieldIdMapping[oldTitleFieldId];
      updatedValidators.slug_title_field = {
        ...slugValidator,
        title_field_id: newTitleFieldId,
      };
      debugLog(debug, 'VALIDATOR', `Updated slug_title_field.title_field_id: ${oldTitleFieldId} → ${newTitleFieldId}`);
    } else if (oldTitleFieldId) {
      // The referenced field hasn't been created yet or doesn't exist
      // This can happen if the field order is incorrect or the title field is missing
      debugLog(debug, 'VALIDATOR', `Warning: slug_title_field references field ${oldTitleFieldId} which is not in the mapping yet`);
      // Remove the validator to avoid API errors - the field will work without it
      delete updatedValidators.slug_title_field;
      debugLog(debug, 'VALIDATOR', `Removed slug_title_field validator to avoid API errors`);
    }
  }
  
  // Handle other validators that might reference field IDs
  // (Add more cases here as needed for other field types)
  
  return updatedValidators;
}

/**
 * Creates a new regular model from a block model, copying all fields
 * @param forceLocalizedFields - If true, all fields will be created as localized (for blocks in localized contexts)
 */
async function createNewModelFromBlock(
  client: CMAClient,
  analysis: BlockAnalysis,
  debug: DebugOptions = DEFAULT_DEBUG_OPTIONS,
  forceLocalizedFields: boolean = false
): Promise<{ id: string; api_key: string }> {
  // DatoCMS API key limit is 40 characters
  const API_KEY_MAX_LENGTH = 40;
  
  // Sanitize the api_key - must be lowercase alphanumeric with underscores only
  let sanitizedBlockApiKey = analysis.block.apiKey
    .toLowerCase()
    .replace(/[^a-z0-9_]/g, '_')
    .replace(/_+/g, '_') // Remove duplicate underscores
    .replace(/^_|_$/g, ''); // Remove leading/trailing underscores
  
  // DatoCMS requires model API keys to be plural (end with 's')
  // If the api_key doesn't end with 's', add one
  if (!sanitizedBlockApiKey.endsWith('s')) {
    sanitizedBlockApiKey = sanitizedBlockApiKey + 's';
  }
  
  // In debug mode, add the suffix to the base api_key
  // Sanitize the debug suffix to remove any numbers (DatoCMS api_keys don't allow numbers)
  const rawDebugSuffix = debug.enabled && debug.suffix ? debug.suffix : '';
  const debugSuffix = rawDebugSuffix.replace(/[^a-z_]/gi, '').toLowerCase();
  
  // Calculate how much space we have for the base api_key
  // We need room for: {base}_conv{suffix} or {base}_conv{suffix}_{counter}
  const suffixPart = `_conv${debugSuffix}`;
  const maxBaseLength = API_KEY_MAX_LENGTH - suffixPart.length - 3; // -3 for potential counter "_99"
  
  // Truncate the base api_key if needed
  const truncatedBlockApiKey = sanitizedBlockApiKey.length > maxBaseLength 
    ? sanitizedBlockApiKey.slice(0, maxBaseLength).replace(/_$/, '') // Remove trailing underscore if truncation creates one
    : sanitizedBlockApiKey;
  
  const baseApiKey = `${truncatedBlockApiKey}${suffixPart}`;
  const baseName = analysis.block.name.replace(/[^\w\s-]/g, '').trim() || 'Converted Block';
  const debugNameSuffix = debug.enabled && debug.suffix ? ` ${debug.suffix}` : '';

  const existingModels = await client.itemTypes.list();

  // Helper function to convert a counter number to a letter suffix (a, b, c, ... z, aa, ab, ...)
  // DatoCMS api_keys don't allow numbers, so we use letters instead
  const counterToLetterSuffix = (n: number): string => {
    let result = '';
    while (n > 0) {
      n--; // Adjust for 0-based indexing
      result = String.fromCharCode(97 + (n % 26)) + result; // 97 = 'a'
      n = Math.floor(n / 26);
    }
    return result;
  };

  // Helper function to sanitize api_key (remove numbers, clean up)
  const sanitizeApiKey = (key: string): string => {
    return key
      .toLowerCase()
      .replace(/[^a-z_]/g, '') // Only allow lowercase letters and underscores - NO numbers
      .replace(/_+/g, '_') // Collapse multiple underscores
      .replace(/^_+|_+$/g, ''); // Remove leading/trailing underscores
  };

  let finalApiKey = sanitizeApiKey(baseApiKey);
  let finalName = `${baseName} (Converted)${debugNameSuffix}`;
  let counter = 0;

  debugLog(debug, 'MODEL', `Checking for existing models. Base api_key: "${baseApiKey}" -> sanitized: "${finalApiKey}" (length: ${finalApiKey.length}), base name: "${baseName}"`);
  debugLog(debug, 'MODEL', `Existing models count: ${existingModels.length}`);
  
  // Check for uniqueness using the SANITIZED api_key
  while (
    existingModels.some((m) => m.api_key === finalApiKey) ||
    existingModels.some((m) => m.name === finalName)
  ) {
    counter++;
    const conflictingApiKey = existingModels.find((m) => m.api_key === finalApiKey);
    const conflictingName = existingModels.find((m) => m.name === finalName);
    const letterSuffix = counterToLetterSuffix(counter);
    debugLog(debug, 'MODEL', `Conflict found - api_key: ${conflictingApiKey?.api_key || 'none'}, name: ${conflictingName?.name || 'none'}, trying suffix: ${letterSuffix}`);
    
    // Use letter suffix instead of number to avoid being stripped by sanitization
    finalApiKey = sanitizeApiKey(`${baseApiKey}_${letterSuffix}`);
    finalName = `${baseName} (Converted ${counter})${debugNameSuffix}`;

    if (counter > 100) {
      throw new Error('Could not find a unique model name/api_key after 100 attempts');
    }
  }

  // Final length check - truncate if still over limit
  let validatedApiKey = finalApiKey;
  if (validatedApiKey.length > API_KEY_MAX_LENGTH) {
    validatedApiKey = validatedApiKey.slice(0, API_KEY_MAX_LENGTH).replace(/_$/, '');
  }
  
  if (!validatedApiKey || validatedApiKey.length < 1) {
    throw new Error(`Invalid api_key generated: "${baseApiKey}" -> "${validatedApiKey}"`);
  }
  
  debugLog(debug, 'MODEL', `Final api_key: "${validatedApiKey}" (length: ${validatedApiKey.length})`);

  debugLog(debug, 'MODEL', `Creating model with name: "${finalName}", api_key: "${validatedApiKey}"`);
  
  const newModel = await client.itemTypes.create({
    name: finalName,
    api_key: validatedApiKey,
    modular_block: false,
    sortable: true,
    draft_mode_active: false,
    collection_appearance: 'table',
  });

  debugLog(debug, 'MODEL', `Model created with ID: ${newModel.id}`);

  let titleFieldId: string | null = null;
  
  // Build a mapping from old field IDs to new field IDs
  // This is needed to update validators that reference other fields (e.g., slug fields)
  const fieldIdMapping: Record<string, string> = {};
  
  // Sort fields so that non-slug fields are created first
  // This ensures the title field exists before we create the slug field that references it
  const sortedFields = [...analysis.fields].sort((a, b) => {
    // Put slug fields last so their referenced fields exist first
    if (a.fieldType === 'slug' && b.fieldType !== 'slug') return 1;
    if (a.fieldType !== 'slug' && b.fieldType === 'slug') return -1;
    return a.position - b.position;
  });

  for (const field of sortedFields) {
    const sanitizedAppearance = sanitizeAppearance(field.appearance as Record<string, unknown>);

    // If forceLocalizedFields is true, make all fields localized (for blocks in localized contexts)
    const shouldBeLocalized = forceLocalizedFields ? true : field.localized;

    // Update validators that reference other fields (e.g., slug_title_field)
    let updatedValidators = field.validators;
    if (field.validators && typeof field.validators === 'object') {
      updatedValidators = updateValidatorFieldReferences(
        field.validators as Record<string, unknown>,
        fieldIdMapping,
        debug
      );
    }

    // Build field creation data - use Record type to allow dynamic field types from DatoCMS
    const newFieldData: Record<string, unknown> = {
      label: field.label,
      api_key: field.apiKey,
      field_type: field.fieldType,
      localized: shouldBeLocalized,
      validators: updatedValidators,
      appearance: sanitizedAppearance,
      position: field.position,
    };

    if (field.hint) {
      newFieldData.hint = field.hint;
    }

    // Handle default values carefully when forcing localization
    // If we're forcing localization but the original field wasn't localized,
    // skip the default value as the format would be incompatible
    if (field.defaultValue !== undefined) {
      if (forceLocalizedFields && !field.localized) {
        debugLog(debug, 'FIELD', `Skipping default value for "${field.apiKey}" - format incompatible with forced localization`);
      } else {
        newFieldData.default_value = field.defaultValue;
      }
    }

    debugLog(debug, 'FIELD', `Creating field: "${field.label}" (${field.apiKey}) - ${field.fieldType}${forceLocalizedFields ? ' [forced localized]' : ''}`);
    // Cast to expected type - field data is dynamically constructed from source field
    const newField = await client.fields.create(newModel.id, newFieldData as Parameters<typeof client.fields.create>[1]);

    // Store the mapping from old field ID to new field ID
    fieldIdMapping[field.id] = newField.id;
    debugLog(debug, 'FIELD', `Field ID mapping: ${field.id} → ${newField.id}`);

    if (!titleFieldId && field.fieldType === 'string') {
      titleFieldId = newField.id;
      debugLog(debug, 'FIELD', `Setting "${field.apiKey}" as title field`);
    }
  }

  if (titleFieldId) {
    await client.itemTypes.update(newModel.id, {
      title_field: { type: 'field', id: titleFieldId },
    });
  }

  debugLog(debug, 'MODEL', `Model creation complete: ${newModel.api_key} with ${analysis.fields.length} fields`);

  return { id: newModel.id, api_key: newModel.api_key };
}

/**
 * Migrates block instances to new records using nested paths
 * @param forceLocalizedFields If true, wrap field values in localized hashes (duplicate across all locales)
 * @param availableLocales List of available locales when forceLocalizedFields is true
 */
async function migrateBlocksToRecordsNested(
  client: CMAClient,
  nestedPath: NestedBlockPath,
  blockId: string,
  newModelId: string,
  existingMapping: BlockMigrationMapping,
  onMigrated: (count: number) => void,
  debug: DebugOptions = DEFAULT_DEBUG_OPTIONS,
  forceLocalizedFields: boolean = false,
  availableLocales: string[] = []
): Promise<BlockMigrationMapping> {
  const mapping: BlockMigrationMapping = {};
  let migratedCount = Object.keys(existingMapping).length;

  // Get all block instances following the nested path
  debugLog(debug, 'MIGRATE', `Fetching block instances for path: ${nestedPath.path.map(p => p.fieldApiKey).join(' → ')}`);
  const blockInstances = await getAllBlockInstancesNested(client, nestedPath, blockId);
  debugLog(debug, 'MIGRATE', `Found ${blockInstances.length} block instances`);

  // Filter out blocks that were already migrated
  const uniqueBlocks = new Map<string, (typeof blockInstances)[0]>();
  for (const instance of blockInstances) {
    if (!uniqueBlocks.has(instance.blockId) && !existingMapping[instance.blockId]) {
      uniqueBlocks.set(instance.blockId, instance);
    }
  }

  const blocksArray = Array.from(uniqueBlocks.values());
  debugLog(debug, 'MIGRATE', `${blocksArray.length} unique blocks to migrate (${blockInstances.length - blocksArray.length} already mapped)`);
  debugLog(debug, 'MIGRATE', `Force localized fields: ${forceLocalizedFields}`);

  await processBatch(
    blocksArray,
    10,
    async (instance) => {
      try {
        // Sanitize the block data to remove `id` and other read-only properties from nested blocks
        const sanitizedData = sanitizeFieldValuesForCreation(instance.blockData);
        
        // If we need to force localized fields, wrap each field value in a localized hash
        // This happens when the model was created with all localized fields but this specific
        // path is not in a localized context
        let recordData: Record<string, unknown>;
        if (forceLocalizedFields && availableLocales.length > 0) {
          recordData = wrapFieldsInLocalizedHash(sanitizedData, availableLocales);
          debugLog(debug, 'MIGRATE', `Wrapped fields in localized hash for block ${instance.blockId}`, {
            originalKeys: Object.keys(sanitizedData),
            locales: availableLocales,
          });
        } else {
          recordData = sanitizedData;
        }
        
        debugLog(debug, 'MIGRATE', `Creating record from block ${instance.blockId}`, {
          rootRecordId: instance.rootRecordId,
          locale: instance.locale,
          dataKeys: Object.keys(recordData),
        });

        const newRecord = await client.items.create({
          item_type: { type: 'item_type', id: newModelId },
          ...recordData,
        });

        mapping[instance.blockId] = newRecord.id;
        migratedCount++;
        onMigrated(migratedCount);

        debugLog(debug, 'MIGRATE', `Block ${instance.blockId} → Record ${newRecord.id}`);
      } catch (error) {
        console.error(`Failed to migrate block ${instance.blockId}:`, error);
        debugLog(debug, 'ERROR', `Failed to migrate block ${instance.blockId}:`, error);
        throw error;
      }
    },
    200
  );

  debugLog(debug, 'MIGRATE', `Migration complete: ${Object.keys(mapping).length} new records created`);
  return mapping;
}

/**
 * Migrates grouped block instances (from localized contexts) to records with localized field values.
 * Creates ONE record per block position, with field values merged from all locales.
 */
async function migrateGroupedBlocksToRecords(
  client: CMAClient,
  groupedInstances: GroupedBlockInstance[],
  newModelId: string,
  availableLocales: string[],
  existingMapping: BlockMigrationMapping,
  onMigrated: (count: number) => void,
  debug: DebugOptions = DEFAULT_DEBUG_OPTIONS
): Promise<BlockMigrationMapping> {
  const mapping: BlockMigrationMapping = {};
  let migratedCount = Object.keys(existingMapping).length;

  debugLog(debug, 'MIGRATE_GROUPED', `Starting grouped migration for ${groupedInstances.length} grouped instances`);
  debugLog(debug, 'MIGRATE_GROUPED', `Available locales: ${availableLocales.join(', ')}`);

  // Filter out groups where all block IDs were already migrated
  const groupsToMigrate = groupedInstances.filter(group => {
    return !group.allBlockIds.every(id => existingMapping[id]);
  });

  debugLog(debug, 'MIGRATE_GROUPED', `${groupsToMigrate.length} groups to migrate (${groupedInstances.length - groupsToMigrate.length} already mapped)`);

  await processBatch(
    groupsToMigrate,
    10,
    async (group) => {
      try {
        // Build localized field values from the group's locale data
        const localizedFieldData: Record<string, Record<string, unknown>> = {};

        // Check if we have __default__ data (non-localized context that was marked as localized)
        const hasDefaultData = '__default__' in group.localeData;
        const defaultData = hasDefaultData ? group.localeData['__default__'] : null;

        debugLog(debug, 'MIGRATE_GROUPED', `Group ${group.groupKey} localeData keys: ${JSON.stringify(Object.keys(group.localeData))}`);
        if (hasDefaultData) {
          debugLog(debug, 'MIGRATE_GROUPED', `Found __default__ data, will duplicate across all locales`);
        }

        // Get all field keys from all locales
        const allFieldKeys = new Set<string>();
        for (const localeKey of Object.keys(group.localeData)) {
          for (const fieldKey of Object.keys(group.localeData[localeKey])) {
            allFieldKeys.add(fieldKey);
          }
        }

        // For each field, build a localized value object
        // First, find a fallback locale that has data (prefer 'en' or first available)
        const localesWithData = Object.keys(group.localeData).filter(k => k !== '__default__');
        const fallbackLocale = localesWithData.includes('en') ? 'en' : localesWithData[0];
        const fallbackData = fallbackLocale ? group.localeData[fallbackLocale] : null;
        
        for (const fieldKey of allFieldKeys) {
          const localizedValue: Record<string, unknown> = {};
          
          for (const locale of availableLocales) {
            // Get the value from this locale's block data
            let localeBlockData = group.localeData[locale];
            
            // If no locale-specific data, fall back to __default__ data
            // This handles cases where locale wasn't properly propagated
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
          
          localizedFieldData[fieldKey] = localizedValue;
        }

        // Sanitize the localized field data
        const sanitizedData = sanitizeLocalizedFieldValuesForCreation(localizedFieldData);

        debugLog(debug, 'MIGRATE_GROUPED', `Creating record from grouped block ${group.groupKey}`, {
          rootRecordId: group.rootRecordId,
          pathIndices: group.pathIndices,
          localesPresent: Object.keys(group.localeData),
          fieldsCount: Object.keys(sanitizedData).length,
        });

        const newRecord = await client.items.create({
          item_type: { type: 'item_type', id: newModelId },
          ...sanitizedData,
        });

        // Map ALL original block IDs from all locales to this single new record
        for (const blockId of group.allBlockIds) {
          mapping[blockId] = newRecord.id;
        }
        
        // Also map the group key for easy reference
        mapping[group.groupKey] = newRecord.id;
        
        migratedCount++;
        onMigrated(migratedCount);

        debugLog(debug, 'MIGRATE_GROUPED', `Group ${group.groupKey} → Record ${newRecord.id} (mapped ${group.allBlockIds.length} block IDs)`);
      } catch (error) {
        console.error(`Failed to migrate grouped block ${group.groupKey}:`, error);
        debugLog(debug, 'ERROR', `Failed to migrate grouped block ${group.groupKey}:`, error);
        throw error;
      }
    },
    200
  );

  debugLog(debug, 'MIGRATE_GROUPED', `Grouped migration complete: ${Object.keys(mapping).length} new mappings created`);
  return mapping;
}

/**
 * Sanitizes localized field values for creating a new top-level record.
 * Each field value is expected to be an object with locale keys.
 */
function sanitizeLocalizedFieldValuesForCreation(
  data: Record<string, Record<string, unknown>>
): Record<string, Record<string, unknown>> {
  const result: Record<string, Record<string, unknown>> = {};

  for (const [fieldKey, localizedValue] of Object.entries(data)) {
    const sanitizedLocalizedValue: Record<string, unknown> = {};
    
    for (const [locale, value] of Object.entries(localizedValue)) {
      if (Array.isArray(value)) {
        // Could be an array of blocks (modular content field)
        sanitizedLocalizedValue[locale] = value.map((item) => {
          if (item && typeof item === 'object') {
            const obj = item as Record<string, unknown>;
            const isBlock =
              obj.__itemTypeId !== undefined ||
              obj.item_type !== undefined ||
              (obj.relationships && typeof obj.relationships === 'object');

            if (isBlock) {
              return sanitizeBlockDataForCreation(item);
            }
          }
          return sanitizeBlockDataForCreation(item);
        });
      } else if (value && typeof value === 'object') {
        const obj = value as Record<string, unknown>;

        // Check for structured text (has document and possibly blocks)
        if ('document' in obj || 'blocks' in obj) {
          sanitizedLocalizedValue[locale] = {
            ...obj,
            blocks: obj.blocks
              ? (obj.blocks as unknown[]).map((b) => sanitizeBlockDataForCreation(b))
              : undefined,
          };
        } else {
          // Regular object or single block
          sanitizedLocalizedValue[locale] = sanitizeBlockDataForCreation(value);
        }
      } else {
        sanitizedLocalizedValue[locale] = value;
      }
    }
    
    result[fieldKey] = sanitizedLocalizedValue;
  }

  return result;
}

/**
 * Migrates structured text field data by transforming the DAST document in-place.
 * Replaces block/inlineBlock nodes with inlineItem nodes pointing to the new records.
 */
async function migrateStructuredTextFieldData(
  client: CMAClient,
  modelId: string,
  fieldApiKey: string,
  isLocalized: boolean,
  targetBlockId: string,
  mapping: BlockMigrationMapping,
  availableLocales: string[],
  debug: DebugOptions = DEFAULT_DEBUG_OPTIONS
): Promise<void> {
  // In debug mode, we ADD links after blocks instead of replacing them
  const debugMode = debug.skipDeletions;
  if (debugMode) {
    debugLog(debug, 'ST_MIGRATE', `DEBUG MODE: Will add links AFTER blocks for "${fieldApiKey}" - original blocks preserved`);
  }

  debugLog(debug, 'ST_MIGRATE', `Migrating structured text field "${fieldApiKey}"`, {
    modelId,
    isLocalized,
    mappingCount: Object.keys(mapping).length,
    availableLocales,
  });

  let recordCount = 0;
  let updatedCount = 0;

  for await (const record of client.items.listPagedIterator({
    filter: { type: modelId },
    nested: true,
  })) {
    recordCount++;
    const fieldValue = record[fieldApiKey];
    if (!fieldValue) {
      debugLog(debug, 'ST_MIGRATE', `Record ${record.id}: No value in "${fieldApiKey}", skipping`);
      continue;
    }

    let newValue: unknown = null;
    let hasChanges = false;

    if (isLocalized && typeof fieldValue === 'object' && !Array.isArray(fieldValue)) {
      // Localized field - process each locale
      // IMPORTANT: We must include ALL project locales to avoid DatoCMS interpreting
      // missing locales as "removed". For locales not in the source, we keep null.
      const localizedValue: Record<string, unknown> = {};
      const sourceLocaleValues = fieldValue as Record<string, unknown>;
      
      for (const locale of availableLocales) {
        const localeValue = sourceLocaleValues[locale];
        
        if (localeValue !== undefined) {
          if (isStructuredTextValue(localeValue)) {
            const transformed = transformDastBlocksToLinks(localeValue, targetBlockId, mapping, debugMode);
            if (transformed) {
              localizedValue[locale] = transformed;
              hasChanges = true;
              debugLog(debug, 'ST_MIGRATE', `Record ${record.id}[${locale}]: Transformed DAST (debug=${debugMode})`);
            } else {
              localizedValue[locale] = localeValue;
            }
          } else {
            localizedValue[locale] = localeValue;
          }
        } else {
          // Locale not present in source - set to null to preserve the "missing" state
          // without DatoCMS thinking we're trying to remove it
          localizedValue[locale] = null;
        }
      }
      
      newValue = localizedValue;
    } else if (isStructuredTextValue(fieldValue)) {
      // Non-localized field
      const transformed = transformDastBlocksToLinks(fieldValue, targetBlockId, mapping, debugMode);
      if (transformed) {
        newValue = transformed;
        hasChanges = true;
        debugLog(debug, 'ST_MIGRATE', `Record ${record.id}: Transformed DAST (debug=${debugMode})`);
      }
    }

    if (hasChanges && newValue) {
      try {
        await client.items.update(record.id, {
          [fieldApiKey]: newValue,
        });
        updatedCount++;
        debugLog(debug, 'ST_MIGRATE', `Record ${record.id}: Updated successfully`);
      } catch (error) {
        debugLog(debug, 'ERROR', `Record ${record.id}: Failed to update`, error);
      }
    }
  }

  debugLog(debug, 'ST_MIGRATE', `Structured text migration complete: ${updatedCount}/${recordCount} records updated`);
}

/**
 * Handles conversion of structured text fields.
 * Unlike modular content fields, structured text fields:
 * 1. Keep the same field type (structured_text)
 * 2. Transform the DAST document in-place to replace block nodes with inlineItem nodes
 * 3. Update validators to remove the block type and add the new model as a linkable type
 */
async function handleStructuredTextFieldConversion(
  client: CMAClient,
  mcField: ModularContentFieldInfo,
  currentField: { id: string; validators: Record<string, unknown>; [key: string]: unknown },
  newModelId: string,
  blockIdToRemove: string,
  remainingBlockIds: string[],
  mapping: BlockMigrationMapping,
  nestedPaths: NestedBlockPath[],
  availableLocales: string[],
  debug: DebugOptions = DEFAULT_DEBUG_OPTIONS
): Promise<void> {
  debugLog(debug, 'ST_CONVERT', `Converting structured text field "${mcField.apiKey}" in "${mcField.parentModelName}"`);
  
  const validators = currentField.validators as Record<string, unknown>;

  // PHASE 1: Add the new model to allowed links, but KEEP the block type
  // We MUST add the new model as an allowed link type BEFORE we can save records with inlineItem nodes
  // IMPORTANT: We keep the block type during data migration to prevent DatoCMS from stripping
  // the blocks before we can transform them to inlineItem nodes
  const currentBlocksValidator = validators.structured_text_blocks as { item_types?: string[] } | undefined;
  const currentLinksValidator = validators.structured_text_links as { item_types?: string[] } | undefined;

  // Build Phase 1 validators - add link type but preserve block type
  const phase1Validators: Record<string, unknown> = { ...validators };

  // Update links validator - add the new model ID
  const existingLinkTypes = currentLinksValidator?.item_types || [];
  if (!existingLinkTypes.includes(newModelId)) {
    phase1Validators.structured_text_links = {
      ...currentLinksValidator,
      item_types: [...existingLinkTypes, newModelId],
    };
    debugLog(debug, 'ST_CONVERT', `Phase 1: Adding new model ${newModelId} to allowed link types`);
  }

  // IMPORTANT: Always preserve the block type during data migration
  // This prevents DatoCMS from stripping blocks before we can transform them
  if (currentBlocksValidator) {
    phase1Validators.structured_text_blocks = currentBlocksValidator;
  }
  debugLog(debug, 'ST_CONVERT', `Phase 1: Keeping block type in validators during data migration`, {
    blockTypes: currentBlocksValidator?.item_types,
  });

  // Apply Phase 1 validator update
  debugLog(debug, 'ST_CONVERT', `Applying Phase 1 validators`, {
    newLinkTypes: (phase1Validators.structured_text_links as { item_types?: string[] })?.item_types,
  });

  await client.fields.update(mcField.id, {
    validators: phase1Validators,
  });

  // PHASE 2: Migrate the DAST data (transform blocks to inlineItems)
  // This must happen AFTER Phase 1 validator update, otherwise DatoCMS will reject the records
  // At this point, blocks are still valid because we kept the block type in validators
  if (mcField.parentIsBlock) {
    // Field is inside a block - need to use nested migration
    const nestedPath = findNestedPathForField(nestedPaths, mcField);
    if (nestedPath) {
      debugLog(debug, 'ST_CONVERT', `Phase 2: Using nested migration for block field`);
      await migrateNestedStructuredTextFieldData(
        client,
        nestedPath,
        mcField.apiKey,
        blockIdToRemove,
        mapping,
        debug
      );
    } else {
      debugLog(debug, 'ST_CONVERT', `Warning: Could not find nested path for block field, skipping data migration`);
    }
  } else {
    // Field is in a root model - use standard migration
    debugLog(debug, 'ST_CONVERT', `Phase 2: Migrating DAST data for root model field`);
    await migrateStructuredTextFieldData(
      client,
      mcField.parentModelId,
      mcField.apiKey,
      mcField.localized,
      blockIdToRemove,
      mapping,
      availableLocales,
      debug
    );
  }

  // PHASE 3: Now remove the block type from validators (only if NOT in debug mode)
  // The blocks have been transformed to inlineItems, so they no longer need to be valid
  if (!debug.skipDeletions) {
    const phase3Validators: Record<string, unknown> = { ...phase1Validators };
    
    if (remainingBlockIds.length > 0) {
      phase3Validators.structured_text_blocks = {
        ...currentBlocksValidator,
        item_types: remainingBlockIds,
      };
    } else {
      // No remaining blocks - set empty
      phase3Validators.structured_text_blocks = {
        ...currentBlocksValidator,
        item_types: [],
      };
    }
    
    debugLog(debug, 'ST_CONVERT', `Phase 3: Removing block type ${blockIdToRemove} from allowed blocks`, {
      remainingBlockIds,
    });
    
    await client.fields.update(mcField.id, {
      validators: phase3Validators,
    });
  } else {
    debugLog(debug, 'ST_CONVERT', `DEBUG MODE: Skipping Phase 3 - keeping block type in validators`, {
      originalBlockTypes: currentBlocksValidator?.item_types,
    });
  }

  debugLog(debug, 'ST_CONVERT', `Structured text field conversion complete for "${mcField.apiKey}"`);
}

/**
 * Migrates structured text data for fields inside nested blocks.
 * Similar to migrateNestedBlockFieldData but transforms DAST in-place.
 */
async function migrateNestedStructuredTextFieldData(
  client: CMAClient,
  nestedPath: NestedBlockPath,
  fieldApiKey: string,
  targetBlockId: string,
  mapping: BlockMigrationMapping,
  debug: DebugOptions = DEFAULT_DEBUG_OPTIONS
): Promise<void> {
  // In debug mode, we ADD links after blocks instead of replacing them
  const debugMode = debug.skipDeletions;
  if (debugMode) {
    debugLog(debug, 'ST_NESTED', `DEBUG MODE: Will add links AFTER blocks for nested field "${fieldApiKey}" - original blocks preserved`);
  }

  debugLog(debug, 'ST_NESTED', `Migrating nested structured text field data`, {
    rootModel: nestedPath.rootModelName,
    path: nestedPath.path.map(p => p.fieldApiKey).join(' → '),
    field: fieldApiKey,
  });

  let recordCount = 0;
  let updatedCount = 0;

  // Query records from the ROOT model
  for await (const record of client.items.listPagedIterator({
    filter: { type: nestedPath.rootModelId },
    nested: true,
  })) {
    recordCount++;

    // Get the root field value (first step in the path)
    const rootFieldApiKey = nestedPath.path[0].fieldApiKey;
    const rootFieldValue = record[rootFieldApiKey];

    if (!rootFieldValue) {
      continue;
    }

    // Create the update function that will be called for parent blocks
    const updateBlockFn = (blockData: Record<string, unknown>, locale: string | null): Record<string, unknown> => {
      const stValue = getNestedFieldValueFromBlock(blockData, fieldApiKey);
      
      if (!stValue || !isStructuredTextValue(stValue)) {
        return blockData;
      }

      const transformed = transformDastBlocksToLinks(stValue, targetBlockId, mapping, debugMode);
      if (transformed) {
        debugLog(debug, 'ST_NESTED', `Transformed DAST for nested field (debug=${debugMode})`, { locale });
        return setNestedFieldValueInBlock(blockData, fieldApiKey, transformed);
      }

      return blockData;
    };

    // Use the same traversal logic as migrateNestedBlockFieldData
    const pathToParentBlock = nestedPath.path.slice(0, -1);
    
    let result: { updated: boolean; newValue: unknown };

    if (pathToParentBlock.length === 0) {
      result = traverseAndUpdateNestedBlocksAtLevel(
        rootFieldValue,
        nestedPath.path[0],
        updateBlockFn,
        debug
      );
    } else {
      result = traverseAndUpdateNestedBlocks(
        rootFieldValue,
        pathToParentBlock,
        0,
        updateBlockFn,
        debug
      );
    }

    if (result.updated) {
      try {
        await client.items.update(record.id, {
          [rootFieldApiKey]: result.newValue,
        });
        updatedCount++;
      } catch (error) {
        console.error(`Failed to update record ${record.id} with nested structured text:`, error);
        throw error;
      }
    }
  }

  debugLog(debug, 'ST_NESTED', `Nested structured text migration complete: ${updatedCount}/${recordCount} records updated`);
}

/**
 * Converts a modular content field to a links field, including data migration.
 * Handles both top-level fields (in root models) and nested fields (inside blocks).
 * 
 * For structured_text fields, transforms the DAST in-place and updates validators
 * rather than creating a separate links field.
 */
async function convertModularContentToLinksField(
  client: CMAClient,
  mcField: ModularContentFieldInfo,
  newModelId: string,
  blockIdToRemove: string,
  mapping: BlockMigrationMapping,
  nestedPaths: NestedBlockPath[],
  availableLocales: string[],
  debug: DebugOptions = DEFAULT_DEBUG_OPTIONS
): Promise<void> {
  const currentField = await client.fields.find(mcField.id);
  const originalLabel = currentField.label;
  const originalApiKey = mcField.apiKey;
  const originalPosition = currentField.position;
  const originalHint = currentField.hint || undefined;
  
  // Preserve fieldset information - the field may be inside a fieldset
  const originalFieldset = currentField.fieldset;

  const remainingBlockIds = mcField.allowedBlockIds.filter((id) => id !== blockIdToRemove);

  // Determine if this is a single_block field - use 'link' (singular) instead of 'links'
  const isSingleBlock = mcField.fieldType === 'single_block';
  const newFieldType = isSingleBlock ? 'link' : 'links';

  // In debug mode, add suffix to field api_key
  const debugSuffix = debug.enabled && debug.suffix ? debug.suffix : '';

  debugLog(debug, 'FIELD', `Converting field "${originalApiKey}" in "${mcField.parentModelName}"`, {
    fieldType: mcField.fieldType,
    isSingleBlock,
    newFieldType,
    remainingBlockTypes: remainingBlockIds.length,
    debugMode: debug.enabled,
    skipDeletions: debug.skipDeletions,
  });

  // SPECIAL HANDLING FOR STRUCTURED TEXT FIELDS
  // For structured text, we transform the DAST in-place and update validators
  // rather than creating a separate links field
  if (mcField.fieldType === 'structured_text') {
    await handleStructuredTextFieldConversion(
      client,
      mcField,
      currentField,
      newModelId,
      blockIdToRemove,
      remainingBlockIds,
      mapping,
      nestedPaths,
      availableLocales,
      debug
    );
    return;
  }

  // Clean up any existing TEMP fields only (not the links field which may have data from previous conversions)
  if (!debug.skipDeletions) {
    const existingFields = await client.fields.list(mcField.parentModelId);
    for (const field of existingFields) {
      // Only clean up temp fields - NOT the actual links/link field which may have data
      if (field.api_key === `${originalApiKey}_temp_links`) {
        try {
          debugLog(debug, 'FIELD', `Cleaning up existing temp field: ${field.api_key}`);
          await client.fields.destroy(field.id);
          await delay(300);
        } catch (e) {
          console.warn(`Could not clean up existing field ${field.api_key}:`, e);
        }
      }
    }
  } else {
    debugLog(debug, 'FIELD', '⚠️  Skipping cleanup of temp fields (debug mode)');
  }

  // In debug mode, we always create a NEW field with suffix, never modify/delete the original
  if (debug.skipDeletions) {
    // DEBUG MODE: Create a new links field with suffix, keep original untouched
    const newApiKey = `${originalApiKey}_links${debugSuffix}`;
    
    debugLog(debug, 'FIELD', `DEBUG MODE: Creating new links field: ${newApiKey} (original field preserved)`);
    
    const newLinksFieldData = {
      label: `${originalLabel} (Links)${debugSuffix ? ` ${debugSuffix}` : ''}`,
      api_key: newApiKey,
      field_type: newFieldType,
      localized: mcField.localized,
      validators: isSingleBlock 
        ? { item_item_type: { item_types: [newModelId] } }
        : { items_item_type: { item_types: [newModelId] } },
      appearance: {
        editor: isSingleBlock ? 'link_embed' : 'links_embed',
        parameters: {},
        addons: [],
      },
      position: originalPosition + 1,
      fieldset: originalFieldset || undefined,
    } as Parameters<typeof client.fields.create>[1];
    
    const newLinksField = await client.fields.create(mcField.parentModelId, newLinksFieldData);
    debugLog(debug, 'FIELD', `Created links field: ${newLinksField.api_key} (id: ${newLinksField.id})`);

    // Migrate data to the new field - use nested migration if field is inside a block
    if (mcField.parentIsBlock) {
      // Field is inside a block - use nested migration
      const nestedPath = findNestedPathForField(nestedPaths, mcField);
      if (nestedPath) {
        debugLog(debug, 'FIELD', `Using nested migration for block field`);
        await migrateNestedBlockFieldData(
          client,
          nestedPath,
          originalApiKey,
          newLinksField.api_key,
          blockIdToRemove,
          mapping,
          isSingleBlock,
          availableLocales,
          debug
        );
      } else {
        debugLog(debug, 'FIELD', `Warning: Could not find nested path for block field, skipping data migration`);
      }
    } else {
      // Field is in a root model - use standard migration
      await migrateFieldData(
        client,
        mcField.parentModelId,
        originalApiKey,
        newLinksField.api_key,
        mcField.localized,
        blockIdToRemove,
        mapping,
        isSingleBlock,
        debug
      );
    }

    debugLog(debug, 'FIELD', `DEBUG MODE: Original field "${originalApiKey}" preserved, new field "${newApiKey}" created`);
    return;
  }

  // NORMAL MODE (non-debug) - original behavior
  if (remainingBlockIds.length > 0 && !isSingleBlock) {
    // Check if a links field already exists from a previous conversion
    const existingFields = await client.fields.list(mcField.parentModelId);
    const expectedLinksApiKey = `${originalApiKey}_links`;
    
    const existingLinksField = existingFields.find(f => f.api_key === expectedLinksApiKey);
    
    if (existingLinksField) {
      // APPEND MODE: Links field already exists - add new model to validators and append data
      debugLog(debug, 'FIELD', `Found existing links field "${existingLinksField.api_key}" - appending new model`);
      
      // Get current allowed item types and add the new model
      const currentValidators = existingLinksField.validators as Record<string, unknown>;
      const currentItemsValidator = currentValidators.items_item_type as { item_types?: string[] } | undefined;
      const currentItemTypes = currentItemsValidator?.item_types || [];
      
      // Add new model ID if not already present
      if (!currentItemTypes.includes(newModelId)) {
        const updatedItemTypes = [...currentItemTypes, newModelId];
        debugLog(debug, 'FIELD', `Updating validators: ${currentItemTypes.length} → ${updatedItemTypes.length} allowed types`);
        
        await client.fields.update(existingLinksField.id, {
          validators: {
            ...currentValidators,
            items_item_type: {
              item_types: updatedItemTypes,
            },
          },
        });
      }
      
      // Append new links to the existing field data (don't replace!)
      if (mcField.parentIsBlock) {
        const nestedPath = findNestedPathForField(nestedPaths, mcField);
        if (nestedPath) {
          debugLog(debug, 'FIELD', `Using nested migration to APPEND data for block field`);
          await migrateNestedBlockFieldDataAppend(
            client,
            nestedPath,
            originalApiKey,
            existingLinksField.api_key,
            blockIdToRemove,
            mapping,
            availableLocales,
            debug
          );
        } else {
          debugLog(debug, 'FIELD', `Warning: Could not find nested path for block field, skipping data migration`);
        }
      } else {
        await migrateFieldDataAppend(
          client,
          mcField.parentModelId,
          originalApiKey,
          existingLinksField.api_key,
          mcField.localized,
          blockIdToRemove,
          mapping,
          debug
        );
      }
    } else {
      // CREATE MODE: No existing links field - create a new one
      debugLog(debug, 'FIELD', 'Creating additional links field (keeping modular content for remaining blocks)');
      
      const newLinksFieldData: Parameters<typeof client.fields.create>[1] = {
        label: `${originalLabel} (Links)`,
        api_key: `${originalApiKey}_links`,
        field_type: 'links',
        localized: mcField.localized,
        validators: {
          items_item_type: {
            item_types: [newModelId],
          },
        },
        appearance: {
          editor: 'links_embed',
          parameters: {},
          addons: [],
        },
        position: originalPosition + 1,
      };
      
      // Preserve fieldset if the original field was in one
      if (originalFieldset) {
        newLinksFieldData.fieldset = originalFieldset;
      }
      
      const newLinksField = await client.fields.create(mcField.parentModelId, newLinksFieldData);
      debugLog(debug, 'FIELD', `Created links field: ${newLinksField.api_key}`);

      // Migrate data: read from old field, extract matching blocks, write link IDs to new field
      if (mcField.parentIsBlock) {
        // Field is inside a block - use nested migration
        const nestedPath = findNestedPathForField(nestedPaths, mcField);
        if (nestedPath) {
          debugLog(debug, 'FIELD', `Using nested migration for block field`);
          await migrateNestedBlockFieldData(
            client,
            nestedPath,
            originalApiKey,
            newLinksField.api_key,
            blockIdToRemove,
            mapping,
            isSingleBlock,
            availableLocales,
            debug
          );
        } else {
          debugLog(debug, 'FIELD', `Warning: Could not find nested path for block field, skipping data migration`);
        }
      } else {
        // Field is in a root model - use standard migration
        await migrateFieldData(
          client,
          mcField.parentModelId,
          originalApiKey,
          newLinksField.api_key,
          mcField.localized,
          blockIdToRemove,
          mapping,
          isSingleBlock,
          debug
        );
      }
    }

    // Update the original field to remove the converted block type
    debugLog(debug, 'FIELD', `Updating original field to remove block type ${blockIdToRemove}`);
    await client.fields.update(mcField.id, {
      validators: {
        ...currentField.validators,
        rich_text_blocks: {
          item_types: remainingBlockIds,
        },
      },
    });
  } else {
    // Full replacement: This is the LAST block in the modular content field
    // BUT we need to check if a _links field already exists from previous conversions!
    debugLog(debug, 'FIELD', 'Full field replacement (last block being converted)');
    
    const existingFields = await client.fields.list(mcField.parentModelId);
    const expectedLinksApiKey = `${originalApiKey}_links`;
    const existingLinksField = existingFields.find(f => f.api_key === expectedLinksApiKey);
    
    if (existingLinksField) {
      // APPEND TO EXISTING: A _links field exists from previous conversions
      // We should APPEND to it, not replace it!
      debugLog(debug, 'FIELD', `Found existing links field "${expectedLinksApiKey}" - appending and deleting modular content`);
      
      // Get current allowed item types and add the new model
      const currentValidators = existingLinksField.validators as Record<string, unknown>;
      const currentItemsValidator = currentValidators.items_item_type as { item_types?: string[] } | undefined;
      const currentItemTypes = currentItemsValidator?.item_types || [];
      
      // Add new model ID if not already present
      if (!currentItemTypes.includes(newModelId)) {
        const updatedItemTypes = [...currentItemTypes, newModelId];
        debugLog(debug, 'FIELD', `Updating validators: ${currentItemTypes.length} → ${updatedItemTypes.length} allowed types`);
        
        await client.fields.update(existingLinksField.id, {
          validators: {
            ...currentValidators,
            items_item_type: {
              item_types: updatedItemTypes,
            },
          },
        });
      }
      
      // Append new links to the existing field data
      if (mcField.parentIsBlock) {
        const nestedPath = findNestedPathForField(nestedPaths, mcField);
        if (nestedPath) {
          debugLog(debug, 'FIELD', `Using nested migration to APPEND data for block field`);
          await migrateNestedBlockFieldDataAppend(
            client,
            nestedPath,
            originalApiKey,
            existingLinksField.api_key,
            blockIdToRemove,
            mapping,
            availableLocales,
            debug
          );
        }
      } else {
        await migrateFieldDataAppend(
          client,
          mcField.parentModelId,
          originalApiKey,
          existingLinksField.api_key,
          mcField.localized,
          blockIdToRemove,
          mapping,
          debug
        );
      }
      
      // Now delete the original modular content field (it's empty now)
      debugLog(debug, 'FIELD', `Deleting original modular content field: ${originalApiKey}`);
      await client.fields.destroy(mcField.id);
      await delay(500);
      
      // Rename the links field to take over the original position (optional - keep as _links for clarity)
      // Actually, let's move the links field to the original position
      await client.fields.update(existingLinksField.id, {
        position: originalPosition,
      });
      
    } else {
      // NO EXISTING LINKS FIELD: Standard full replacement
      
      // Step 1: Create temp field with new type
      const tempApiKey = `${originalApiKey}_temp_links`;
      const tempFieldData = {
        label: `${originalLabel} (Temp)`,
        api_key: tempApiKey,
        field_type: newFieldType,
        localized: mcField.localized,
        validators: isSingleBlock 
          ? { item_item_type: { item_types: [newModelId] } }
          : { items_item_type: { item_types: [newModelId] } },
        appearance: {
          editor: isSingleBlock ? 'link_embed' : 'links_embed',
          parameters: {},
          addons: [],
        },
        position: originalPosition + 1,
        fieldset: originalFieldset || undefined,
      } as Parameters<typeof client.fields.create>[1];
      
      await client.fields.create(mcField.parentModelId, tempFieldData);
      debugLog(debug, 'FIELD', `Created temp field: ${tempApiKey}`);

      // Step 2: Migrate data from old field to temp field
      if (mcField.parentIsBlock) {
        // Field is inside a block - use nested migration
        const nestedPath = findNestedPathForField(nestedPaths, mcField);
        if (nestedPath) {
          debugLog(debug, 'FIELD', `Using nested migration for block field`);
          await migrateNestedBlockFieldData(
            client,
            nestedPath,
            originalApiKey,
            tempApiKey,
            blockIdToRemove,
            mapping,
            isSingleBlock,
            availableLocales,
            debug
          );
        } else {
          debugLog(debug, 'FIELD', `Warning: Could not find nested path for block field, skipping data migration`);
        }
      } else {
        // Field is in a root model - use standard migration
        await migrateFieldData(
          client,
          mcField.parentModelId,
          originalApiKey,
          tempApiKey,
          mcField.localized,
          blockIdToRemove,
          mapping,
          isSingleBlock,
          debug
        );
      }

      // Step 3: Delete old field
      debugLog(debug, 'FIELD', `Deleting original field: ${originalApiKey}`);
      await client.fields.destroy(mcField.id);
      await delay(500);

      // Step 4: Find the temp field and update it with the original api_key
      const fieldsAfterDelete = await client.fields.list(mcField.parentModelId);
      const tempField = fieldsAfterDelete.find(f => f.api_key === tempApiKey);
      if (tempField) {
        debugLog(debug, 'FIELD', `Renaming temp field ${tempApiKey} → ${originalApiKey}`);
        const updateData: Parameters<typeof client.fields.update>[1] = {
          label: originalLabel,
          api_key: originalApiKey,
          position: originalPosition,
          hint: originalHint,
        };
        
        // Preserve fieldset if the original field was in one
        if (originalFieldset) {
          updateData.fieldset = originalFieldset;
        }
        
        await client.fields.update(tempField.id, updateData);
      }
    }
  }
  
  debugLog(debug, 'FIELD', `Field conversion complete for "${originalApiKey}"`);
}

/**
 * Migrates data from an old modular content field to a new links field
 */
async function migrateFieldData(
  client: CMAClient,
  modelId: string,
  oldFieldApiKey: string,
  newFieldApiKey: string,
  isLocalized: boolean,
  targetBlockId: string,
  mapping: BlockMigrationMapping,
  isSingleValue: boolean,
  debug: DebugOptions = DEFAULT_DEBUG_OPTIONS
): Promise<void> {
  debugLog(debug, 'DATA', `Migrating data from "${oldFieldApiKey}" to "${newFieldApiKey}"`, {
    modelId,
    isLocalized,
    isSingleValue,
    mappingCount: Object.keys(mapping).length,
  });

  let recordCount = 0;
  let updatedCount = 0;

  // Read all records of this model
  for await (const record of client.items.listPagedIterator({
    filter: { type: modelId },
    nested: true,
  })) {
    recordCount++;
    const oldValue = record[oldFieldApiKey];
    if (!oldValue) {
      debugLog(debug, 'DATA', `Record ${record.id}: No value in "${oldFieldApiKey}", skipping`);
      continue;
    }

    let newValue: unknown;

    if (isLocalized && typeof oldValue === 'object' && !Array.isArray(oldValue)) {
      // Localized field - process each locale
      const localizedValue: Record<string, unknown> = {};
      for (const [locale, localeValue] of Object.entries(oldValue as Record<string, unknown>)) {
        localizedValue[locale] = extractLinksFromValue(
          localeValue,
          targetBlockId,
          mapping,
          isSingleValue
        );
      }
      newValue = localizedValue;
      debugLog(debug, 'DATA', `Record ${record.id}: Processed ${Object.keys(localizedValue).length} locales`);
    } else {
      // Non-localized field
      newValue = extractLinksFromValue(oldValue, targetBlockId, mapping, isSingleValue);
      debugLog(debug, 'DATA', `Record ${record.id}: Extracted links:`, newValue);
    }

    // Update the record with the new field value
    try {
      await client.items.update(record.id, {
        [newFieldApiKey]: newValue,
      });
      updatedCount++;
      debugLog(debug, 'DATA', `Record ${record.id}: Updated successfully`);
    } catch (error) {
      console.error(`Failed to migrate data for record ${record.id}:`, error);
      debugLog(debug, 'ERROR', `Record ${record.id}: Failed to update`, error);
      // Continue with other records
    }
  }

  debugLog(debug, 'DATA', `Data migration complete: ${updatedCount}/${recordCount} records updated`);
}

/**
 * Appends links from a new block conversion to an existing links field.
 * This is used when converting multiple blocks from the same modular content field.
 * Unlike migrateFieldData which replaces, this function preserves existing links and adds new ones.
 */
async function migrateFieldDataAppend(
  client: CMAClient,
  modelId: string,
  oldFieldApiKey: string,
  linksFieldApiKey: string,
  isLocalized: boolean,
  targetBlockId: string,
  mapping: BlockMigrationMapping,
  debug: DebugOptions = DEFAULT_DEBUG_OPTIONS
): Promise<void> {
  debugLog(debug, 'DATA_APPEND', `Appending data from "${oldFieldApiKey}" to existing "${linksFieldApiKey}"`, {
    modelId,
    isLocalized,
    mappingCount: Object.keys(mapping).length,
  });

  let recordCount = 0;
  let updatedCount = 0;

  // IMPORTANT: Read WITHOUT nested to get raw field values (just IDs, not expanded objects)
  // This is critical because when nested: true, the links field returns full record objects
  // which can be empty arrays if the records don't exist yet or have issues
  for await (const record of client.items.listPagedIterator({
    filter: { type: modelId },
    // DO NOT use nested: true here - we need raw field values for links
  })) {
    recordCount++;
    
    // For the modular content field (oldFieldApiKey), we need to fetch with nested to get block data
    // Let's fetch the same record with nested: true to get the block data
    const nestedRecord = await client.items.find(record.id, { nested: true });
    const oldValue = nestedRecord[oldFieldApiKey];
    
    // For the links field, use the NON-nested value (raw IDs)
    const existingLinksValue = record[linksFieldApiKey];
    
    if (!oldValue) {
      debugLog(debug, 'DATA_APPEND', `Record ${record.id}: No value in "${oldFieldApiKey}", skipping`);
      continue;
    }

    let newValue: unknown;

    if (isLocalized && typeof oldValue === 'object' && !Array.isArray(oldValue)) {
      // Localized field - process each locale
      const localizedValue: Record<string, unknown> = {};
      const existingLocalized = (existingLinksValue || {}) as Record<string, unknown>;
      const oldValueLocalized = oldValue as Record<string, unknown>;
      
      // Get all locales from BOTH the old value AND the existing links field
      // This ensures we don't lose data from locales that don't have new blocks to convert
      const allLocales = new Set([
        ...Object.keys(oldValueLocalized),
        ...Object.keys(existingLocalized),
      ]);
      
      for (const locale of allLocales) {
        const localeValue = oldValueLocalized[locale];
        
        // Extract new links from this locale (may be empty if no blocks to convert)
        const newLinks = localeValue 
          ? extractLinksFromValue(localeValue, targetBlockId, mapping, false) as string[]
          : [];
        // Get existing links for this locale
        // Handle both cases: links as IDs (strings) or as objects with id property
        const rawExistingLinks = existingLocalized[locale] || [];
        const existingLinks: string[] = [];
        if (Array.isArray(rawExistingLinks)) {
          for (const link of rawExistingLinks) {
            if (typeof link === 'string') {
              existingLinks.push(link);
            } else if (link && typeof link === 'object' && 'id' in link) {
              existingLinks.push((link as { id: string }).id);
            }
          }
        }
        
        // Combine: existing links + new links (avoiding duplicates)
        const combinedLinks = [...existingLinks];
        for (const link of newLinks) {
          if (!combinedLinks.includes(link)) {
            combinedLinks.push(link);
          }
        }
        localizedValue[locale] = combinedLinks;
      }
      newValue = localizedValue;
      debugLog(debug, 'DATA_APPEND', `Record ${record.id}: Appended links for ${Object.keys(localizedValue).length} locales`);
    } else {
      // Non-localized field
      const newLinks = extractLinksFromValue(oldValue, targetBlockId, mapping, false) as string[];
      // Handle both cases: links as IDs (strings) or as objects with id property
      const rawExistingLinks = existingLinksValue || [];
      const existingLinks: string[] = [];
      if (Array.isArray(rawExistingLinks)) {
        for (const link of rawExistingLinks) {
          if (typeof link === 'string') {
            existingLinks.push(link);
          } else if (link && typeof link === 'object' && 'id' in link) {
            existingLinks.push((link as { id: string }).id);
          }
        }
      }
      // Combine: existing links + new links (avoiding duplicates)
      const combinedLinks = [...existingLinks];
      for (const link of newLinks) {
        if (!combinedLinks.includes(link)) {
          combinedLinks.push(link);
        }
      }
      newValue = combinedLinks;
      debugLog(debug, 'DATA_APPEND', `Record ${record.id}: Appended ${newLinks.length} new links to ${existingLinks.length} existing`);
    }

    // Update the record with the combined value
    try {
      await client.items.update(record.id, {
        [linksFieldApiKey]: newValue,
      });
      updatedCount++;
      debugLog(debug, 'DATA_APPEND', `Record ${record.id}: Updated successfully`);
    } catch (error) {
      console.error(`Failed to append data for record ${record.id}:`, error);
      debugLog(debug, 'ERROR', `Record ${record.id}: Failed to update`, error);
    }
  }

  debugLog(debug, 'DATA_APPEND', `Data append complete: ${updatedCount}/${recordCount} records updated`);
}

/**
 * Appends links from a new block conversion to an existing links field for nested blocks.
 * Similar to migrateFieldDataAppend but handles fields inside blocks.
 */
async function migrateNestedBlockFieldDataAppend(
  client: CMAClient,
  nestedPath: NestedBlockPath,
  oldFieldApiKey: string,
  linksFieldApiKey: string,
  targetBlockId: string,
  mapping: BlockMigrationMapping,
  availableLocales: string[],
  debug: DebugOptions = DEFAULT_DEBUG_OPTIONS
): Promise<void> {
  debugLog(debug, 'NESTED_APPEND', `Appending nested field data`, {
    rootModel: nestedPath.rootModelName,
    path: nestedPath.path.map(p => p.fieldApiKey).join(' → '),
    oldField: oldFieldApiKey,
    linksField: linksFieldApiKey,
  });

  let recordCount = 0;
  let updatedCount = 0;

  const pathToParentBlock = nestedPath.path.slice(0, -1);
  
  debugLog(debug, 'NESTED_APPEND', `Path to parent: ${pathToParentBlock.map(p => p.fieldApiKey).join(' → ') || '(root)'}`);

  // Create the update function that will be called for PARENT blocks
  const updateBlockFn = (blockData: Record<string, unknown>, locale: string | null): Record<string, unknown> => {
    // Get the old field value (modular content with blocks)
    const oldValue = getNestedFieldValueFromBlock(blockData, oldFieldApiKey);
    // Get the existing links field value
    const existingLinksValue = getNestedFieldValueFromBlock(blockData, linksFieldApiKey);
    
    if (!oldValue) {
      return blockData;
    }

    // Extract new links from the old value
    const newLinks = extractLinksFromValue(oldValue, targetBlockId, mapping, false) as string[];
    // Get existing links
    const existingLinks = (existingLinksValue || []) as string[];
    
    // Combine: existing links + new links (avoiding duplicates)
    const combinedLinks = [...existingLinks];
    for (const link of newLinks) {
      if (!combinedLinks.includes(link)) {
        combinedLinks.push(link);
      }
    }

    debugLog(debug, 'NESTED_APPEND', `Appended ${newLinks.length} new links to ${existingLinks.length} existing`, { locale });

    // Set the combined links value
    return setNestedFieldValueInBlock(blockData, linksFieldApiKey, combinedLinks);
  };

  // Query records from the ROOT model
  for await (const record of client.items.listPagedIterator({
    filter: { type: nestedPath.rootModelId },
    nested: true,
  })) {
    recordCount++;

    const rootFieldApiKey = nestedPath.path[0].fieldApiKey;
    const rootFieldValue = record[rootFieldApiKey];

    if (!rootFieldValue) {
      continue;
    }

    let result: { updated: boolean; newValue: unknown };

    if (pathToParentBlock.length === 0) {
      result = traverseAndUpdateNestedBlocksAtLevel(
        rootFieldValue,
        nestedPath.path[0],
        updateBlockFn,
        debug
      );
    } else {
      result = traverseAndUpdateNestedBlocks(
        rootFieldValue,
        pathToParentBlock,
        0,
        updateBlockFn,
        debug
      );
    }

    if (result.updated) {
      try {
        // Ensure all locales are present
        let updateValue = result.newValue;
        
        if (nestedPath.path[0].localized && typeof result.newValue === 'object' && !Array.isArray(result.newValue)) {
          const localizedValue = result.newValue as Record<string, unknown>;
          const completeLocalizedValue: Record<string, unknown> = {};
          
          for (const locale of availableLocales) {
            if (locale in localizedValue) {
              completeLocalizedValue[locale] = localizedValue[locale];
            } else {
              const originalLocaleValue = (rootFieldValue as Record<string, unknown>)?.[locale];
              completeLocalizedValue[locale] = originalLocaleValue !== undefined ? originalLocaleValue : null;
            }
          }
          
          updateValue = completeLocalizedValue;
        }
        
        await client.items.update(record.id, {
          [rootFieldApiKey]: updateValue,
        });
        updatedCount++;
      } catch (error) {
        console.error(`Failed to update record ${record.id} with nested data:`, error);
        throw error;
      }
    }
  }

  debugLog(debug, 'NESTED_APPEND', `Nested data append complete: ${updatedCount}/${recordCount} records updated`);
}

/**
 * Extracts link IDs from a field value (blocks array or structured text) based on the target block type and mapping.
 * For structured text fields, uses DAST traversal to find block references.
 */
function extractLinksFromValue(
  value: unknown,
  targetBlockId: string,
  mapping: BlockMigrationMapping,
  isSingleValue: boolean
): unknown {
  if (!value) {
    return isSingleValue ? null : [];
  }

  // Handle structured text fields - use DAST traversal
  if (isStructuredTextValue(value)) {
    const linkIds = extractLinksFromStructuredText(value, targetBlockId, mapping);
    return isSingleValue ? (linkIds[0] || null) : linkIds;
  }

  // Handle single block (not an array)
  if (isSingleValue && typeof value === 'object' && !Array.isArray(value)) {
    const blockObj = value as Record<string, unknown>;
    const blockTypeId = getBlockTypeId(blockObj);
    const blockId = getBlockId(blockObj);
    
    if (blockTypeId === targetBlockId && blockId && mapping[blockId]) {
      return mapping[blockId];
    }
    return null;
  }

  // Handle array of blocks
  if (!Array.isArray(value)) {
    return isSingleValue ? null : [];
  }

  const linkIds: string[] = [];
  for (const block of value) {
    if (!block || typeof block !== 'object') continue;

    const blockObj = block as Record<string, unknown>;
    const blockTypeId = getBlockTypeId(blockObj);
    const blockId = getBlockId(blockObj);

    if (blockTypeId === targetBlockId && blockId && mapping[blockId]) {
      linkIds.push(mapping[blockId]);
    }
  }

  return isSingleValue ? (linkIds[0] || null) : linkIds;
}

/**
 * Cleans up nested blocks from the original modular content field.
 * This is used in non-debug mode when doing partial replacement (keeping both fields).
 * It removes the target blocks from the original field since they've been migrated to the links field.
 */
async function cleanupNestedBlocksFromOriginalField(
  client: CMAClient,
  rootModelId: string,
  paths: NestedBlockPath[],
  targetBlockId: string,
  debug: DebugOptions = DEFAULT_DEBUG_OPTIONS
): Promise<void> {
  debugLog(debug, 'CLEANUP', `Cleaning up nested blocks in model ${rootModelId}`, {
    pathCount: paths.length,
    targetBlockId,
  });

  let recordCount = 0;
  let updatedCount = 0;

  // Iterate through all records of the root model
  for await (const record of client.items.listPagedIterator({
    filter: { type: rootModelId },
    nested: true,
  })) {
    recordCount++;
    let needsUpdate = false;
    const updates: Record<string, unknown> = {};

    // Process each path that applies to this root model
    for (const path of paths) {
      const rootFieldApiKey = path.path[0].fieldApiKey;
      const rootFieldValue = record[rootFieldApiKey];

      if (!rootFieldValue) continue;

      // Use the traverse function to remove target blocks from the original field
      const result = traverseAndRemoveBlocks(
        rootFieldValue,
        path.path,
        0,
        targetBlockId,
        debug
      );

      if (result.updated) {
        needsUpdate = true;
        updates[rootFieldApiKey] = result.newValue;
        debugLog(debug, 'CLEANUP', `Record ${record.id}: Removed blocks from "${rootFieldApiKey}"`);
      }
    }

    if (needsUpdate) {
      try {
        debugLog(debug, 'CLEANUP', `Record ${record.id}: Applying cleanup updates`);
        await client.items.update(record.id, updates);
        updatedCount++;
      } catch (error) {
        console.error(`Failed to cleanup record ${record.id}:`, error);
        debugLog(debug, 'ERROR', `Record ${record.id}: Failed to cleanup nested blocks`, error);
        throw error;
      }
    }
  }

  debugLog(debug, 'CLEANUP', `Nested block cleanup complete: ${updatedCount}/${recordCount} records updated`);
}

/**
 * Recursively traverses nested block structures and removes target blocks.
 * Used for cleaning up the original modular content field after migration.
 */
function traverseAndRemoveBlocks(
  fieldValue: unknown,
  path: NestedBlockPath['path'],
  pathIndex: number,
  targetBlockId: string,
  debug: DebugOptions = DEFAULT_DEBUG_OPTIONS
): { updated: boolean; newValue: unknown } {
  if (pathIndex >= path.length || !fieldValue) {
    return { updated: false, newValue: fieldValue };
  }

  const currentStep = path[pathIndex];
  const isLastStep = pathIndex === path.length - 1;

  // Helper to process and filter blocks at this level
  const processBlocksArray = (blocks: unknown[]): { updated: boolean; newBlocks: unknown[] } => {
    let updated = false;
    const newBlocks: unknown[] = [];

    for (const block of blocks) {
      if (!block || typeof block !== 'object') {
        newBlocks.push(block);
        continue;
      }

      const blockObj = block as Record<string, unknown>;
      const blockTypeId = getBlockTypeId(blockObj);

      if (isLastStep) {
        // At final level - remove target blocks, keep others
        if (blockTypeId === targetBlockId) {
          // Don't add this block - it's being removed
          updated = true;
          debugLog(debug, 'CLEANUP', `Removing block of type ${targetBlockId}`);
        } else {
          newBlocks.push(block);
        }
      } else {
        // Check if this block matches the expected type at this path level
        if (blockTypeId === currentStep.expectedBlockTypeId) {
          // Need to go deeper - get the next field value from this block
          const nextStep = path[pathIndex + 1];
          const nestedFieldValue = getNestedFieldValueFromBlock(blockObj, nextStep.fieldApiKey);

          if (nestedFieldValue !== undefined) {
            // Recurse into the nested field
            const result = traverseAndRemoveBlocks(
              nestedFieldValue,
              path,
              pathIndex + 1,
              targetBlockId,
              debug
            );

            if (result.updated) {
              // Update the block with the cleaned nested field value
              const updatedBlock = setNestedFieldValueInBlock(
                blockObj,
                nextStep.fieldApiKey,
                result.newValue
              );
              newBlocks.push(updatedBlock);
              updated = true;
            } else {
              newBlocks.push(block);
            }
          } else {
            newBlocks.push(block);
          }
        } else {
          // Block type doesn't match - keep as is
          newBlocks.push(block);
        }
      }
    }

    return { updated, newBlocks };
  };

  // Handle localized vs non-localized fields
  if (currentStep.localized && typeof fieldValue === 'object' && !Array.isArray(fieldValue)) {
    // Localized field - process each locale
    let anyUpdated = false;
    const newLocalizedValue: Record<string, unknown> = {};

    for (const [locale, localeValue] of Object.entries(fieldValue as Record<string, unknown>)) {
      const blocks = extractBlocksFromValue(localeValue, currentStep.fieldType);
      
      if (blocks.length > 0) {
        const result = processBlocksArray(blocks);
        if (result.updated) {
          anyUpdated = true;
          // Reconstruct the value based on field type
          if (currentStep.fieldType === 'single_block') {
            newLocalizedValue[locale] = result.newBlocks[0] || null;
          } else if (currentStep.fieldType === 'structured_text') {
            // For structured text with inlined blocks, update the blocks in the DAST document
            newLocalizedValue[locale] = reconstructStructuredTextWithUpdatedBlocks(
              localeValue as Record<string, unknown>,
              result.newBlocks
            );
          } else {
            newLocalizedValue[locale] = result.newBlocks;
          }
        } else {
          newLocalizedValue[locale] = localeValue;
        }
      } else {
        newLocalizedValue[locale] = localeValue;
      }
    }

    return { updated: anyUpdated, newValue: newLocalizedValue };
  } else {
    // Non-localized field
    const blocks = extractBlocksFromValue(fieldValue, currentStep.fieldType);
    
    if (blocks.length > 0) {
      const result = processBlocksArray(blocks);
      if (result.updated) {
        // Reconstruct the value based on field type
        if (currentStep.fieldType === 'single_block') {
          return { updated: true, newValue: result.newBlocks[0] || null };
        } else if (currentStep.fieldType === 'structured_text') {
          // For structured text with inlined blocks, update the blocks in the DAST document
          return {
            updated: true,
            newValue: reconstructStructuredTextWithUpdatedBlocks(
              fieldValue as Record<string, unknown>,
              result.newBlocks
            ),
          };
        } else {
          return { updated: true, newValue: result.newBlocks };
        }
      }
    }

    return { updated: false, newValue: fieldValue };
  }
}

/**
 * Optional cleanup: Delete the original block model
 */
export async function deleteOriginalBlock(client: CMAClient, blockId: string): Promise<void> {
  await client.itemTypes.destroy(blockId);
}

/**
 * Rename the converted model to have the same name and api_key as the original block.
 * Also updates the corresponding menu item's label.
 * This should be called AFTER the original block has been deleted (to free up the name/api_key).
 * 
 * Note: DatoCMS requires regular model api_keys to be plural. If the original block's api_key
 * doesn't end with 's', we'll try to keep it as-is first, and if that fails, we'll add 's'.
 */
export async function renameModelToOriginal(
  client: CMAClient,
  modelId: string,
  originalName: string,
  originalApiKey: string
): Promise<{ success: boolean; finalName: string; finalApiKey: string; error?: string }> {
  try {
    // Sanitize the api_key - must be lowercase alphanumeric with underscores only
    let targetApiKey = originalApiKey
      .toLowerCase()
      .replace(/[^a-z0-9_]/g, '_')
      .replace(/_+/g, '_')
      .replace(/^_|_$/g, '');
    
    let finalName = originalName;
    let finalApiKey = targetApiKey;
    let partialError: string | undefined;
    
    // First, try to update the model with the exact original api_key
    try {
      await client.itemTypes.update(modelId, {
        name: originalName,
        api_key: targetApiKey,
      });
      finalApiKey = targetApiKey;
    } catch (firstError) {
      // If the exact api_key failed (possibly due to plural requirement), try with 's' suffix
      const errorMessage = firstError instanceof Error ? firstError.message : String(firstError);
      
      // Check if this is a validation error about api_key
      if (errorMessage.toLowerCase().includes('api_key') || errorMessage.toLowerCase().includes('plural')) {
        // Try adding 's' if it doesn't already end with 's'
        if (!targetApiKey.endsWith('s')) {
          const pluralApiKey = targetApiKey + 's';
          
          try {
            await client.itemTypes.update(modelId, {
              name: originalName,
              api_key: pluralApiKey,
            });
            finalApiKey = pluralApiKey;
          } catch (secondError) {
            // Both attempts failed - try updating just the name
            try {
              await client.itemTypes.update(modelId, {
                name: originalName,
              });
              
              // Get the current api_key
              const model = await client.itemTypes.find(modelId);
              finalApiKey = model.api_key;
              partialError = `Could not update api_key to "${targetApiKey}" (DatoCMS may require plural api_keys), kept existing api_key`;
            } catch {
              // Even name update failed
              throw secondError;
            }
          }
        }
      } else {
        // For other errors, try updating just the name
        try {
          await client.itemTypes.update(modelId, {
            name: originalName,
          });
          
          const model = await client.itemTypes.find(modelId);
          finalApiKey = model.api_key;
          partialError = `Could not update api_key: ${errorMessage}`;
        } catch {
          throw firstError;
        }
      }
    }
    
    // Now update the menu item's label to match the original name
    try {
      // Find the menu item associated with this model
      const menuItems = await client.menuItems.list();
      const menuItem = menuItems.find((item) => {
        // Menu items have an item_type relationship pointing to the model
        return item.item_type?.id === modelId;
      });
      
      if (menuItem) {
        await client.menuItems.update(menuItem.id, {
          label: originalName,
        });
      }
    } catch (menuError) {
      // Menu item update failed, but model was updated successfully
      const menuErrorMessage = menuError instanceof Error ? menuError.message : String(menuError);
      if (partialError) {
        partialError += `; Menu item label could not be updated: ${menuErrorMessage}`;
      } else {
        partialError = `Menu item label could not be updated: ${menuErrorMessage}`;
      }
    }
    
    return {
      success: true,
      finalName,
      finalApiKey,
      error: partialError,
    };
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    
    // Get current model info for error reporting
    try {
      const model = await client.itemTypes.find(modelId);
      return {
        success: false,
        finalName: model.name,
        finalApiKey: model.api_key,
        error: `Failed to rename model: ${errorMessage}`,
      };
    } catch {
      return {
        success: false,
        finalName: '',
        finalApiKey: '',
        error: `Failed to rename model: ${errorMessage}`,
      };
    }
  }
}
