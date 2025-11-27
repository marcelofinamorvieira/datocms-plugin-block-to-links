import type {
  CMAClient,
  BlockAnalysis,
  ConversionResult,
  ProgressCallback,
  BlockMigrationMapping,
  ModularContentFieldInfo,
  NestedBlockPath,
  GroupedBlockInstance,
} from '../types';

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
  addInlineItemsAlongsideBlocks,
} from './dast';


/**
 * Logs the start of a major operation
 */

/**
 * Logs a sub-section within an operation
 */

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
 * @returns Updated field value and whether any updates were made
 */
function traverseAndUpdateNestedBlocks(
  fieldValue: unknown,
  path: NestedBlockPath['path'],
  pathIndex: number,
  updateFn: (blockData: Record<string, unknown>, locale: string | null) => Record<string, unknown>
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
              updateFn
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
  availableLocales: string[]
): Promise<void> {

  let recordCount = 0;
  let updatedCount = 0;

  // The path goes from root model TO the target blocks (e.g., sections → socials → Social Media Icons)
  // But we need to update the PARENT block (e.g., Hero Section) with the new link field
  // So we use a path that stops one level before the final block level
  
  // For a single-step path (field is directly in a root block), we update at that level
  // For multi-step paths, we need to stop at the parent block level
  const pathToParentBlock = nestedPath.path.slice(0, -1);
  

  // Query records from the ROOT model (not the block)
  for await (const record of client.items.listPagedIterator({
    filter: { type: nestedPath.rootModelId },
    nested: true,
    version: 'current', // Fetch draft version to get latest changes
  })) {
    recordCount++;

    // Get the root field value (first step in the path)
    const rootFieldApiKey = nestedPath.path[0].fieldApiKey;
    const rootFieldValue = record[rootFieldApiKey];

    if (!rootFieldValue) {
      continue;
    }

    // Create the update function that will be called for PARENT blocks
    // This function reads the old field, extracts block IDs, maps them to record IDs, and sets the new field
    const updateBlockFn = (blockData: Record<string, unknown>, _locale: string | null): Record<string, unknown> => {
      // Get the old field value from the parent block (e.g., Hero Section's "socials" field)
      const oldValue = getNestedFieldValueFromBlock(blockData, oldFieldApiKey);
      
      if (!oldValue) {
        return blockData;
      }

      // Extract link IDs from the old value (the array of blocks in the old field)
      const newValue = extractLinksFromValue(oldValue, targetBlockId, mapping, isSingleValue);


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
        updateBlockFn
    );
    } else {
      // Multi-step path: we need to traverse to the parent block level first
      result = traverseAndUpdateNestedBlocks(
        rootFieldValue,
        pathToParentBlock,
        0,
        updateBlockFn
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

}

/**
 * Traverses blocks at a single level and applies the update function
 * Used when we need to update blocks at the first level of nesting
 */
function traverseAndUpdateNestedBlocksAtLevel(
  fieldValue: unknown,
  step: NestedBlockPath['path'][0],
  updateFn: (blockData: Record<string, unknown>, locale: string | null) => Record<string, unknown>
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
  fullyReplace: boolean = false
): Promise<ConversionResult> {
  const totalSteps = fullyReplace ? 7 : 6;
  let migratedRecordsCount = 0;
  let convertedFieldsCount = 0;

  try {
    // Step 1: Analyze the block
    onProgress({
      currentStep: 1,
      totalSteps,
      stepDescription: 'Analyzing block structure...',
      percentage: 5,
    });


    const analysis = await analyzeBlock(client, blockId);


    if (analysis.modularContentFields.length === 0) {
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

    // Determine if any path is in a localized context
    const shouldLocalizeFields = nestedPaths.some(p => p.isInLocalizedContext);

    // Fetch available locales from site settings
    // Always needed for structured text updates to avoid "removing locales" issues
    const site = await client.site.find();
    const availableLocales = site.locales;

    // Step 2: Create new model with same fields
    onProgress({
      currentStep: 2,
      totalSteps,
      stepDescription: `Creating new model "${analysis.block.name}"${shouldLocalizeFields ? ' (with localized fields)' : ''}...`,
      percentage: 15,
      details: `Copying ${analysis.fields.length} fields${shouldLocalizeFields ? ' as localized' : ''}`,
    });

    if (shouldLocalizeFields) {
    }

    const newModel = await createNewModelFromBlock(client, analysis, shouldLocalizeFields);


    // Step 3: Migrate block content to new records
    onProgress({
      currentStep: 3,
      totalSteps,
      stepDescription: `Migrating block content to new records${shouldLocalizeFields ? ' (grouped by locale)' : ''}...`,
      percentage: 30,
      details: `Processing ${nestedPaths.length} nested paths`,
    });


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


      let mapping: BlockMigrationMapping;

      if (nestedPath.isInLocalizedContext) {
        // Use grouped migration for localized contexts - merges locale data into single records
        
        const groupedInstances = await getGroupedBlockInstances(client, nestedPath, blockId);
        
        mapping = await migrateGroupedBlocksToRecords(
          client,
          groupedInstances,
          newModel.id,
          availableLocales,
          globalMapping,
          (count) => {
            migratedRecordsCount = count;
          }
        );
      } else {
        // Use standard migration for non-localized contexts
        // BUT if shouldLocalizeFields is true, we need to wrap values in localized hashes
        // because the model was created with all fields localized
        
        mapping = await migrateBlocksToRecordsNested(
          client,
          nestedPath,
          blockId,
          newModel.id,
          globalMapping,
          (count) => {
            migratedRecordsCount = count;
          },
          shouldLocalizeFields,  // Pass forceLocalizedFields flag
          availableLocales       // Pass available locales
        );
      }

      Object.assign(globalMapping, mapping);
    }


    // Step 4: Convert modular content fields to links fields (includes data migration)
    onProgress({
      currentStep: 4,
      totalSteps,
      stepDescription: 'Converting field types and migrating data...',
      percentage: 55,
    });


    for (let i = 0; i < analysis.modularContentFields.length; i++) {
      const mcField = analysis.modularContentFields[i];
      onProgress({
        currentStep: 4,
        totalSteps,
        stepDescription: `Converting "${mcField.parentModelName}.${mcField.apiKey}" to links field...`,
        percentage: 55 + (15 * i) / analysis.modularContentFields.length,
      });


      await convertModularContentToLinksField(
        client,
        mcField,
        newModel.id,
        blockId,
        globalMapping,
        nestedPaths,
        availableLocales,
        fullyReplace
      );
      convertedFieldsCount++;

    }

    // Step 5: Cleanup nested block references
    // This step removes converted blocks from original fields
    // when using partial replacement (keeping both modular content and links fields)
    onProgress({
      currentStep: 5,
      totalSteps,
      stepDescription: 'Cleaning up nested block references...',
      percentage: 75,
    });

    {
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


      let rootModelIndex = 0;
      for (const [rootModelId, paths] of pathsByRootModel) {
        const rootModelName = paths[0].rootModelName;
        onProgress({
          currentStep: 5,
          totalSteps,
          stepDescription: `Cleaning up nested blocks in "${rootModelName}"...`,
          percentage: 75 + (15 * rootModelIndex) / pathsByRootModel.size,
        });


        await cleanupNestedBlocksFromOriginalField(
          client,
          rootModelId,
          paths,
          blockId
        );
        rootModelIndex++;
      }
    }

    // Step 6 or 7: Delete original block (if fully replacing)
    if (fullyReplace) {
      onProgress({
        currentStep: 6,
        totalSteps,
        stepDescription: 'Deleting original block model...',
        percentage: 90,
      });

      await deleteOriginalBlock(client, blockId);

      onProgress({
        currentStep: 7,
        totalSteps,
        stepDescription: 'Conversion complete!',
        percentage: 100,
        details: `Created model "${newModel.api_key}" with ${migratedRecordsCount} records, original block deleted`,
      });
    } else {
      // Step 6: Done (without deletion)
      onProgress({
        currentStep: 6,
        totalSteps,
        stepDescription: 'Conversion complete!',
        percentage: 100,
        details: `Created model "${newModel.api_key}" with ${migratedRecordsCount} records`,
      });
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
  fieldIdMapping: Record<string, string>
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
    } else if (oldTitleFieldId) {
      // The referenced field hasn't been created yet or doesn't exist
      // This can happen if the field order is incorrect or the title field is missing
      // Remove the validator to avoid API errors - the field will work without it
      delete updatedValidators.slug_title_field;
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
  
  // Calculate how much space we have for the base api_key
  // We need room for: {base}_conv{suffix} or {base}_conv{suffix}_{counter}
  const suffixPart = `_conv`;
  const maxBaseLength = API_KEY_MAX_LENGTH - suffixPart.length - 3; // -3 for potential counter "_99"
  
  // Truncate the base api_key if needed
  const truncatedBlockApiKey = sanitizedBlockApiKey.length > maxBaseLength 
    ? sanitizedBlockApiKey.slice(0, maxBaseLength).replace(/_$/, '') // Remove trailing underscore if truncation creates one
    : sanitizedBlockApiKey;
  
  const baseApiKey = `${truncatedBlockApiKey}${suffixPart}`;
  const baseName = analysis.block.name.replace(/[^\w\s-]/g, '').trim() || 'Converted Block';
  
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
  let finalName = `${baseName} (Converted)`;
  let counter = 0;

  
  // Check for uniqueness using the SANITIZED api_key
  while (
    existingModels.some((m) => m.api_key === finalApiKey) ||
    existingModels.some((m) => m.name === finalName)
  ) {
    counter++;
    const letterSuffix = counterToLetterSuffix(counter);
    
    // Use letter suffix instead of number to avoid being stripped by sanitization
    finalApiKey = sanitizeApiKey(`${baseApiKey}_${letterSuffix}`);
    finalName = `${baseName} (Converted ${counter})`;

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
  

  
  const newModel = await client.itemTypes.create({
    name: finalName,
    api_key: validatedApiKey,
    modular_block: false,
    sortable: true,
    draft_mode_active: false,
    collection_appearance: 'table',
  });


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
        fieldIdMapping
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
      } else {
        newFieldData.default_value = field.defaultValue;
      }
    }

    // Cast to expected type - field data is dynamically constructed from source field
    const newField = await client.fields.create(newModel.id, newFieldData as Parameters<typeof client.fields.create>[1]);

    // Store the mapping from old field ID to new field ID
    fieldIdMapping[field.id] = newField.id;

    if (!titleFieldId && field.fieldType === 'string') {
      titleFieldId = newField.id;
    }
  }

  if (titleFieldId) {
    await client.itemTypes.update(newModel.id, {
      title_field: { type: 'field', id: titleFieldId },
    });
  }


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
  forceLocalizedFields: boolean = false,
  availableLocales: string[] = []
): Promise<BlockMigrationMapping> {
  const mapping: BlockMigrationMapping = {};
  let migratedCount = Object.keys(existingMapping).length;

  // Get all block instances following the nested path
  const blockInstances = await getAllBlockInstancesNested(client, nestedPath, blockId);

  // Filter out blocks that were already migrated
  const uniqueBlocks = new Map<string, (typeof blockInstances)[0]>();
  for (const instance of blockInstances) {
    if (!uniqueBlocks.has(instance.blockId) && !existingMapping[instance.blockId]) {
      uniqueBlocks.set(instance.blockId, instance);
    }
  }

  const blocksArray = Array.from(uniqueBlocks.values());

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
        } else {
          recordData = sanitizedData;
        }
        

        const newRecord = await client.items.create({
          item_type: { type: 'item_type', id: newModelId },
          ...recordData,
        });

        mapping[instance.blockId] = newRecord.id;
        migratedCount++;
        onMigrated(migratedCount);

      } catch (error) {
        console.error(`Failed to migrate block ${instance.blockId}:`, error);
        throw error;
      }
    },
    200
  );

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
  onMigrated: (count: number) => void
): Promise<BlockMigrationMapping> {
  const mapping: BlockMigrationMapping = {};
  let migratedCount = Object.keys(existingMapping).length;


  // Filter out groups where all block IDs were already migrated
  const groupsToMigrate = groupedInstances.filter(group => {
    return !group.allBlockIds.every(id => existingMapping[id]);
  });


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

        if (hasDefaultData) {
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

      } catch (error) {
        console.error(`Failed to migrate grouped block ${group.groupKey}:`, error);
        throw error;
      }
    },
    200
  );

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
  availableLocales: string[]
): Promise<void> {
    


  let recordCount = 0;
  let updatedCount = 0;

  for await (const record of client.items.listPagedIterator({
    filter: { type: modelId },
    nested: true,
    version: 'current', // Fetch draft version to get latest changes
  })) {
    recordCount++;
    const fieldValue = record[fieldApiKey];
    if (!fieldValue) {
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
            const transformed = transformDastBlocksToLinks(localeValue, targetBlockId, mapping);
            if (transformed) {
              localizedValue[locale] = transformed;
              hasChanges = true;
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
      const transformed = transformDastBlocksToLinks(fieldValue, targetBlockId, mapping);
      if (transformed) {
        newValue = transformed;
        hasChanges = true;
      }
    }

    if (hasChanges && newValue) {
      try {
        await client.items.update(record.id, {
          [fieldApiKey]: newValue,
        });
        updatedCount++;
      } catch (error) {
      }
    }
  }

}

/**
 * Migrates structured text field data for PARTIAL mode.
 * Adds inlineItem nodes alongside existing blocks (keeps blocks in place).
 */
async function migrateStructuredTextFieldDataPartial(
  client: CMAClient,
  modelId: string,
  fieldApiKey: string,
  isLocalized: boolean,
  targetBlockId: string,
  mapping: BlockMigrationMapping,
  availableLocales: string[]
): Promise<void> {
  for await (const record of client.items.listPagedIterator({
    filter: { type: modelId },
    nested: true,
    version: 'current', // Fetch draft version to get latest changes
  })) {
    const fieldValue = record[fieldApiKey];
    if (!fieldValue) {
      continue;
    }

    let newValue: unknown = null;
    let hasChanges = false;

    if (isLocalized && typeof fieldValue === 'object' && !Array.isArray(fieldValue)) {
      // Localized field - process each locale
      const localizedValue: Record<string, unknown> = {};
      const sourceLocaleValues = fieldValue as Record<string, unknown>;
      
      for (const locale of availableLocales) {
        const localeValue = sourceLocaleValues[locale];
        
        if (localeValue !== undefined) {
          if (isStructuredTextValue(localeValue)) {
            // Use addInlineItemsAlongsideBlocks instead of transformDastBlocksToLinks
            const transformed = addInlineItemsAlongsideBlocks(localeValue, targetBlockId, mapping);
            if (transformed) {
              localizedValue[locale] = transformed;
              hasChanges = true;
            } else {
              localizedValue[locale] = localeValue;
            }
          } else {
            localizedValue[locale] = localeValue;
          }
        } else {
          localizedValue[locale] = null;
        }
      }
      
      newValue = localizedValue;
    } else if (isStructuredTextValue(fieldValue)) {
      // Non-localized field
      const transformed = addInlineItemsAlongsideBlocks(fieldValue, targetBlockId, mapping);
      if (transformed) {
        newValue = transformed;
        hasChanges = true;
      }
    }

    if (hasChanges && newValue) {
      try {
        await client.items.update(record.id, {
          [fieldApiKey]: newValue,
        });
      } catch (error) {
        console.error(`Failed to update record ${record.id} with partial structured text:`, error);
      }
    }
  }
}

/**
 * Migrates nested structured text field data for PARTIAL mode.
 * Adds inlineItem nodes alongside existing blocks (keeps blocks in place).
 */
async function migrateNestedStructuredTextFieldDataPartial(
  client: CMAClient,
  nestedPath: NestedBlockPath,
  fieldApiKey: string,
  targetBlockId: string,
  mapping: BlockMigrationMapping
): Promise<void> {
  // Query records from the ROOT model
  for await (const record of client.items.listPagedIterator({
    filter: { type: nestedPath.rootModelId },
    nested: true,
    version: 'current', // Fetch draft version to get latest changes
  })) {
    // Get the root field value (first step in the path)
    const rootFieldApiKey = nestedPath.path[0].fieldApiKey;
    const rootFieldValue = record[rootFieldApiKey];

    if (!rootFieldValue) {
      continue;
    }

    // Create the update function that will be called for parent blocks
    const updateBlockFn = (blockData: Record<string, unknown>, _locale: string | null): Record<string, unknown> => {
      const stValue = getNestedFieldValueFromBlock(blockData, fieldApiKey);
      
      if (!stValue || !isStructuredTextValue(stValue)) {
        return blockData;
      }

      // Use addInlineItemsAlongsideBlocks instead of transformDastBlocksToLinks
      const transformed = addInlineItemsAlongsideBlocks(stValue, targetBlockId, mapping);
      if (transformed) {
        return setNestedFieldValueInBlock(blockData, fieldApiKey, transformed);
      }

      return blockData;
    };

    // Use the same traversal logic as migrateNestedStructuredTextFieldData
    const pathToParentBlock = nestedPath.path.slice(0, -1);
    
    let result: { updated: boolean; newValue: unknown };

    if (pathToParentBlock.length === 0) {
      result = traverseAndUpdateNestedBlocksAtLevel(
        rootFieldValue,
        nestedPath.path[0],
        updateBlockFn
    );
    } else {
      result = traverseAndUpdateNestedBlocks(
        rootFieldValue,
        pathToParentBlock,
        0,
        updateBlockFn
    );
    }

    if (result.updated) {
      try {
        await client.items.update(record.id, {
          [rootFieldApiKey]: result.newValue,
        });
      } catch (error) {
        console.error(`Failed to update record ${record.id} with nested partial structured text:`, error);
        throw error;
      }
    }
  }
}

/**
 * Handles conversion of structured text fields.
 * Unlike modular content fields, structured text fields:
 * 1. Keep the same field type (structured_text)
 * 2. Transform the DAST document in-place to replace block nodes with inlineItem nodes
 * 3. Update validators to remove the block type and add the new model as a linkable type
 * 
 * @param fullyReplaceBlock - When true, transforms DAST and removes block type from validators.
 *                            When false (partial mode), only adds link type to validators,
 *                            leaving blocks in place for manual conversion later.
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
  fullyReplaceBlock: boolean
): Promise<void> {
  
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
  }

  // IMPORTANT: Always preserve the block type during data migration
  // This prevents DatoCMS from stripping blocks before we can transform them
  if (currentBlocksValidator) {
    phase1Validators.structured_text_blocks = currentBlocksValidator;
  }

  // Apply Phase 1 validator update

  await client.fields.update(mcField.id, {
    validators: phase1Validators,
  });

  // PARTIAL MODE: When fullyReplaceBlock is false, we add the link type to validators
  // AND add inlineItem nodes alongside the blocks (keeping blocks in place).
  if (!fullyReplaceBlock) {
    // Phase 1.5: Add inlineItem nodes alongside blocks in the DAST
    // This must happen AFTER Phase 1 validator update so DatoCMS accepts the inlineItem nodes
    if (mcField.parentIsBlock) {
      // Field is inside a block - need to use nested migration
      const nestedPath = findNestedPathForField(nestedPaths, mcField);
      if (nestedPath) {
        await migrateNestedStructuredTextFieldDataPartial(
          client,
          nestedPath,
          mcField.apiKey,
          blockIdToRemove,
          mapping
        );
      }
    } else {
      // Field is in a root model - use standard migration
      await migrateStructuredTextFieldDataPartial(
        client,
        mcField.parentModelId,
        mcField.apiKey,
        mcField.localized,
        blockIdToRemove,
        mapping,
        availableLocales
      );
    }
    // In partial mode, we're done - blocks remain, inlineItems added, validators allow both
    return;
  }

  // PHASE 2: Migrate the DAST data (transform blocks to inlineItems)
  // This must happen AFTER Phase 1 validator update, otherwise DatoCMS will reject the records
  // At this point, blocks are still valid because we kept the block type in validators
  if (mcField.parentIsBlock) {
    // Field is inside a block - need to use nested migration
    const nestedPath = findNestedPathForField(nestedPaths, mcField);
    if (nestedPath) {
      await migrateNestedStructuredTextFieldData(
        client,
        nestedPath,
        mcField.apiKey,
        blockIdToRemove,
        mapping
    );
    } else {
    }
  } else {
    // Field is in a root model - use standard migration
    await migrateStructuredTextFieldData(
      client,
      mcField.parentModelId,
      mcField.apiKey,
      mcField.localized,
      blockIdToRemove,
      mapping,
      availableLocales
    );
  }

  // PHASE 3: Now remove the block type from validators
  // The blocks have been transformed to inlineItems, so they no longer need to be valid
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
  
  await client.fields.update(mcField.id, {
    validators: phase3Validators,
  });
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
  mapping: BlockMigrationMapping
): Promise<void> {
    


  let recordCount = 0;
  let updatedCount = 0;

  // Query records from the ROOT model
  for await (const record of client.items.listPagedIterator({
    filter: { type: nestedPath.rootModelId },
    nested: true,
    version: 'current', // Fetch draft version to get latest changes
  })) {
    recordCount++;

    // Get the root field value (first step in the path)
    const rootFieldApiKey = nestedPath.path[0].fieldApiKey;
    const rootFieldValue = record[rootFieldApiKey];

    if (!rootFieldValue) {
      continue;
    }

    // Create the update function that will be called for parent blocks
    const updateBlockFn = (blockData: Record<string, unknown>, _locale: string | null): Record<string, unknown> => {
      const stValue = getNestedFieldValueFromBlock(blockData, fieldApiKey);
      
      if (!stValue || !isStructuredTextValue(stValue)) {
        return blockData;
      }

      const transformed = transformDastBlocksToLinks(stValue, targetBlockId, mapping);
      if (transformed) {
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
        updateBlockFn
    );
    } else {
      result = traverseAndUpdateNestedBlocks(
        rootFieldValue,
        pathToParentBlock,
        0,
        updateBlockFn
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

}

/**
 * Converts a modular content field to a links field, including data migration.
 * Handles both top-level fields (in root models) and nested fields (inside blocks).
 * 
 * For structured_text fields, transforms the DAST in-place and updates validators
 * rather than creating a separate links field.
 * 
 * @param fullyReplaceBlock - When true, transforms data and removes block type from validators.
 *                            When false (partial mode), only adds link type to validators.
 */
async function convertModularContentToLinksField(
  client: CMAClient,
  mcField: ModularContentFieldInfo,
  newModelId: string,
  blockIdToRemove: string,
  mapping: BlockMigrationMapping,
  nestedPaths: NestedBlockPath[],
  availableLocales: string[],
  fullyReplaceBlock: boolean
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
      fullyReplaceBlock
    );
    return;
  }

  // Clean up any existing TEMP fields only (not the links field which may have data from previous conversions)
  const existingFields = await client.fields.list(mcField.parentModelId);
  for (const field of existingFields) {
    // Only clean up temp fields - NOT the actual links/link field which may have data
    if (field.api_key === `${originalApiKey}_temp_links`) {
      try {
        await client.fields.destroy(field.id);
        await delay(300);
      } catch (e) {
        console.warn(`Could not clean up existing field ${field.api_key}:`, e);
      }
    }
  }
  if (remainingBlockIds.length > 0 && !isSingleBlock) {
    // Check if a links field already exists from a previous conversion
    const existingFields = await client.fields.list(mcField.parentModelId);
    const expectedLinksApiKey = `${originalApiKey}_links`;
    
    const existingLinksField = existingFields.find(f => f.api_key === expectedLinksApiKey);
    
    if (existingLinksField) {
      // APPEND MODE: Links field already exists - add new model to validators and append data
      
      // Get current allowed item types and add the new model
      const currentValidators = existingLinksField.validators as Record<string, unknown>;
      const currentItemsValidator = currentValidators.items_item_type as { item_types?: string[] } | undefined;
      const currentItemTypes = currentItemsValidator?.item_types || [];
      
      // Add new model ID if not already present
      if (!currentItemTypes.includes(newModelId)) {
        const updatedItemTypes = [...currentItemTypes, newModelId];
        
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
          await migrateNestedBlockFieldDataAppend(
            client,
            nestedPath,
            originalApiKey,
            existingLinksField.api_key,
            blockIdToRemove,
            mapping,
            availableLocales
    );
        } else {
        }
      } else {
        await migrateFieldDataAppend(
          client,
          mcField.parentModelId,
          originalApiKey,
          existingLinksField.api_key,
          mcField.localized,
          blockIdToRemove,
          mapping
    );
      }
    } else {
      // CREATE MODE: No existing links field - create a new one
      
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

      // Migrate data: read from old field, extract matching blocks, write link IDs to new field
      if (mcField.parentIsBlock) {
        // Field is inside a block - use nested migration
        const nestedPath = findNestedPathForField(nestedPaths, mcField);
        if (nestedPath) {
          await migrateNestedBlockFieldData(
            client,
            nestedPath,
            originalApiKey,
            newLinksField.api_key,
            blockIdToRemove,
            mapping,
            isSingleBlock,
            availableLocales
    );
        } else {
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
          isSingleBlock
    );
      }
    }

    // Update the original field to remove the converted block type
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
    
    const existingFields = await client.fields.list(mcField.parentModelId);
    const expectedLinksApiKey = `${originalApiKey}_links`;
    const existingLinksField = existingFields.find(f => f.api_key === expectedLinksApiKey);
    
    if (existingLinksField) {
      // APPEND TO EXISTING: A _links field exists from previous conversions
      // We should APPEND to it, not replace it!
      
      // Get current allowed item types and add the new model
      const currentValidators = existingLinksField.validators as Record<string, unknown>;
      const currentItemsValidator = currentValidators.items_item_type as { item_types?: string[] } | undefined;
      const currentItemTypes = currentItemsValidator?.item_types || [];
      
      // Add new model ID if not already present
      if (!currentItemTypes.includes(newModelId)) {
        const updatedItemTypes = [...currentItemTypes, newModelId];
        
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
          await migrateNestedBlockFieldDataAppend(
            client,
            nestedPath,
            originalApiKey,
            existingLinksField.api_key,
            blockIdToRemove,
            mapping,
            availableLocales
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
          mapping
    );
      }
      
      // Now delete the original modular content field (it's empty now)
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

      // Step 2: Migrate data from old field to temp field
      if (mcField.parentIsBlock) {
        // Field is inside a block - use nested migration
        const nestedPath = findNestedPathForField(nestedPaths, mcField);
        if (nestedPath) {
          await migrateNestedBlockFieldData(
            client,
            nestedPath,
            originalApiKey,
            tempApiKey,
            blockIdToRemove,
            mapping,
            isSingleBlock,
            availableLocales
    );
        } else {
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
          isSingleBlock
    );
      }

      // Step 3: Delete old field
      await client.fields.destroy(mcField.id);
      await delay(500);

      // Step 4: Find the temp field and update it with the original api_key
      const fieldsAfterDelete = await client.fields.list(mcField.parentModelId);
      const tempField = fieldsAfterDelete.find(f => f.api_key === tempApiKey);
      if (tempField) {
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
  isSingleValue: boolean
): Promise<void> {

  let recordCount = 0;
  let updatedCount = 0;

  // Read all records of this model
  for await (const record of client.items.listPagedIterator({
    filter: { type: modelId },
    nested: true,
    version: 'current', // Fetch draft version to get latest changes
  })) {
    recordCount++;
    const oldValue = record[oldFieldApiKey];
    if (!oldValue) {
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
    } else {
      // Non-localized field
      newValue = extractLinksFromValue(oldValue, targetBlockId, mapping, isSingleValue);
    }

    // Update the record with the new field value
    try {
      await client.items.update(record.id, {
        [newFieldApiKey]: newValue,
      });
      updatedCount++;
    } catch (error) {
      console.error(`Failed to migrate data for record ${record.id}:`, error);
      // Continue with other records
    }
  }

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
  mapping: BlockMigrationMapping
): Promise<void> {

  let recordCount = 0;
  let updatedCount = 0;

  // IMPORTANT: Read WITHOUT nested to get raw field values (just IDs, not expanded objects)
  // This is critical because when nested: true, the links field returns full record objects
  // which can be empty arrays if the records don't exist yet or have issues
  for await (const record of client.items.listPagedIterator({
    filter: { type: modelId },
    version: 'current', // Fetch draft version to get latest changes
    // DO NOT use nested: true here - we need raw field values for links
  })) {
    recordCount++;
    
    // For the modular content field (oldFieldApiKey), we need to fetch with nested to get block data
    // Let's fetch the same record with nested: true to get the block data
    const nestedRecord = await client.items.find(record.id, { nested: true, version: 'current' });
    const oldValue = nestedRecord[oldFieldApiKey];
    
    // For the links field, use the NON-nested value (raw IDs)
    const existingLinksValue = record[linksFieldApiKey];
    
    if (!oldValue) {
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
    }

    // Update the record with the combined value
    try {
      await client.items.update(record.id, {
        [linksFieldApiKey]: newValue,
      });
      updatedCount++;
    } catch (error) {
      console.error(`Failed to append data for record ${record.id}:`, error);
    }
  }

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
  availableLocales: string[]
): Promise<void> {

  let recordCount = 0;
  let updatedCount = 0;

  const pathToParentBlock = nestedPath.path.slice(0, -1);
  

  // Create the update function that will be called for PARENT blocks
  const updateBlockFn = (blockData: Record<string, unknown>, _locale: string | null): Record<string, unknown> => {
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


    // Set the combined links value
    return setNestedFieldValueInBlock(blockData, linksFieldApiKey, combinedLinks);
  };

  // Query records from the ROOT model
  for await (const record of client.items.listPagedIterator({
    filter: { type: nestedPath.rootModelId },
    nested: true,
    version: 'current', // Fetch draft version to get latest changes
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
        updateBlockFn
    );
    } else {
      result = traverseAndUpdateNestedBlocks(
        rootFieldValue,
        pathToParentBlock,
        0,
        updateBlockFn
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
 * This is used when doing partial replacement (keeping both fields).
 * It removes the target blocks from the original field since they've been migrated to the links field.
 */
async function cleanupNestedBlocksFromOriginalField(
  client: CMAClient,
  rootModelId: string,
  paths: NestedBlockPath[],
  targetBlockId: string
): Promise<void> {

  let recordCount = 0;
  let updatedCount = 0;

  // Iterate through all records of the root model
  for await (const record of client.items.listPagedIterator({
    filter: { type: rootModelId },
    nested: true,
    version: 'current', // Fetch draft version to get latest changes
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
        targetBlockId
    );

      if (result.updated) {
        needsUpdate = true;
        updates[rootFieldApiKey] = result.newValue;
      }
    }

    if (needsUpdate) {
      try {
        await client.items.update(record.id, updates);
        updatedCount++;
      } catch (error) {
        console.error(`Failed to cleanup record ${record.id}:`, error);
        throw error;
      }
    }
  }

}

/**
 * Recursively traverses nested block structures and removes target blocks.
 * Used for cleaning up the original modular content field after migration.
 */
function traverseAndRemoveBlocks(
  fieldValue: unknown,
  path: NestedBlockPath['path'],
  pathIndex: number,
  targetBlockId: string
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
              targetBlockId
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
