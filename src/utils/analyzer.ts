import type {
  CMAClient,
  BlockAnalysis,
  FieldInfo,
  ModularContentFieldInfo,
  NestedBlockPath,
  GroupedBlockInstance,
  StructuredTextValue,
  DastBlockRecord,
} from '../types';
import {
  isStructuredTextValue,
  findBlockNodesInDast,
} from './dast';

// Type definitions for caches
type ItemTypeInfo = {
  id: string;
  name: string;
  api_key: string;
  modular_block: boolean;
};

type FieldCacheEntry = {
  id: string;
  label: string;
  api_key: string;
  field_type: string;
  localized: boolean;
  validators: Record<string, unknown>;
  position: number;
  hint: string | null;
};

// Cache for item types to avoid repeated API calls
let itemTypesCache: ItemTypeInfo[] | null = null;

// Cache for fields by item type
const fieldsCache: Map<string, FieldCacheEntry[]> = new Map();

/**
 * Clears caches - call this before starting a new analysis
 */
export function clearCaches(): void {
  itemTypesCache = null;
  fieldsCache.clear();
}

/**
 * Gets all item types (cached)
 */
async function getAllItemTypes(client: CMAClient): Promise<ItemTypeInfo[]> {
  if (itemTypesCache === null) {
    const itemTypes = await client.itemTypes.list();
    itemTypesCache = itemTypes.map(it => ({
      id: it.id,
      name: it.name,
      api_key: it.api_key,
      modular_block: it.modular_block,
    }));
  }
  return itemTypesCache;
}

/**
 * Gets fields for an item type (cached)
 */
async function getFieldsForItemType(client: CMAClient, itemTypeId: string): Promise<FieldCacheEntry[]> {
  const cached = fieldsCache.get(itemTypeId);
  if (cached) {
    return cached;
  }
  
  const fields = await client.fields.list(itemTypeId);
  const mapped = fields.map(f => ({
    id: f.id,
    label: f.label,
    api_key: f.api_key,
    field_type: f.field_type,
    localized: f.localized,
    validators: f.validators as Record<string, unknown>,
    position: f.position,
    hint: f.hint,
  }));
  fieldsCache.set(itemTypeId, mapped);
  return mapped;
}

/**
 * Analyzes a block model to understand its structure and usage
 */
export async function analyzeBlock(
  client: CMAClient,
  blockId: string
): Promise<BlockAnalysis> {
  clearCaches(); // Start fresh for each analysis

  // Get the block model
  const block = await client.itemTypes.find(blockId);

  if (!block.modular_block) {
    throw new Error(`Item type ${block.api_key} is not a block model`);
  }

  // Get all fields of the block
  const blockFields = await client.fields.list(blockId);
  const fields: FieldInfo[] = blockFields.map((field) => ({
    id: field.id,
    label: field.label,
    apiKey: field.api_key,
    fieldType: field.field_type,
    localized: field.localized,
    validators: field.validators as Record<string, unknown>,
    appearance: field.appearance as FieldInfo['appearance'],
    position: field.position,
    hint: field.hint || undefined,
    defaultValue: field.default_value,
  }));

  // Find all modular content fields that reference this block (including nested in other blocks)
  const modularContentFields = await findModularContentFieldsUsingBlock(
    client,
    blockId
  );

  // Build nested paths to root models for each field
  const nestedPaths = await buildNestedPathsToRootModels(client, modularContentFields, blockId);

  // Count affected records using the nested paths
  let totalAffectedRecords = 0;
  for (const nestedPath of nestedPaths) {
    const count = await countRecordsWithNestedBlock(client, nestedPath, blockId);
    totalAffectedRecords += count;
  }

  return {
    block: {
      id: block.id,
      name: block.name,
      apiKey: block.api_key,
    },
    fields,
    modularContentFields,
    totalAffectedRecords,
  };
}

/**
 * Finds all modular content fields (rich_text or structured_text) that use the specified block.
 * This includes fields in other blocks (for nested block scenarios).
 */
async function findModularContentFieldsUsingBlock(
  client: CMAClient,
  blockId: string
): Promise<ModularContentFieldInfo[]> {
  const result: ModularContentFieldInfo[] = [];
  const itemTypes = await getAllItemTypes(client);

  for (const itemType of itemTypes) {
    const fields = await getFieldsForItemType(client, itemType.id);

    for (const field of fields) {
      // Check if it's a modular content field (rich_text in API)
      if (field.field_type === 'rich_text') {
        const validators = field.validators;
        const richTextBlocks = validators.rich_text_blocks as
          | { item_types: string[] }
          | undefined;

        if (richTextBlocks?.item_types?.includes(blockId)) {
          result.push({
            id: field.id,
            label: field.label,
            apiKey: field.api_key,
            parentModelId: itemType.id,
            parentModelName: itemType.name,
            parentModelApiKey: itemType.api_key,
            parentIsBlock: itemType.modular_block,
            localized: field.localized,
            allowedBlockIds: richTextBlocks.item_types,
            position: field.position,
            hint: field.hint || undefined,
            fieldType: 'rich_text',
          });
        }
      }

      // Also check structured_text fields which can contain blocks
      if (field.field_type === 'structured_text') {
        const validators = field.validators;
        const structuredTextBlocks = validators.structured_text_blocks as
          | { item_types: string[] }
          | undefined;

        if (structuredTextBlocks?.item_types?.includes(blockId)) {
          result.push({
            id: field.id,
            label: field.label,
            apiKey: field.api_key,
            parentModelId: itemType.id,
            parentModelName: itemType.name,
            parentModelApiKey: itemType.api_key,
            parentIsBlock: itemType.modular_block,
            localized: field.localized,
            allowedBlockIds: structuredTextBlocks.item_types,
            position: field.position,
            hint: field.hint || undefined,
            fieldType: 'structured_text',
          });
        }
      }

      // Also check single_block fields which can contain exactly one block
      if (field.field_type === 'single_block') {
        const validators = field.validators;
        const singleBlockBlocks = validators.single_block_blocks as
          | { item_types: string[] }
          | undefined;

        if (singleBlockBlocks?.item_types?.includes(blockId)) {
          result.push({
            id: field.id,
            label: field.label,
            apiKey: field.api_key,
            parentModelId: itemType.id,
            parentModelName: itemType.name,
            parentModelApiKey: itemType.api_key,
            parentIsBlock: itemType.modular_block,
            localized: field.localized,
            allowedBlockIds: singleBlockBlocks.item_types,
            position: field.position,
            hint: field.hint || undefined,
            fieldType: 'single_block',
          });
        }
      }
    }
  }

  return result;
}

/**
 * Recursively finds all paths from root models (non-blocks) to modular content fields.
 * Handles arbitrarily deep nesting of blocks within blocks.
 */
export async function buildNestedPathsToRootModels(
  client: CMAClient,
  modularContentFields: ModularContentFieldInfo[],
  targetBlockId: string
): Promise<NestedBlockPath[]> {
  const result: NestedBlockPath[] = [];
  const itemTypes = await getAllItemTypes(client);

  for (const mcField of modularContentFields) {
    if (!mcField.parentIsBlock) {
      // Parent is a regular model - simple path
      const path = [{
        fieldApiKey: mcField.apiKey,
        expectedBlockTypeId: targetBlockId,
        localized: mcField.localized,
        fieldType: mcField.fieldType,
      }];
      result.push({
        rootModelId: mcField.parentModelId,
        rootModelName: mcField.parentModelName,
        rootModelApiKey: mcField.parentModelApiKey,
        path,
        fieldInfo: mcField,
        isInLocalizedContext: path.some(step => step.localized),
      });
    } else {
      // Parent is a block - need to recursively find paths to root models
      const pathsToParent = await findPathsToBlock(client, mcField.parentModelId, itemTypes, new Set());
      
      for (const pathToParent of pathsToParent) {
        // Append the current field to the path
        const fullPath = [
          ...pathToParent.path,
          {
            fieldApiKey: mcField.apiKey,
            expectedBlockTypeId: targetBlockId,
            localized: mcField.localized,
            fieldType: mcField.fieldType,
          },
        ];
        result.push({
          rootModelId: pathToParent.rootModelId,
          rootModelName: pathToParent.rootModelName,
          rootModelApiKey: pathToParent.rootModelApiKey,
          path: fullPath,
          fieldInfo: mcField,
          isInLocalizedContext: fullPath.some(step => step.localized),
        });
      }
    }
  }

  return result;
}

/**
 * Helper function to check if a block is in a localized context
 */
export function isBlockInLocalizedContext(nestedPath: NestedBlockPath): boolean {
  return nestedPath.isInLocalizedContext;
}

/**
 * Recursively finds all paths from root models to a specific block type.
 * Returns paths that lead to the block, not including the fields within the block.
 */
async function findPathsToBlock(
  client: CMAClient,
  blockId: string,
  itemTypes: ItemTypeInfo[],
  visitedBlocks: Set<string> // Prevent infinite loops with circular references
): Promise<Array<{
  rootModelId: string;
  rootModelName: string;
  rootModelApiKey: string;
  path: NestedBlockPath['path'];
}>> {
  // Prevent infinite loops
  if (visitedBlocks.has(blockId)) {
    return [];
  }
  visitedBlocks.add(blockId);

  const result: Array<{
    rootModelId: string;
    rootModelName: string;
    rootModelApiKey: string;
    path: NestedBlockPath['path'];
  }> = [];

  // Find all modular content fields that contain this block
  for (const itemType of itemTypes) {
    const fields = await getFieldsForItemType(client, itemType.id);

    for (const field of fields) {
      let containsBlock = false;
      let fieldType: 'rich_text' | 'structured_text' | 'single_block' = 'rich_text';
      
      if (field.field_type === 'rich_text') {
        const validators = field.validators;
        const richTextBlocks = validators.rich_text_blocks as
          | { item_types: string[] }
          | undefined;
        containsBlock = richTextBlocks?.item_types?.includes(blockId) ?? false;
        fieldType = 'rich_text';
      } else if (field.field_type === 'structured_text') {
        const validators = field.validators;
        const structuredTextBlocks = validators.structured_text_blocks as
          | { item_types: string[] }
          | undefined;
        containsBlock = structuredTextBlocks?.item_types?.includes(blockId) ?? false;
        fieldType = 'structured_text';
      } else if (field.field_type === 'single_block') {
        const validators = field.validators;
        const singleBlockBlocks = validators.single_block_blocks as
          | { item_types: string[] }
          | undefined;
        containsBlock = singleBlockBlocks?.item_types?.includes(blockId) ?? false;
        fieldType = 'single_block';
      }

      if (containsBlock) {
        if (!itemType.modular_block) {
          // Found a root model - this is a complete path
          result.push({
            rootModelId: itemType.id,
            rootModelName: itemType.name,
            rootModelApiKey: itemType.api_key,
            path: [{
              fieldApiKey: field.api_key,
              expectedBlockTypeId: blockId,
              localized: field.localized,
              fieldType,
            }],
          });
        } else {
          // Parent is also a block - recurse upward
          const pathsToParent = await findPathsToBlock(client, itemType.id, itemTypes, visitedBlocks);
          
          for (const pathToParent of pathsToParent) {
            result.push({
              rootModelId: pathToParent.rootModelId,
              rootModelName: pathToParent.rootModelName,
              rootModelApiKey: pathToParent.rootModelApiKey,
              path: [
                ...pathToParent.path,
                {
                  fieldApiKey: field.api_key,
                  expectedBlockTypeId: blockId,
                  localized: field.localized,
                  fieldType,
                },
              ],
            });
          }
        }
      }
    }
  }

  return result;
}

/**
 * Extracts the block type ID from a block object
 */
export function getBlockTypeId(block: Record<string, unknown>): string | undefined {
  // Check for __itemTypeId first (convenience property)
  if (typeof block.__itemTypeId === 'string') {
    return block.__itemTypeId;
  }

  // Check for relationships.item_type.data.id (nested structure from CMA client)
  const relationships = block.relationships as Record<string, unknown> | undefined;
  if (relationships) {
    const itemTypeRel = relationships.item_type as Record<string, unknown> | undefined;
    if (itemTypeRel) {
      const data = itemTypeRel.data as Record<string, unknown> | undefined;
      if (data && typeof data.id === 'string') {
        return data.id;
      }
    }
  }

  // Fallback: check for item_type directly (string or object with id)
  const itemType = block.item_type;
  if (typeof itemType === 'string') {
    return itemType;
  }
  if (itemType && typeof itemType === 'object') {
    const obj = itemType as Record<string, unknown>;
    if (typeof obj.id === 'string') {
      return obj.id;
    }
  }

  return undefined;
}

/**
 * Gets the block ID from a block object
 */
export function getBlockId(block: Record<string, unknown>): string | undefined {
  if (typeof block.id === 'string') {
    return block.id;
  }
  return undefined;
}

/**
 * Gets block attributes/data from a block object
 */
export function getBlockAttributes(block: Record<string, unknown>): Record<string, unknown> {
  const attributes = block.attributes as Record<string, unknown> | undefined;
  return attributes || {};
}

/**
 * Counts records that contain the target block following a nested path
 */
async function countRecordsWithNestedBlock(
  client: CMAClient,
  nestedPath: NestedBlockPath,
  targetBlockId: string
): Promise<number> {
  let count = 0;

  for await (const record of client.items.listPagedIterator({
    filter: { type: nestedPath.rootModelId },
    nested: true,
  })) {
    if (recordContainsBlockAtPath(record, nestedPath.path, targetBlockId)) {
      count++;
    }
  }

  return count;
}

/**
 * Checks if a record contains the target block following the given path
 */
function recordContainsBlockAtPath(
  record: Record<string, unknown>,
  path: NestedBlockPath['path'],
  targetBlockId: string
): boolean {
  return findBlocksAtPath(record, path, targetBlockId).length > 0;
}

/**
 * Extracts blocks from a field value based on field type.
 * - rich_text: value is directly an array of blocks
 * - structured_text: uses DAST traversal to find only blocks that are actually referenced
 *   in the document tree (via block/inlineBlock nodes)
 * - single_block: value is a single block object
 */
function extractBlocksFromFieldValue(
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
    // Structured text - we need to traverse the DAST document to find block references
    // Only return blocks that are actually referenced in the document tree
    return extractBlocksFromStructuredTextValue(fieldValue);
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
 * Extracts blocks from a structured text field value by:
 * 1. Traversing the DAST document to find block/inlineBlock node references
 * 2. Returning only the blocks that are actually referenced in the document
 * 
 * This is important because the blocks array may contain blocks that were
 * previously used but are no longer referenced in the document.
 */
function extractBlocksFromStructuredTextValue(fieldValue: unknown): DastBlockRecord[] {
  if (!isStructuredTextValue(fieldValue)) {
    // Fallback: if it's not a proper structured text value, try old approach
    if (typeof fieldValue === 'object' && fieldValue !== null) {
      const stValue = fieldValue as Record<string, unknown>;
      const blocks = stValue.blocks;
      if (Array.isArray(blocks)) {
        return blocks as DastBlockRecord[];
      }
    }
    return [];
  }

  const structuredText = fieldValue as StructuredTextValue;
  const blocks = structuredText.blocks || [];
  
  // Get document children and check for block/inlineBlock types
  const doc = structuredText.document as unknown as Record<string, unknown>;
  const children = doc?.children as unknown[] || [];
  const allChildTypes = children.map((c: unknown) => (c as Record<string, unknown>)?.type);
  
  // Check if there are any 'block' or 'inlineBlock' types in the children
  const hasBlockTypes = allChildTypes.some((t) => t === 'block' || t === 'inlineBlock');
  
  if (hasBlockTypes) {
    // Find the actual block nodes
    const blockChildren = children.filter((c: unknown) => {
      const child = c as Record<string, unknown>;
      return child?.type === 'block' || child?.type === 'inlineBlock';
    });
    
    // With nested: true, blocks are inlined in the document tree
    // The 'item' property contains the full block object, not just an ID
    const inlineBlocks: DastBlockRecord[] = blockChildren.map((child) => {
      const blockNode = child as Record<string, unknown>;
      const itemData = blockNode.item;
      
      // If item is an object (inlined block), extract it
      if (itemData && typeof itemData === 'object') {
        return itemData as DastBlockRecord;
      }
      // If item is just an ID string, we need to look it up in blocks array
      if (typeof itemData === 'string' && blocks.length > 0) {
        const found = blocks.find((b) => b.id === itemData);
        if (found) return found;
      }
      return null;
    }).filter((b): b is DastBlockRecord => b !== null);
    
    if (inlineBlocks.length > 0) {
      return inlineBlocks;
    }
  }
  
  // Fallback to the original approach for cases where blocks array is populated
  if (blocks.length === 0) {
    return [];
  }

  // Find all block/inlineBlock nodes in the document
  const blockNodes = findBlockNodesInDast(structuredText);
  
  if (blockNodes.length === 0) {
    return [];
  }

  // Get the IDs of blocks that are actually referenced
  const referencedBlockIds = new Set(blockNodes.map(node => node.itemId));
  
  // Return only blocks that are referenced in the document
  return blocks.filter(block => referencedBlockIds.has(block.id));
}

/**
 * Finds all target block instances in a record following the given path.
 * Returns array of { block, pathIndices } where pathIndices tracks the position at each level.
 */
export function findBlocksAtPath(
  record: Record<string, unknown>,
  path: NestedBlockPath['path'],
  targetBlockId: string
): Array<{
  block: Record<string, unknown>;
  pathIndices: number[]; // Index at each path level
  locale: string | null;
}> {
  const results: Array<{
    block: Record<string, unknown>;
    pathIndices: number[];
    locale: string | null;
  }> = [];

  function traverse(
    currentData: Record<string, unknown>,
    pathIndex: number,
    currentIndices: number[],
    locale: string | null
  ): void {
    if (pathIndex >= path.length) {
      return;
    }

    const step = path[pathIndex];
    const fieldValue = currentData[step.fieldApiKey];

    if (!fieldValue) return;

    const processBlocks = (blocks: unknown[], loc: string | null) => {
      if (!Array.isArray(blocks)) return;

      blocks.forEach((block, index) => {
        if (!block || typeof block !== 'object') return;
        const blockObj = block as Record<string, unknown>;
        const blockTypeId = getBlockTypeId(blockObj);

        if (pathIndex === path.length - 1) {
          // This is the final step - look for target blocks
          if (blockTypeId === targetBlockId) {
            results.push({
              block: blockObj,
              pathIndices: [...currentIndices, index],
              locale: loc,
            });
          }
        } else {
          // Intermediate step - check if this block matches expected type and recurse
          if (blockTypeId === step.expectedBlockTypeId) {
            // Get the block's attributes which contain its fields
            const blockData = getBlockAttributes(blockObj);
            traverse(
              { ...blockData, ...blockObj }, // Merge attributes with block for nested field access
              pathIndex + 1,
              [...currentIndices, index],
              loc
            );
          }
        }
      });
    };

    if (step.localized && typeof fieldValue === 'object' && !Array.isArray(fieldValue)) {
      // Localized field - iterate over locales
      for (const loc of Object.keys(fieldValue as Record<string, unknown>)) {
        const localeValue = (fieldValue as Record<string, unknown>)[loc];
        const blocks = extractBlocksFromFieldValue(localeValue, step.fieldType);
        processBlocks(blocks, loc);
      }
    } else {
      // Non-localized field
      const blocks = extractBlocksFromFieldValue(fieldValue, step.fieldType);
      processBlocks(blocks, locale);
    }
  }

  traverse(record, 0, [], null);
  return results;
}

/**
 * Gets all block instances of a specific type from all records, following nested paths.
 */
export async function getAllBlockInstancesNested(
  client: CMAClient,
  nestedPath: NestedBlockPath,
  targetBlockId: string
): Promise<
  Array<{
    rootRecordId: string;
    locale: string | null;
    blockData: Record<string, unknown>;
    blockId: string;
    pathIndices: number[];
  }>
> {
  const instances: Array<{
    rootRecordId: string;
    locale: string | null;
    blockData: Record<string, unknown>;
    blockId: string;
    pathIndices: number[];
  }> = [];

  for await (const record of client.items.listPagedIterator({
    filter: { type: nestedPath.rootModelId },
    nested: true,
  })) {
    const blocks = findBlocksAtPath(record, nestedPath.path, targetBlockId);

    for (const { block, pathIndices, locale } of blocks) {
      const blockData = getBlockAttributes(block);
      const id = getBlockId(block);

      instances.push({
        rootRecordId: record.id,
        locale,
        blockData,
        blockId: id || `${record.id}_${pathIndices.join('_')}`,
        pathIndices,
      });
    }
  }

  return instances;
}

/**
 * Gets block instances grouped by position across locales.
 * This is used for localized contexts where blocks at the same position
 * in different locales should be merged into a single record.
 */
export async function getGroupedBlockInstances(
  client: CMAClient,
  nestedPath: NestedBlockPath,
  targetBlockId: string
): Promise<GroupedBlockInstance[]> {
  // Map to collect blocks by group key (rootRecordId + pathIndices)
  const groupMap = new Map<string, {
    rootRecordId: string;
    pathIndices: number[];
    localeData: Record<string, Record<string, unknown>>;
    allBlockIds: string[];
  }>();

  for await (const record of client.items.listPagedIterator({
    filter: { type: nestedPath.rootModelId },
    nested: true,
  })) {
    const blocks = findBlocksAtPath(record, nestedPath.path, targetBlockId);

    for (const { block, pathIndices, locale } of blocks) {
      const blockData = getBlockAttributes(block);
      const blockId = getBlockId(block);
      
      // Create group key from record ID and position indices
      const groupKey = `${record.id}_${pathIndices.join('_')}`;
      
      // Get or create the group entry
      let group = groupMap.get(groupKey);
      if (!group) {
        group = {
          rootRecordId: record.id,
          pathIndices,
          localeData: {},
          allBlockIds: [],
        };
        groupMap.set(groupKey, group);
      }
      
      // Store the block data for this locale
      // Use '__default__' for non-localized contexts
      const localeKey = locale || '__default__';
      group.localeData[localeKey] = blockData;
      
      // Track all block IDs for mapping
      if (blockId) {
        group.allBlockIds.push(blockId);
      } else {
        // Generate synthetic ID if none exists
        group.allBlockIds.push(`${record.id}_${pathIndices.join('_')}_${localeKey}`);
      }
    }
  }

  // Convert map to array of GroupedBlockInstance
  const result: GroupedBlockInstance[] = [];
  for (const [groupKey, group] of groupMap) {
    result.push({
      groupKey,
      rootRecordId: group.rootRecordId,
      pathIndices: group.pathIndices,
      localeData: group.localeData,
      allBlockIds: group.allBlockIds,
      referenceBlockId: group.allBlockIds[0] || groupKey,
    });
  }

  return result;
}

// Keep legacy function for backwards compatibility
export async function getAllBlockInstances(
  client: CMAClient,
  modularContentField: ModularContentFieldInfo,
  blockId: string
): Promise<
  Array<{
    recordId: string;
    locale: string | null;
    blockData: Record<string, unknown>;
    blockId: string;
    position: number;
  }>
> {
  // Build path for this field
  const paths = await buildNestedPathsToRootModels(client, [modularContentField], blockId);
  
  const allInstances: Array<{
    recordId: string;
    locale: string | null;
    blockData: Record<string, unknown>;
    blockId: string;
    position: number;
  }> = [];

  for (const path of paths) {
    const nestedInstances = await getAllBlockInstancesNested(client, path, blockId);
    
    for (const instance of nestedInstances) {
      allInstances.push({
        recordId: instance.rootRecordId,
        locale: instance.locale,
        blockData: instance.blockData,
        blockId: instance.blockId,
        position: instance.pathIndices[instance.pathIndices.length - 1] || 0,
      });
    }
  }

  return allInstances;
}
