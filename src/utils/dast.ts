/**
 * DAST (DatoCMS Abstract Syntax Tree) Utilities
 * 
 * This module provides functions for traversing and manipulating DAST documents,
 * which are used in DatoCMS Structured Text fields.
 */

import type {
  Node,
  Block,
  InlineBlock,
  WithChildrenNode,
} from 'datocms-structured-text-utils';
import type {
  StructuredTextValue,
  DastBlockNodeInfo,
  DastBlockRecord,
  BlockMigrationMapping,
} from '../types';

// =============================================================================
// Type Guards
// =============================================================================

/**
 * Check if a value is a structured text field value.
 * This checks for:
 * 1. schema === 'dast' (standard format)
 * 2. OR document.type === 'root' (for cases where schema might not be present)
 */
export function isStructuredTextValue(value: unknown): value is StructuredTextValue {
  if (!value || typeof value !== 'object') return false;
  const obj = value as Record<string, unknown>;
  
  // Check for standard format with schema
  if (obj.schema === 'dast' && obj.document !== undefined) {
    return true;
  }
  
  // Check for document with root type (fallback for when schema is not present)
  if (obj.document && typeof obj.document === 'object') {
    const doc = obj.document as Record<string, unknown>;
    if (doc.type === 'root' && Array.isArray(doc.children)) {
      return true;
    }
  }
  
  return false;
}

/**
 * Check if a node has children
 */
export function isNodeWithChildren(node: Node): node is WithChildrenNode {
  return 'children' in node && Array.isArray((node as WithChildrenNode).children);
}

/**
 * Check if a node is a block reference node
 */
export function isBlockNode(node: Node): node is Block {
  return node.type === 'block';
}

/**
 * Check if a node is an inline block reference node
 */
export function isInlineBlockNode(node: Node): node is InlineBlock {
  return node.type === 'inlineBlock';
}

/**
 * Get the block ID from a block/inlineBlock node.
 * Handles both formats:
 * - String ID: when blocks array is populated separately
 * - Object with 'id' property: when using nested: true (block data is inlined)
 */
export function getBlockNodeItemId(node: Block | InlineBlock): string | undefined {
  const item = node.item;
  if (typeof item === 'string') {
    return item;
  }
  if (item && typeof item === 'object') {
    return (item as Record<string, unknown>).id as string | undefined;
  }
  return undefined;
}

/**
 * Get the block type ID directly from an inlined block node (when using nested: true).
 * Returns undefined if the block is not inlined or if the type cannot be determined.
 */
export function getInlinedBlockTypeId(node: Block | InlineBlock): string | undefined {
  const item = node.item;
  if (item && typeof item === 'object') {
    const inlinedBlock = item as Record<string, unknown>;
    
    // Check for __itemTypeId (convenience property added by CMA)
    if (typeof inlinedBlock.__itemTypeId === 'string') {
      return inlinedBlock.__itemTypeId;
    }
    
    // Check for relationships.item_type.data.id
    const relationships = inlinedBlock.relationships as Record<string, unknown> | undefined;
    if (relationships?.item_type) {
      const itemType = relationships.item_type as Record<string, unknown>;
      const data = itemType.data as Record<string, unknown> | undefined;
      if (data?.id) {
        return data.id as string;
      }
    }
    
    // Check for item_type directly
    if (typeof inlinedBlock.item_type === 'string') {
      return inlinedBlock.item_type;
    }
    if (inlinedBlock.item_type && typeof inlinedBlock.item_type === 'object') {
      return (inlinedBlock.item_type as { id: string }).id;
    }
  }
  return undefined;
}

// =============================================================================
// Block Record Helpers
// =============================================================================

/**
 * Gets the block type ID from a block record in the blocks array
 */
export function getBlockRecordTypeId(block: DastBlockRecord): string | undefined {
  // Check for __itemTypeId first (convenience property)
  if (typeof block.__itemTypeId === 'string') {
    return block.__itemTypeId;
  }

  // Check for relationships.item_type.data.id (nested structure from CMA client)
  if (block.relationships?.item_type?.data?.id) {
    return block.relationships.item_type.data.id;
  }

  // Fallback: check for item_type directly (string or object with id)
  if (typeof block.item_type === 'string') {
    return block.item_type;
  }
  if (block.item_type && typeof block.item_type === 'object') {
    return (block.item_type as { id: string }).id;
  }

  return undefined;
}

/**
 * Finds a block record in the blocks array by its ID
 */
export function findBlockRecordById(
  blocks: DastBlockRecord[] | undefined,
  itemId: string
): DastBlockRecord | undefined {
  if (!blocks) return undefined;
  return blocks.find(block => block.id === itemId);
}

// =============================================================================
// DAST Traversal
// =============================================================================

/**
 * Traverses a DAST document tree and calls the callback for each node.
 * 
 * @param node - The current node to process
 * @param callback - Function called for each node. Return false to stop traversal of children.
 * @param path - Current path in the tree (for debugging/replacement)
 */
export function traverseDast(
  node: Node,
  callback: (node: Node, path: (string | number)[]) => boolean | void,
  path: (string | number)[] = []
): void {
  // Call callback for current node
  const shouldContinue = callback(node, path);
  if (shouldContinue === false) return;

  // Recursively traverse children
  if (isNodeWithChildren(node)) {
    node.children.forEach((child, index) => {
      traverseDast(child as Node, callback, [...path, 'children', index]);
    });
  }
}

/**
 * Finds all block and inlineBlock nodes in a DAST document.
 * Returns information about each node including its type and the block type ID.
 * Handles both formats:
 * - Standard format: node.item is a string ID, blocks array has block data
 * - Nested format (nested: true): node.item is the inlined block object
 * 
 * @param structuredTextValue - The complete structured text field value
 * @returns Array of block node information
 */
export function findBlockNodesInDast(
  structuredTextValue: StructuredTextValue
): DastBlockNodeInfo[] {
  const results: DastBlockNodeInfo[] = [];
  const blocks = structuredTextValue.blocks || [];

  traverseDast(structuredTextValue.document, (node, path) => {
    if (isBlockNode(node) || isInlineBlockNode(node)) {
      // Get block ID (handles both string ID and inlined object formats)
      const itemId = getBlockNodeItemId(node);
      
      // Try to get block type ID - first from inlined data, then from blocks array
      let blockTypeId = getInlinedBlockTypeId(node);
      
      if (!blockTypeId && typeof itemId === 'string') {
        // Fallback: look up in blocks array
        const blockRecord = findBlockRecordById(blocks, itemId);
        blockTypeId = blockRecord ? getBlockRecordTypeId(blockRecord) : undefined;
      }

      results.push({
        nodeType: node.type as 'block' | 'inlineBlock',
        itemId: itemId || (node.item as string), // Fallback to raw value if parsing fails
        blockTypeId,
        path,
      });
    }
  });

  return results;
}

/**
 * Finds all block nodes of a specific block type in a DAST document.
 * 
 * @param structuredTextValue - The complete structured text field value
 * @param targetBlockTypeId - The block type ID to filter by
 * @returns Array of block node information matching the target type
 */
export function findBlockNodesOfType(
  structuredTextValue: StructuredTextValue,
  targetBlockTypeId: string
): DastBlockNodeInfo[] {
  return findBlockNodesInDast(structuredTextValue).filter(
    info => info.blockTypeId === targetBlockTypeId
  );
}

// =============================================================================
// DAST Transformation
// =============================================================================

/**
 * Deep clones a DAST document to avoid mutating the original
 */
function cloneDast<T>(value: T): T {
  return JSON.parse(JSON.stringify(value));
}

/**
 * Transforms a DAST document by replacing block/inlineBlock nodes with inlineItem nodes.
 * This is used when converting blocks to linked records.
 * 
 * @param structuredTextValue - The complete structured text field value
 * @param targetBlockTypeId - The block type ID to transform
 * @param mapping - Mapping from old block IDs to new record IDs
 * @returns Transformed structured text value, or null if no changes were made
 */
export function transformDastBlocksToLinks(
  structuredTextValue: StructuredTextValue,
  targetBlockTypeId: string,
  mapping: BlockMigrationMapping
): StructuredTextValue | null {
  // Find all block nodes of the target type
  const blockNodes = findBlockNodesOfType(structuredTextValue, targetBlockTypeId);
  
  if (blockNodes.length === 0) {
    return null; // No changes needed
  }

  // Clone the value to avoid mutation
  const result = cloneDast(structuredTextValue);
  
  // Track which blocks to remove and which records to add to links
  const blocksToRemove = new Set<string>();
  const newLinks: Array<{ id: string }> = [];
  
  // Process each block node (in reverse order to not invalidate paths)
  // Actually, we'll process by rebuilding the tree with replacements
  
  // Replace nodes in the document tree
  result.document = replaceBlockNodesInTree(
    result.document,
    targetBlockTypeId,
    result.blocks || [],
    mapping,
    blocksToRemove,
    newLinks
  );

  // Remove converted blocks from the blocks array
  if (result.blocks) {
    result.blocks = result.blocks.filter(block => !blocksToRemove.has(block.id));
    if (result.blocks.length === 0) {
      delete result.blocks;
    }
  }

  // Add new records to the links array
  // IMPORTANT: Normalize the format to just { id: string } for consistency
  if (newLinks.length > 0) {
    if (!result.links) {
      result.links = [];
    }
    // Normalize existing links to just { id } format to avoid mixed formats
    // (when fetched with nested: true, links contain full record data)
    result.links = result.links.map(l => ({ id: l.id })) as typeof result.links;
    
    // Add only unique links (avoid duplicates)
    const existingLinkIds = new Set(result.links.map(l => l.id));
    for (const link of newLinks) {
      if (!existingLinkIds.has(link.id)) {
        result.links.push(link as typeof result.links[number]);
        existingLinkIds.add(link.id);
      }
    }
  }

  // Also normalize blocks array format if it exists
  if (result.blocks) {
    // Ensure blocks are in a format DatoCMS accepts when saving
    // (they may have extra nested data from fetch with nested: true)
    result.blocks = result.blocks.map(block => ({
      id: block.id,
      type: block.type,
      attributes: block.attributes,
      relationships: block.relationships,
    })) as typeof result.blocks;
  }

  return result;
}

/**
 * Recursively replaces block/inlineBlock nodes with inlineItem nodes in a tree.
 * Handles both formats:
 * - Standard format: node.item is a string ID, blocks array has block data
 * - Nested format (nested: true): node.item is the inlined block object
 * 
 * IMPORTANT: When replacing root-level 'block' nodes, the replacement must be
 * wrapped in a paragraph because 'inlineItem' cannot appear at the root level
 * in DAST (it can only appear as inline content within paragraphs).
 * 
 * @param isRootLevel - Whether this node is a direct child of the document root
 */
function replaceBlockNodesInTree<T extends Node>(
  node: T,
  targetBlockTypeId: string,
  blocks: DastBlockRecord[],
  mapping: BlockMigrationMapping,
  blocksToRemove: Set<string>,
  newLinks: Array<{ id: string }>,
  isRootLevel: boolean = false
): T {
  // Check if this is a block or inlineBlock node to replace
  if (isBlockNode(node) || isInlineBlockNode(node)) {
    // Get block ID (handles both string ID and inlined object formats)
    const itemId = getBlockNodeItemId(node);
    
    // Try to get block type ID - first from inlined data, then from blocks array
    let blockTypeId = getInlinedBlockTypeId(node);
    
    if (!blockTypeId && typeof itemId === 'string') {
      // Fallback: look up in blocks array
      const blockRecord = findBlockRecordById(blocks, itemId);
      blockTypeId = blockRecord ? getBlockRecordTypeId(blockRecord) : undefined;
    }

    if (blockTypeId === targetBlockTypeId && itemId && mapping[itemId]) {
      // This block should be converted to an inlineItem
      const newRecordId = mapping[itemId];
      
      // Mark block for removal (if it exists in blocks array)
      blocksToRemove.add(itemId);
      
      // Add to new links
      newLinks.push({ id: newRecordId });
      
      // If this is a root-level 'block' node, wrap the inlineItem in a paragraph
      // because inlineItem cannot appear at the document root level in DAST
      // IMPORTANT: Include an empty span before the inlineItem - DatoCMS may not
      // render paragraphs that contain ONLY an inlineItem with no text content.
      if (isRootLevel && node.type === 'block') {
        const replacement = {
          type: 'paragraph',
          children: [
            {
              type: 'span',
              value: '',
            },
            {
              type: 'inlineItem',
              item: newRecordId,
            }
          ],
        };
        return replacement as T;
      }
      
      // For inlineBlock (which is already inline) or non-root-level, just return inlineItem
      const replacement = {
        type: 'inlineItem',
        item: newRecordId,
      };
      return replacement as T;
    }
  }

  // If node has children, recursively process them
  if (isNodeWithChildren(node)) {
    const clonedNode = { ...node } as WithChildrenNode;
    
    // Check if this is the root node - its children are at root level
    const childrenAreRootLevel = (node as Node).type === 'root';
    
    clonedNode.children = clonedNode.children.map(child =>
      replaceBlockNodesInTree(
        child as Node,
        targetBlockTypeId,
        blocks,
        mapping,
        blocksToRemove,
        newLinks,
        childrenAreRootLevel
      )
    ) as typeof clonedNode.children;
    return clonedNode as T;
  }

  // Return node unchanged
  return node;
}

/**
 * Extracts link IDs from a structured text field value based on the target block type and mapping.
 * This is used when migrating data from structured text fields.
 * 
 * @param structuredTextValue - The complete structured text field value
 * @param targetBlockTypeId - The block type ID to extract links for
 * @param mapping - Mapping from old block IDs to new record IDs
 * @returns Array of new record IDs
 */
export function extractLinksFromStructuredText(
  structuredTextValue: StructuredTextValue,
  targetBlockTypeId: string,
  mapping: BlockMigrationMapping
): string[] {
  const blockNodes = findBlockNodesOfType(structuredTextValue, targetBlockTypeId);
  const linkIds: string[] = [];

  for (const nodeInfo of blockNodes) {
    const newRecordId = mapping[nodeInfo.itemId];
    if (newRecordId) {
      linkIds.push(newRecordId);
    }
  }

  return linkIds;
}

// =============================================================================
// Partial Mode: Add InlineItems Alongside Blocks
// =============================================================================

/**
 * Adds inlineItem nodes alongside existing block/inlineBlock nodes in a DAST document.
 * This is used for partial replacement mode where we keep the original blocks
 * but also add references to the converted records.
 * 
 * @param structuredTextValue - The complete structured text field value
 * @param targetBlockTypeId - The block type ID to add links for
 * @param mapping - Mapping from old block IDs to new record IDs
 * @returns Transformed structured text value with blocks AND new inlineItems, or null if no changes
 */
export function addInlineItemsAlongsideBlocks(
  structuredTextValue: StructuredTextValue,
  targetBlockTypeId: string,
  mapping: BlockMigrationMapping
): StructuredTextValue | null {
  // Find all block nodes of the target type
  const blockNodes = findBlockNodesOfType(structuredTextValue, targetBlockTypeId);
  
  if (blockNodes.length === 0) {
    return null; // No changes needed
  }

  // Clone the value to avoid mutation
  const result = cloneDast(structuredTextValue);
  
  // Track which records to add to links
  const newLinks: Array<{ id: string }> = [];
  
  // Add inlineItem nodes alongside block nodes in the document tree
  result.document = addInlineItemsAlongsideBlocksInTree(
    result.document,
    targetBlockTypeId,
    result.blocks || [],
    mapping,
    newLinks
  );

  // ALWAYS normalize links array (even if not adding new ones)
  // This is critical because nested: true returns expanded record data
  // and DatoCMS expects just { id: string } format when saving
  if (result.links) {
    result.links = result.links.map(l => ({ id: l.id })) as typeof result.links;
  }

  // Add new records to the links array
  if (newLinks.length > 0) {
    if (!result.links) {
      result.links = [];
    }
    // Add only unique links (avoid duplicates)
    const existingLinkIds = new Set(result.links.map(l => l.id));
    for (const link of newLinks) {
      if (!existingLinkIds.has(link.id)) {
        result.links.push(link as typeof result.links[number]);
        existingLinkIds.add(link.id);
      }
    }
  }

  // Normalize blocks array format if it exists (keep all blocks)
  if (result.blocks) {
    result.blocks = result.blocks.map(block => ({
      id: block.id,
      type: block.type,
      attributes: block.attributes,
      relationships: block.relationships,
    })) as typeof result.blocks;
  }

  return result;
}

/**
 * Normalizes the `item` property of block, inlineBlock, and inlineItem nodes.
 * When fetching with nested: true, the item property can be expanded to a full object
 * instead of a string ID. DatoCMS expects string IDs when saving.
 */
function normalizeNodeItemProperty<T extends Node>(node: T): T {
  // Check if this is a node type that has an item property
  if (node.type === 'block' || node.type === 'inlineBlock' || node.type === 'inlineItem') {
    const nodeWithItem = node as T & { item: unknown };
    const item = nodeWithItem.item;
    
    // If item is already a string, no normalization needed
    if (typeof item === 'string') {
      return node;
    }
    
    // If item is an object with an id property, extract the string ID
    if (item && typeof item === 'object') {
      const itemObj = item as Record<string, unknown>;
      const stringId = itemObj.id as string | undefined;
      
      if (stringId) {
        // Create a new node with the normalized string ID
        return {
          ...node,
          item: stringId,
        };
      }
    }
  }
  
  return node;
}

/**
 * Recursively traverses the DAST tree and adds inlineItem nodes after each
 * block/inlineBlock node of the target type.
 * 
 * For root-level 'block' nodes: inserts a new paragraph with inlineItem after the block
 * For 'inlineBlock' nodes: inserts an inlineItem directly after the inlineBlock
 */
function addInlineItemsAlongsideBlocksInTree<T extends Node>(
  node: T,
  targetBlockTypeId: string,
  blocks: DastBlockRecord[],
  mapping: BlockMigrationMapping,
  newLinks: Array<{ id: string }>
): T {
  // If node has children, process them and potentially insert new nodes
  if (isNodeWithChildren(node)) {
    const clonedNode = { ...node } as WithChildrenNode;
    const childrenAreRootLevel = (node as Node).type === 'root';
    
    // Process children and build new array with inserted inlineItem nodes
    const newChildren: Node[] = [];
    
    for (const child of clonedNode.children) {
      const childNode = child as Node;
      
      // First, recursively process this child
      const processedChild = addInlineItemsAlongsideBlocksInTree(
        childNode,
        targetBlockTypeId,
        blocks,
        mapping,
        newLinks
      );
      
      // Normalize the item property for block/inlineBlock/inlineItem nodes
      // When fetching with nested: true, item can be an expanded object instead of string ID
      // DatoCMS expects string IDs when saving, so we must normalize
      const normalizedChild = normalizeNodeItemProperty(processedChild);
      
      // Add the normalized child
      newChildren.push(normalizedChild);
      
      // Check if this child is a block/inlineBlock of the target type
      // If so, add an inlineItem node after it
      if (isBlockNode(childNode) || isInlineBlockNode(childNode)) {
        const itemId = getBlockNodeItemId(childNode);
        let blockTypeId = getInlinedBlockTypeId(childNode);
        
        if (!blockTypeId && typeof itemId === 'string') {
          const blockRecord = findBlockRecordById(blocks, itemId);
          blockTypeId = blockRecord ? getBlockRecordTypeId(blockRecord) : undefined;
        }
        
        if (blockTypeId === targetBlockTypeId && itemId && mapping[itemId]) {
          const newRecordId = mapping[itemId];
          
          // Add to links array
          newLinks.push({ id: newRecordId });
          
          // Create and insert the inlineItem node
          if (childrenAreRootLevel && childNode.type === 'block') {
            // For root-level blocks, wrap inlineItem in a paragraph
            const paragraphWithInlineItem = {
              type: 'paragraph',
              children: [
                {
                  type: 'span',
                  value: '',
                },
                {
                  type: 'inlineItem',
                  item: newRecordId,
                }
              ],
            };
            newChildren.push(paragraphWithInlineItem as Node);
          } else {
            // For inline context, just add the inlineItem
            const inlineItemNode = {
              type: 'inlineItem',
              item: newRecordId,
            };
            newChildren.push(inlineItemNode as Node);
          }
        }
      }
    }
    
    clonedNode.children = newChildren as typeof clonedNode.children;
    return clonedNode as T;
  }

  // Return node unchanged if it has no children
  return node;
}

