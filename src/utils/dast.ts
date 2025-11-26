/**
 * DAST (DatoCMS Abstract Syntax Tree) Utilities
 * 
 * This module provides functions for traversing and manipulating DAST documents,
 * which are used in DatoCMS Structured Text fields.
 */

import type {
  StructuredTextValue,
  DastNode,
  DastRootNode,
  DastBlockNode,
  DastInlineBlockNode,
  DastInlineItemNode,
  DastNodeWithChildren,
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
export function isNodeWithChildren(node: DastNode): node is DastNodeWithChildren {
  return 'children' in node && Array.isArray((node as DastNodeWithChildren).children);
}

/**
 * Check if a node is a block reference node
 */
export function isBlockNode(node: DastNode): node is DastBlockNode {
  return node.type === 'block';
}

/**
 * Check if a node is an inline block reference node
 */
export function isInlineBlockNode(node: DastNode): node is DastInlineBlockNode {
  return node.type === 'inlineBlock';
}

/**
 * Get the block ID from a block/inlineBlock node.
 * Handles both formats:
 * - String ID: when blocks array is populated separately
 * - Object with 'id' property: when using nested: true (block data is inlined)
 */
export function getBlockNodeItemId(node: DastBlockNode | DastInlineBlockNode): string | undefined {
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
export function getInlinedBlockTypeId(node: DastBlockNode | DastInlineBlockNode): string | undefined {
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

/**
 * Check if a node is an inline item (record link) node
 */
export function isInlineItemNode(node: DastNode): node is DastInlineItemNode {
  return node.type === 'inlineItem';
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
  node: DastNode,
  callback: (node: DastNode, path: (string | number)[]) => boolean | void,
  path: (string | number)[] = []
): void {
  // Call callback for current node
  const shouldContinue = callback(node, path);
  if (shouldContinue === false) return;

  // Recursively traverse children
  if (isNodeWithChildren(node)) {
    node.children.forEach((child, index) => {
      traverseDast(child as DastNode, callback, [...path, 'children', index]);
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

/**
 * Checks if a structured text value contains any blocks of the specified type.
 * 
 * @param structuredTextValue - The complete structured text field value
 * @param targetBlockTypeId - The block type ID to check for
 * @returns True if the document contains at least one block of the specified type
 */
export function containsBlockOfType(
  structuredTextValue: StructuredTextValue,
  targetBlockTypeId: string
): boolean {
  const blocks = structuredTextValue.blocks || [];
  
  // First, check if any blocks in the blocks array match the type
  const hasMatchingBlock = blocks.some(block => getBlockRecordTypeId(block) === targetBlockTypeId);
  if (!hasMatchingBlock) return false;

  // Then verify the block is actually referenced in the document
  const blockNodes = findBlockNodesOfType(structuredTextValue, targetBlockTypeId);
  return blockNodes.length > 0;
}

/**
 * Gets all block records of a specific type from a structured text value.
 * Only returns blocks that are actually referenced in the document tree.
 * 
 * @param structuredTextValue - The complete structured text field value
 * @param targetBlockTypeId - The block type ID to filter by
 * @returns Array of block records matching the target type
 */
export function getBlockRecordsOfType(
  structuredTextValue: StructuredTextValue,
  targetBlockTypeId: string
): DastBlockRecord[] {
  const blocks = structuredTextValue.blocks || [];
  const blockNodes = findBlockNodesOfType(structuredTextValue, targetBlockTypeId);
  
  // Get unique block IDs that are referenced and match the type
  const referencedBlockIds = new Set(blockNodes.map(node => node.itemId));
  
  return blocks.filter(
    block => referencedBlockIds.has(block.id) && getBlockRecordTypeId(block) === targetBlockTypeId
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
 * @param debugMode - If true, adds links AFTER blocks instead of replacing them
 * @returns Transformed structured text value, or null if no changes were made
 */
export function transformDastBlocksToLinks(
  structuredTextValue: StructuredTextValue,
  targetBlockTypeId: string,
  mapping: BlockMigrationMapping,
  debugMode: boolean = false
): StructuredTextValue | null {
  // Find all block nodes of the target type
  const blockNodes = findBlockNodesOfType(structuredTextValue, targetBlockTypeId);
  
  if (blockNodes.length === 0) {
    return null; // No changes needed
  }
  
  // In debug mode, use the "add after" approach instead of replacing
  if (debugMode) {
    return addLinksAfterBlocks(structuredTextValue, targetBlockTypeId, mapping);
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
 * In debug mode, adds inlineItem nodes AFTER each matching block node instead of replacing.
 * This allows seeing both the original block and the new link.
 * 
 * IMPORTANT: This function recursively processes the entire DAST tree, just like
 * replaceBlockNodesInTree does in non-debug mode, to ensure consistent behavior.
 */
function addLinksAfterBlocks(
  structuredTextValue: StructuredTextValue,
  targetBlockTypeId: string,
  mapping: BlockMigrationMapping
): StructuredTextValue | null {
  // Clone the value to avoid mutation
  const result = cloneDast(structuredTextValue);
  const blocks = result.blocks || [];
  const newLinks: Array<{ id: string }> = [];
  let hasChanges = false;
  
  // Process the document's children at root level
  const rootDoc = result.document as DastRootNode;
  if (!rootDoc.children || !Array.isArray(rootDoc.children)) {
    return null;
  }
  
  /**
   * Recursively process nodes and add debug links after matching blocks.
   * For root-level block nodes, adds a new paragraph after them.
   * Returns the processed children array (may have additional elements).
   */
  function processChildren(
    children: DastNode[],
    isRootLevel: boolean
  ): DastNode[] {
    const newChildren: DastNode[] = [];
    
    for (const child of children) {
      // First, recursively process this node's children if it has any
      let processedChild = child;
      if (isNodeWithChildren(child)) {
        const childNode = { ...child } as DastNodeWithChildren;
        const childIsRoot = child.type === 'root';
        childNode.children = processChildren(
          childNode.children as DastNode[],
          childIsRoot
        ) as typeof childNode.children;
        processedChild = childNode as DastNode;
      }
      
      // Always keep the (processed) child
      newChildren.push(processedChild);
      
      // Check if this is a block node that matches our target
      if (isBlockNode(processedChild) || isInlineBlockNode(processedChild)) {
        const blockNode = processedChild as DastBlockNode | DastInlineBlockNode;
        const itemId = getBlockNodeItemId(blockNode);
        
        // Get block type ID
        let blockTypeId = getInlinedBlockTypeId(blockNode);
        if (!blockTypeId && typeof itemId === 'string') {
          const blockRecord = findBlockRecordById(blocks, itemId);
          blockTypeId = blockRecord ? getBlockRecordTypeId(blockRecord) : undefined;
        }
        
        // If this block matches our target and we have a mapping for it
        if (blockTypeId === targetBlockTypeId && itemId && mapping[itemId]) {
          const newRecordId = mapping[itemId];
          
          if (isRootLevel) {
            // For root-level blocks, add a paragraph after the block
            // (inlineItem can't be at root level, must be inside a paragraph)
            newChildren.push({
              type: 'paragraph',
              children: [
                {
                  type: 'span',
                  value: '🔗 [DEBUG: Link to converted record] ',
                } as DastNode,
                {
                  type: 'inlineItem',
                  item: newRecordId,
                } as DastNode,
              ],
            } as DastNode);
          } else {
            // For non-root-level (inline) blocks, add an inline link after
            // This handles inlineBlock nodes that appear within paragraphs
            newChildren.push({
              type: 'span',
              value: ' 🔗',
            } as DastNode);
            newChildren.push({
              type: 'inlineItem',
              item: newRecordId,
            } as DastNode);
          }
          
          // Track the new link
          newLinks.push({ id: newRecordId });
          hasChanges = true;
        }
      }
    }
    
    return newChildren;
  }
  
  // Process the root document's children
  const processedChildren = processChildren(rootDoc.children as DastNode[], true);
  
  if (!hasChanges) {
    return null;
  }
  
  // Update the document with new children
  (result.document as DastRootNode).children = processedChildren as DastRootNode['children'];
  
  // Add new records to the links array
  // IMPORTANT: Normalize the format to just { id: string } for consistency
  // (same as non-debug mode to ensure consistent behavior)
  if (newLinks.length > 0) {
    if (!result.links) {
      result.links = [];
    }
    // Normalize existing links to just { id } format to avoid mixed formats
    result.links = result.links.map(l => ({ id: l.id })) as typeof result.links;
    
    const existingLinkIds = new Set(result.links.map(l => l.id));
    for (const link of newLinks) {
      if (!existingLinkIds.has(link.id)) {
        result.links.push(link as typeof result.links[number]);
        existingLinkIds.add(link.id);
      }
    }
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
function replaceBlockNodesInTree<T extends DastNode>(
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
    const clonedNode = { ...node } as DastNodeWithChildren;
    
    // Check if this is the root node - its children are at root level
    const childrenAreRootLevel = (node as DastNode).type === 'root';
    
    clonedNode.children = clonedNode.children.map(child =>
      replaceBlockNodesInTree(
        child as DastNode,
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

