import type { Client } from '@datocms/cma-client-browser';

export interface BlockAnalysis {
  block: {
    id: string;
    name: string;
    apiKey: string;
  };
  fields: FieldInfo[];
  modularContentFields: ModularContentFieldInfo[];
  totalAffectedRecords: number;
}

export interface FieldInfo {
  id: string;
  label: string;
  apiKey: string;
  fieldType: string;
  localized: boolean;
  validators: Record<string, unknown>;
  appearance: {
    editor: string;
    parameters: Record<string, unknown>;
    addons: Array<{
      id: string;
      parameters: Record<string, unknown>;
    }>;
  };
  position: number;
  hint?: string;
  defaultValue?: unknown;
}

export interface ModularContentFieldInfo {
  id: string;
  label: string;
  apiKey: string;
  parentModelId: string;
  parentModelName: string;
  parentModelApiKey: string;
  parentIsBlock: boolean; // NEW: indicates if parent is a block
  localized: boolean;
  allowedBlockIds: string[];
  position?: number;
  hint?: string;
  fieldType: 'rich_text' | 'structured_text' | 'single_block'; // Field type for proper handling
}

/**
 * Represents a path from a root model to a nested block's field.
 * Each step in the path represents navigating into a modular content field.
 */
export interface NestedBlockPath {
  // The root model (not a block) that contains everything
  rootModelId: string;
  rootModelName: string;
  rootModelApiKey: string;
  
  // The path of field API keys to navigate from root to the target field
  // Each entry is: { fieldApiKey, expectedBlockTypeId, localized, fieldType }
  // The last entry is the field that directly contains the target block
  path: Array<{
    fieldApiKey: string;
    expectedBlockTypeId: string; // The block type we expect to find at this level
    localized: boolean;
    fieldType: 'rich_text' | 'structured_text' | 'single_block';
  }>;
  
  // The original modular content field info
  fieldInfo: ModularContentFieldInfo;
  
  /** True if ANY step in the path is localized (meaning block data varies by locale) */
  isInLocalizedContext: boolean;
}

/**
 * Represents a group of block instances from different locales at the same position.
 * Used for merging locale-specific block data into a single record with localized fields.
 */
export interface GroupedBlockInstance {
  /** Unique key: rootRecordId + pathIndices */
  groupKey: string;
  /** The root record containing these blocks */
  rootRecordId: string;
  /** Position indices in the nested structure */
  pathIndices: number[];
  /** Map of locale -> block data. For non-localized contexts: { __default__: data } */
  localeData: Record<string, Record<string, unknown>>;
  /** All block IDs from all locales (for mapping) */
  allBlockIds: string[];
  /** Reference block ID (from first locale found) for primary mapping */
  referenceBlockId: string;
}

export interface ConversionProgress {
  currentStep: number;
  totalSteps: number;
  stepDescription: string;
  percentage: number;
  details?: string;
}

export interface ConversionResult {
  success: boolean;
  newModelId?: string;
  newModelApiKey?: string;
  migratedRecordsCount: number;
  convertedFieldsCount: number;
  error?: string;
  /** Original block model name (for renaming after deletion) */
  originalBlockName?: string;
  /** Original block model api_key (for renaming after deletion) */
  originalBlockApiKey?: string;
  /** Cleanup context for when user clicks "Delete Original Block" */
  cleanupContext?: CleanupContext;
}

/**
 * Context needed to clean up original block data when user clicks "Delete Original Block"
 */
export interface CleanupContext {
  /** The block ID being converted */
  blockId: string;
  /** Mapping from old block instance IDs to new record IDs */
  mapping: BlockMigrationMapping;
  /** Fields that were converted and need cleanup */
  convertedFields: ConvertedFieldInfo[];
  /** Nested paths for cleaning up nested block references */
  nestedPaths: NestedBlockPath[];
  /** Available locales in the project */
  availableLocales: string[];
}

/**
 * Info about a converted field for cleanup purposes
 */
export interface ConvertedFieldInfo {
  id: string;
  apiKey: string;
  parentModelId: string;
  parentModelApiKey: string;
  parentIsBlock: boolean;
  localized: boolean;
  fieldType: 'rich_text' | 'structured_text' | 'single_block';
  /** IDs of block types that remain in this field after removing the converted block */
  remainingBlockIds: string[];
  /** API key of the new links field that was created */
  newLinksFieldApiKey: string;
}

export interface BlockMigrationMapping {
  // Maps original block instance ID to new record ID
  [blockInstanceId: string]: string;
}

export interface RecordBlockData {
  recordId: string;
  fieldApiKey: string;
  blocks: Array<{
    id: string;
    itemType: string;
    data: Record<string, unknown>;
  }>;
}

export type ProgressCallback = (progress: ConversionProgress) => void;

export type CMAClient = Client;

// =============================================================================
// DAST (DatoCMS Abstract Syntax Tree) Types
// =============================================================================

/**
 * The complete structured text field value as stored in DatoCMS.
 * When using `nested: true`, blocks and links are expanded inline.
 */
export interface StructuredTextValue {
  /** The schema identifier, always "dast" for DatoCMS */
  schema: 'dast';
  /** The DAST document tree */
  document: DastRootNode;
  /** Array of block instances embedded in this structured text */
  blocks?: DastBlockRecord[];
  /** Array of linked records referenced in this structured text */
  links?: DastLinkRecord[];
}

/**
 * A block record embedded in structured text (from the blocks array)
 */
export interface DastBlockRecord {
  id: string;
  /** Block type relationships */
  relationships?: {
    item_type?: {
      data?: {
        type: 'item_type';
        id: string;
      };
    };
  };
  /** Block attributes/field values */
  attributes?: Record<string, unknown>;
  /** Convenience property for item type ID */
  __itemTypeId?: string;
  /** Direct item_type reference (alternative format) */
  item_type?: string | { id: string };
  [key: string]: unknown;
}

/**
 * A linked record referenced in structured text (from the links array)
 */
export interface DastLinkRecord {
  id: string;
  [key: string]: unknown;
}

// -----------------------------------------------------------------------------
// DAST Node Types
// -----------------------------------------------------------------------------

/** Base type for all DAST nodes */
export interface DastNodeBase {
  type: string;
}

/** Root node - the top-level container of the document */
export interface DastRootNode extends DastNodeBase {
  type: 'root';
  children: DastRootChildNode[];
}

/** Nodes that can be direct children of root */
export type DastRootChildNode =
  | DastParagraphNode
  | DastHeadingNode
  | DastListNode
  | DastCodeNode
  | DastBlockquoteNode
  | DastBlockNode
  | DastThematicBreakNode;

/** Paragraph node */
export interface DastParagraphNode extends DastNodeBase {
  type: 'paragraph';
  children: DastInlineNode[];
}

/** Heading node */
export interface DastHeadingNode extends DastNodeBase {
  type: 'heading';
  level: 1 | 2 | 3 | 4 | 5 | 6;
  children: DastInlineNode[];
}

/** List node */
export interface DastListNode extends DastNodeBase {
  type: 'list';
  style: 'bulleted' | 'numbered';
  children: DastListItemNode[];
}

/** List item node */
export interface DastListItemNode extends DastNodeBase {
  type: 'listItem';
  children: (DastParagraphNode | DastListNode)[];
}

/** Code block node */
export interface DastCodeNode extends DastNodeBase {
  type: 'code';
  language?: string;
  highlight?: number[];
  code: string;
}

/** Blockquote node */
export interface DastBlockquoteNode extends DastNodeBase {
  type: 'blockquote';
  attribution?: string;
  children: DastParagraphNode[];
}

/** Block node - references an embedded block record (root-level only) */
export interface DastBlockNode extends DastNodeBase {
  type: 'block';
  /** ID of the block record in the blocks array */
  item: string;
}

/** Thematic break node */
export interface DastThematicBreakNode extends DastNodeBase {
  type: 'thematicBreak';
}

/** Nodes that can appear inline within text */
export type DastInlineNode =
  | DastSpanNode
  | DastLinkNode
  | DastItemLinkNode
  | DastInlineItemNode
  | DastInlineBlockNode;

/** Span node - text content */
export interface DastSpanNode extends DastNodeBase {
  type: 'span';
  value: string;
  marks?: DastMark[];
}

/** Text decoration marks */
export type DastMark = 
  | 'strong'
  | 'emphasis'
  | 'underline'
  | 'strikethrough'
  | 'code'
  | 'highlight';

/** Link node - hyperlink to external URL */
export interface DastLinkNode extends DastNodeBase {
  type: 'link';
  url: string;
  meta?: Array<{ id: string; value: string }>;
  children: DastSpanNode[];
}

/** Item link node - hyperlink to a DatoCMS record */
export interface DastItemLinkNode extends DastNodeBase {
  type: 'itemLink';
  /** ID of the linked record in the links array */
  item: string;
  meta?: Array<{ id: string; value: string }>;
  children: DastSpanNode[];
}

/** Inline item node - reference to a DatoCMS record without link text */
export interface DastInlineItemNode extends DastNodeBase {
  type: 'inlineItem';
  /** ID of the linked record in the links array */
  item: string;
}

/** Inline block node - embedded block within text flow */
export interface DastInlineBlockNode extends DastNodeBase {
  type: 'inlineBlock';
  /** ID of the block record in the blocks array */
  item: string;
}

/** Union of all DAST node types */
export type DastNode =
  | DastRootNode
  | DastParagraphNode
  | DastHeadingNode
  | DastListNode
  | DastListItemNode
  | DastCodeNode
  | DastBlockquoteNode
  | DastBlockNode
  | DastThematicBreakNode
  | DastSpanNode
  | DastLinkNode
  | DastItemLinkNode
  | DastInlineItemNode
  | DastInlineBlockNode;

/** Helper type for nodes that can have children */
export type DastNodeWithChildren = 
  | DastRootNode
  | DastParagraphNode
  | DastHeadingNode
  | DastListNode
  | DastListItemNode
  | DastBlockquoteNode
  | DastLinkNode
  | DastItemLinkNode;

/**
 * Result of finding block nodes in a DAST document
 */
export interface DastBlockNodeInfo {
  /** The node type (block or inlineBlock) */
  nodeType: 'block' | 'inlineBlock';
  /** The item ID referencing the blocks array */
  itemId: string;
  /** The block type ID (from blocks array lookup) */
  blockTypeId: string | undefined;
  /** Path to this node in the tree for later replacement */
  path: (string | number)[];
}
