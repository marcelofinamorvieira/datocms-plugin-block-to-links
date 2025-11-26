import { useState, useEffect, useMemo, useCallback } from 'react';
import type { RenderConfigScreenCtx } from 'datocms-plugin-sdk';
import {
  Canvas,
  Button,
  Spinner,
  SwitchField,
  TextField,
} from 'datocms-react-ui';
import { createClient } from '../utils/client';
import { analyzeBlock } from '../utils/analyzer';
import { convertBlockToModel, deleteOriginalBlock, renameModelToOriginal } from '../utils/converter';
import type { BlockAnalysis, ConversionProgress, DebugOptions } from '../types';
import s from './styles.module.css';

type Props = {
  ctx: RenderConfigScreenCtx;
};

type BlockModel = {
  id: string;
  name: string;
  api_key: string;
};

type ConversionState =
  | { status: 'idle' }
  | { status: 'analyzing' }
  | { status: 'analyzed'; analysis: BlockAnalysis }
  | { status: 'converting'; progress: ConversionProgress }
  | { status: 'success'; result: { 
      newModelId: string;
      newModelApiKey: string; 
      migratedRecords: number; 
      convertedFields: number;
      originalBlockName?: string;
      originalBlockApiKey?: string;
    } }
  | { status: 'error'; message: string };

// Generate a default debug suffix using letters only (DatoCMS api_keys don't allow numbers)
function generateDebugSuffix(): string {
  // Use a short random string of lowercase letters
  const chars = 'abcdefghijklmnopqrstuvwxyz';
  let randomPart = '';
  for (let i = 0; i < 6; i++) {
    randomPart += chars.charAt(Math.floor(Math.random() * chars.length));
  }
  return `_dev_${randomPart}`;
}

export default function ConfigScreen({ ctx }: Props) {
  const [selectedBlockId, setSelectedBlockId] = useState<string>('');
  const [conversionState, setConversionState] = useState<ConversionState>({ status: 'idle' });
  const [blockModels, setBlockModels] = useState<BlockModel[]>([]);
  const [loadingBlocks, setLoadingBlocks] = useState(true);
  
  // Debug mode state
  const [debugModeEnabled, setDebugModeEnabled] = useState(false);
  const [debugSuffix, setDebugSuffix] = useState(generateDebugSuffix);
  
  // Handler for regenerating the debug suffix
  const handleRegenerateSuffix = useCallback(() => {
    setDebugSuffix(generateDebugSuffix());
  }, []);
  
  // Build the debug options object
  const debugOptions: DebugOptions = useMemo(() => ({
    enabled: debugModeEnabled,
    suffix: debugModeEnabled ? debugSuffix : '',
    skipDeletions: debugModeEnabled, // Always skip deletions in debug mode
    verboseLogging: debugModeEnabled, // Always log in debug mode
  }), [debugModeEnabled, debugSuffix]);

  // Create CMA client
  const client = useMemo(() => {
    if (!ctx.currentUserAccessToken) return null;
    return createClient(ctx.currentUserAccessToken);
  }, [ctx.currentUserAccessToken]);

  // Fetch all block models using CMA client
  useEffect(() => {
    async function fetchBlockModels() {
      if (!client) {
        setLoadingBlocks(false);
        return;
      }

      try {
        setLoadingBlocks(true);
        // Fetch all item types and filter for modular blocks
        const allItemTypes = await client.itemTypes.list();
        const modularBlocks = allItemTypes.filter((itemType) => itemType.modular_block);

        const blocks: BlockModel[] = modularBlocks
          .map((itemType) => ({
            id: itemType.id,
            name: itemType.name,
            api_key: itemType.api_key,
          }))
          .sort((a, b) => a.name.localeCompare(b.name));

        setBlockModels(blocks);
      } catch (error) {
        console.error('Failed to fetch block models:', error);
        await ctx.alert(`Failed to fetch block models: ${error instanceof Error ? error.message : String(error)}`);
      } finally {
        setLoadingBlocks(false);
      }
    }

    fetchBlockModels();
  }, [client, ctx]);

  // Handle block selection
  const handleBlockSelect = useCallback(
    async (blockId: string) => {
      setSelectedBlockId(blockId);
      setConversionState({ status: 'idle' });

      if (!blockId || !client) return;

      setConversionState({ status: 'analyzing' });

      try {
        const analysis = await analyzeBlock(client, blockId);
        setConversionState({ status: 'analyzed', analysis });
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        setConversionState({ status: 'error', message });
      }
    },
    [client]
  );

  // Handle conversion
  const handleConvert = useCallback(async () => {
    if (!client || !selectedBlockId) return;

    // In debug mode, show a different confirmation dialog
    const confirmTitle = debugOptions.enabled 
      ? 'Convert Block to Model (DEBUG MODE)?' 
      : 'Convert Block to Model?';
    
    const confirmContent = debugOptions.enabled
      ? `DEBUG MODE is enabled. This will:\n\n• Create a new model with suffix "${debugSuffix}"\n• Create records from block instances\n• Create new link fields (original fields preserved)\n• NOT delete any existing fields or blocks\n\nYou can run this multiple times safely.`
      : 'This will create a new model with records from the block instances, and convert modular content fields to link fields. This operation cannot be easily undone. Are you sure you want to proceed?';

    const confirmed = await ctx.openConfirm({
      title: confirmTitle,
      content: confirmContent,
      choices: [
        {
          label: debugOptions.enabled ? 'Convert (Debug)' : 'Convert',
          value: 'convert',
          intent: debugOptions.enabled ? 'positive' : 'negative',
        },
      ],
      cancel: {
        label: 'Cancel',
        value: false,
      },
    });

    if (confirmed !== 'convert') return;

    // Log debug mode status to console
    if (debugOptions.enabled) {
      console.log('======================================');
      console.log('[DEBUG MODE] Starting conversion...');
      console.log('[DEBUG MODE] Suffix:', debugSuffix);
      console.log('[DEBUG MODE] Skip deletions:', debugOptions.skipDeletions);
      console.log('[DEBUG MODE] Verbose logging:', debugOptions.verboseLogging);
      console.log('======================================');
    }

    setConversionState({
      status: 'converting',
      progress: {
        currentStep: 0,
        totalSteps: 6,
        stepDescription: debugOptions.enabled ? 'Starting conversion (DEBUG MODE)...' : 'Starting conversion...',
        percentage: 0,
      },
    });

    try {
      const result = await convertBlockToModel(client, selectedBlockId, (progress) => {
        setConversionState({ status: 'converting', progress });
      }, debugOptions);

      if (result.success) {
        setConversionState({
          status: 'success',
          result: {
            newModelId: result.newModelId || '',
            newModelApiKey: result.newModelApiKey || '',
            migratedRecords: result.migratedRecordsCount,
            convertedFields: result.convertedFieldsCount,
            originalBlockName: result.originalBlockName,
            originalBlockApiKey: result.originalBlockApiKey,
          },
        });

        const successMessage = debugOptions.enabled
          ? `[DEBUG] Successfully converted block to model "${result.newModelApiKey}"! Original fields/blocks preserved.`
          : `Successfully converted block to model "${result.newModelApiKey}"!`;
        
        await ctx.notice(successMessage);

        // In debug mode, skip the deletion dialog entirely
        if (!debugOptions.enabled) {
          // Ask if user wants to delete the original block
          const deleteOriginal = await ctx.openConfirm({
            title: 'Delete Original Block?',
            content:
              'The conversion was successful. Do you want to delete the original block model? This is optional - you can keep it for reference or delete it later.',
            choices: [
              {
                label: 'Delete Block',
                value: 'delete',
                intent: 'negative',
              },
              {
                label: 'Keep Block',
                value: 'keep',
                intent: 'positive',
              },
            ],
            cancel: {
              label: 'Decide Later',
              value: false,
            },
          });

          if (deleteOriginal === 'delete') {
            try {
              await deleteOriginalBlock(client, selectedBlockId);
              
              // After deleting the original block, rename the new model to have the original name/api_key
              if (result.newModelId && result.originalBlockName && result.originalBlockApiKey) {
                const renameResult = await renameModelToOriginal(
                  client,
                  result.newModelId,
                  result.originalBlockName,
                  result.originalBlockApiKey
                );
                
                if (renameResult.success) {
                  // Update the displayed result with the final name/api_key
                  setConversionState({
                    status: 'success',
                    result: {
                      newModelId: result.newModelId,
                      newModelApiKey: renameResult.finalApiKey,
                      migratedRecords: result.migratedRecordsCount,
                      convertedFields: result.convertedFieldsCount,
                    },
                  });
                  
                  if (renameResult.error) {
                    await ctx.notice(`Original block deleted and model renamed to "${renameResult.finalName}". Note: ${renameResult.error}`);
                  } else {
                    await ctx.notice(`Original block deleted and model renamed to "${renameResult.finalName}" (${renameResult.finalApiKey})!`);
                  }
                } else {
                  await ctx.notice(`Original block deleted, but could not rename model: ${renameResult.error}`);
                }
              } else {
                await ctx.notice('Original block deleted successfully!');
              }
            } catch (error) {
              await ctx.alert(`Failed to delete original block: ${error instanceof Error ? error.message : String(error)}`);
            }
          }
        } else {
          console.log('[DEBUG MODE] Skipping deletion dialog - original block preserved');
        }
      } else {
        setConversionState({
          status: 'error',
          message: result.error || 'Unknown error occurred',
        });
        await ctx.alert(`Conversion failed: ${result.error}`);
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      setConversionState({ status: 'error', message });
      await ctx.alert(`Conversion failed: ${message}`);
    }
  }, [client, selectedBlockId, ctx, debugOptions, debugSuffix]);

  // Reset state
  const handleReset = useCallback(() => {
    setSelectedBlockId('');
    setConversionState({ status: 'idle' });
  }, []);

  // Check for access token
  if (!ctx.currentUserAccessToken) {
    return (
      <Canvas ctx={ctx}>
        <div className={s.container}>
          <div className={s.error}>
            <h2>Missing Permission</h2>
            <p>
              This plugin requires the "Current user access token" permission to work.
              Please update the plugin settings to grant this permission.
            </p>
          </div>
        </div>
      </Canvas>
    );
  }

  return (
    <Canvas ctx={ctx}>
      <div className={s.container}>
        <div className={s.header}>
          <h1 className={s.title}>Block to Links Converter</h1>
          <p className={s.description}>
            Convert a block model into a regular model, transforming modular content fields
            into link fields while preserving all your content data.
          </p>
        </div>

        {/* Debug Mode Section */}
        <div className={s.debugSection}>
          <div className={s.debugHeader}>
            <h3 className={s.debugTitle}>Development Mode</h3>
          </div>
          
          <div className={s.debugControls}>
            <SwitchField
              id="debug-mode-toggle"
              name="debug-mode-toggle"
              label="Enable Debug Mode"
              hint="When enabled, no deletions will occur and all created items will have a suffix for safe, repeatable testing"
              value={debugModeEnabled}
              onChange={(newValue) => setDebugModeEnabled(newValue)}
            />
            
            {debugModeEnabled && (
              <div className={s.debugSuffixField}>
                <TextField
                  id="debug-suffix"
                  name="debug-suffix"
                  label="Debug Suffix"
                  hint="This suffix will be appended to all created models and fields"
                  value={debugSuffix}
                  onChange={(newValue) => setDebugSuffix(newValue)}
                  placeholder="_dev_001"
                />
                <Button
                  buttonType="muted"
                  buttonSize="s"
                  onClick={handleRegenerateSuffix}
                >
                  ↻ Regenerate
                </Button>
              </div>
            )}
          </div>

          {debugModeEnabled && (
            <div className={s.debugBanner}>
              <div className={s.debugBannerIcon}>🔧</div>
              <div className={s.debugBannerContent}>
                <strong>Debug Mode Active</strong>
                <p>
                  All operations are non-destructive. Created items will have suffix: <code>{debugSuffix}</code>
                </p>
                <ul className={s.debugBannerList}>
                  <li>✓ Original fields and blocks will be preserved</li>
                  <li>✓ New link fields will be created alongside originals</li>
                  <li>✓ Verbose logging enabled in browser console</li>
                  <li>✓ Safe to run multiple times</li>
                </ul>
              </div>
            </div>
          )}
        </div>

        {loadingBlocks ? (
          <div className={s.loading}>
            <Spinner size={32} />
            <p>Loading block models...</p>
          </div>
        ) : blockModels.length === 0 ? (
          <div className={s.empty}>
            <p>No block models found in this project.</p>
            <p>Create a block model first to use this plugin.</p>
          </div>
        ) : (
          <>
            <div className={s.selector}>
              <label htmlFor="block-model-select" style={{ display: 'block', marginBottom: '8px', fontWeight: 600 }}>
                Select Block Model
              </label>
              <select
                id="block-model-select"
                value={selectedBlockId}
                onChange={(e) => handleBlockSelect(e.target.value)}
                style={{
                  width: '100%',
                  padding: '8px 12px',
                  fontSize: '14px',
                  border: '1px solid var(--border-color, #ccc)',
                  borderRadius: '4px',
                  backgroundColor: 'white',
                }}
              >
                <option value="">-- Select a block ({blockModels.length} available) --</option>
                {blockModels.map((block) => (
                  <option key={block.id} value={block.id}>
                    {block.name} ({block.api_key})
                  </option>
                ))}
              </select>
              <p style={{ fontSize: '12px', color: '#666', marginTop: '4px' }}>
                Choose the block model you want to convert to a regular model
              </p>
            </div>

            {/* Analysis State */}
            {conversionState.status === 'analyzing' && (
              <div className={s.loading}>
                <Spinner size={32} />
                <p>Analyzing block structure...</p>
              </div>
            )}

            {/* Analysis Results */}
            {conversionState.status === 'analyzed' && (
              <div className={s.analysis}>
                <h2>Analysis Results</h2>

                <div className={s.analysisSection}>
                  <h3>Block: {conversionState.analysis.block.name}</h3>
                  <p>API Key: <code>{conversionState.analysis.block.apiKey}</code></p>
                </div>

                <div className={s.analysisSection}>
                  <h3>Fields ({conversionState.analysis.fields.length})</h3>
                  <ul className={s.fieldList}>
                    {conversionState.analysis.fields.map((field) => (
                      <li key={field.id}>
                        <strong>{field.label}</strong> ({field.apiKey}) - {field.fieldType}
                        {field.localized && <span className={s.badge}>Localized</span>}
                      </li>
                    ))}
                  </ul>
                </div>

                <div className={s.analysisSection}>
                  <h3>Used In ({conversionState.analysis.modularContentFields.length} fields)</h3>
                  {conversionState.analysis.modularContentFields.length === 0 ? (
                    <p className={s.warning}>
                      This block is not used in any modular content fields.
                    </p>
                  ) : (
                    <ul className={s.fieldList}>
                      {conversionState.analysis.modularContentFields.map((field) => (
                        <li key={field.id}>
                          <strong>{field.parentModelName}</strong>.{field.apiKey}
                          {field.parentIsBlock && <span className={s.badgeNested}>Nested in Block</span>}
                          {field.localized && <span className={s.badge}>Localized</span>}
                          {field.allowedBlockIds.length > 1 && (
                            <span className={s.badge}>
                              {field.allowedBlockIds.length} block types
                            </span>
                          )}
                        </li>
                      ))}
                    </ul>
                  )}
                </div>

                <div className={s.analysisSection}>
                  <h3>Impact</h3>
                  <p>
                    <strong>{conversionState.analysis.totalAffectedRecords}</strong> records will be affected
                  </p>
                </div>

                {conversionState.analysis.modularContentFields.length > 0 && (
                  <div className={s.actions}>
                    <Button
                      onClick={handleConvert}
                      buttonType={debugOptions.enabled ? 'primary' : 'negative'}
                      buttonSize="l"
                    >
                      {debugOptions.enabled ? '🔧 Convert (Debug Mode)' : 'Convert Block to Model'}
                    </Button>
                    <Button onClick={handleReset} buttonType="muted">
                      Cancel
                    </Button>
                  </div>
                )}
              </div>
            )}

            {/* Converting State */}
            {conversionState.status === 'converting' && (
              <div className={debugOptions.enabled ? s.convertingDebug : s.converting}>
                {debugOptions.enabled && (
                  <div className={s.convertingDebugBadge}>🔧 DEBUG MODE</div>
                )}
                <div className={s.progressHeader}>
                  <Spinner size={24} />
                  <h2>{debugOptions.enabled ? 'Converting (Debug)...' : 'Converting...'}</h2>
                </div>

                <div className={s.progressBar}>
                  <div
                    className={debugOptions.enabled ? s.progressFillDebug : s.progressFill}
                    style={{ width: `${conversionState.progress.percentage}%` }}
                  />
                </div>

                <p className={s.progressStep}>
                  Step {conversionState.progress.currentStep} of {conversionState.progress.totalSteps}
                </p>
                <p className={s.progressDescription}>
                  {conversionState.progress.stepDescription}
                </p>
                {conversionState.progress.details && (
                  <p className={s.progressDetails}>{conversionState.progress.details}</p>
                )}
                {debugOptions.enabled && (
                  <p className={s.progressDebugNote}>
                    Check browser console for detailed logs
                  </p>
                )}
              </div>
            )}

            {/* Success State */}
            {conversionState.status === 'success' && (
              <div className={debugOptions.enabled ? s.successDebug : s.success}>
                <h2>{debugOptions.enabled ? '🔧 Debug Conversion Complete!' : '✓ Conversion Complete!'}</h2>
                <div className={s.successDetails}>
                  <p>
                    <strong>New Model:</strong> {conversionState.result.newModelApiKey}
                  </p>
                  <p>
                    <strong>Records Migrated:</strong> {conversionState.result.migratedRecords}
                  </p>
                  <p>
                    <strong>Fields Converted:</strong> {conversionState.result.convertedFields}
                  </p>
                  {debugOptions.enabled && (
                    <>
                      <hr style={{ margin: '12px 0', border: 'none', borderTop: '1px solid #ddd' }} />
                      <p style={{ color: '#666', fontSize: '0.9em' }}>
                        <strong>Debug Mode:</strong> Original fields and blocks preserved
                      </p>
                      <p style={{ color: '#666', fontSize: '0.9em' }}>
                        <strong>Suffix Used:</strong> <code>{debugSuffix}</code>
                      </p>
                    </>
                  )}
                </div>
                <Button onClick={handleReset} buttonType="primary">
                  Convert Another Block
                </Button>
              </div>
            )}

            {/* Error State */}
            {conversionState.status === 'error' && (
              <div className={s.error}>
                <h2>Error</h2>
                <p>{conversionState.message}</p>
                <Button onClick={handleReset} buttonType="primary">
                  Try Again
                </Button>
              </div>
            )}
          </>
        )}
      </div>
    </Canvas>
  );
}
