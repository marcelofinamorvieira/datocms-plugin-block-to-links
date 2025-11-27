import { useState, useEffect, useMemo, useCallback } from 'react';
import type { RenderConfigScreenCtx } from 'datocms-plugin-sdk';
import {
  Canvas,
  Button,
  Spinner,
  SwitchField,
  TextField,
  Form,
  SelectField,
  Section,
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

type Option = {
  label: string;
  value: string;
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

// Icons
const Icons = {
  Block: () => (
    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <rect x="2" y="7" width="20" height="14" rx="2" ry="2"></rect>
      <path d="M16 21V5a2 2 0 0 0-2-2h-4a2 2 0 0 0-2 2v16"></path>
    </svg>
  ),
  Database: () => (
    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <ellipse cx="12" cy="5" rx="9" ry="3"></ellipse>
      <path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3"></path>
      <path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5"></path>
    </svg>
  ),
  Field: () => (
    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <path d="M4 17l6-6"></path>
      <path d="M4 7l6 6"></path>
      <path d="M20 7h-6"></path>
      <path d="M20 17h-6"></path>
    </svg>
  ),
  Check: () => (
    <svg width="48" height="48" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" className={s.checkIcon}>
      <path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"></path>
      <polyline points="22 4 12 14.01 9 11.01"></polyline>
    </svg>
  ),
  Warning: () => (
    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="#ff9800" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"></path>
      <line x1="12" y1="9" x2="12" y2="13"></line>
      <line x1="12" y1="17" x2="12.01" y2="17"></line>
    </svg>
  ),
  Code: () => (
     <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <polyline points="16 18 22 12 16 6"></polyline>
      <polyline points="8 6 2 12 8 18"></polyline>
    </svg>
  )
};

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
  const [originalBlockDeleted, setOriginalBlockDeleted] = useState(false);
  const [deletingBlock, setDeletingBlock] = useState(false);
  
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
    setOriginalBlockDeleted(false);
    setDeletingBlock(false);
  }, []);

  // Handle deleting the original block
  const handleDeleteOriginalBlock = useCallback(async () => {
    if (!client || conversionState.status !== 'success') return;

    setDeletingBlock(true);
    try {
      await deleteOriginalBlock(client, selectedBlockId);
      
      const result = conversionState.result;
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
              migratedRecords: result.migratedRecords,
              convertedFields: result.convertedFields,
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
      
      setOriginalBlockDeleted(true);
    } catch (error) {
      await ctx.alert(`Failed to delete original block: ${error instanceof Error ? error.message : String(error)}`);
    } finally {
      setDeletingBlock(false);
    }
  }, [client, selectedBlockId, conversionState, ctx]);

  const blockOptions: Option[] = useMemo(() => {
    return blockModels.map(block => ({
      label: `${block.name} (${block.api_key})`,
      value: block.id
    }));
  }, [blockModels]);

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
            Convert modular content blocks into independent models with links, preserving all data.
          </p>
        </div>

        {loadingBlocks ? (
          <div className={s.loading}>
            <Spinner size={32} />
            <p>Loading block models...</p>
          </div>
        ) : blockModels.length === 0 ? (
          <div className={s.empty}>
            <p>No block models found in this project.</p>
            <Button onClick={() => window.location.reload()}>Refresh</Button>
          </div>
        ) : (
          <Form>
            <div className={s.card}>
              <Section title="Block Selection">
                <SelectField
                  name="block"
                  id="block"
                  label="Choose a Block Model"
                  hint="Select the modular block model you wish to convert."
                  value={blockOptions.find(o => o.value === selectedBlockId)}
                  onChange={(newValue) => handleBlockSelect(newValue ? (newValue as Option).value : '')}
                  selectInputProps={{
                    options: blockOptions,
                  }}
                />
              </Section>
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
                <div className={s.card}>
                  <Section title="Analysis Results">
                    <div className={s.analysisGrid}>
                      <div className={s.analysisItem}>
                        <h3><span className={s.iconWrapper}><Icons.Block /></span> Block Details</h3>
                        <div className={s.analysisStat}>
                          <span className={s.statLabel}>Name</span>
                          <span className={s.statValueMain}>{conversionState.analysis.block.name}</span>
                        </div>
                        <div className={s.analysisStat}>
                          <span className={s.statLabel}>API Key</span>
                          <span className={s.statValueCode}>{conversionState.analysis.block.apiKey}</span>
                        </div>
                      </div>
                      <div className={s.analysisItem}>
                        <h3><span className={s.iconWrapper}><Icons.Database /></span> Usage Impact</h3>
                        <div className={s.analysisStat}>
                          <span className={s.statLabel}>Records</span>
                          <span className={s.statValueMain}>{conversionState.analysis.totalAffectedRecords}</span>
                        </div>
                        <div className={s.analysisStat}>
                          <span className={s.statLabel}>Locations</span>
                          <span className={s.statValueMain}>
                            {conversionState.analysis.modularContentFields.length}
                            {conversionState.analysis.modularContentFields.length === 0 && (
                              <span className={s.warningIcon} title="Not used in any fields"> <Icons.Warning /></span>
                            )}
                          </span>
                        </div>
                      </div>
                    </div>

                    <div className={s.analysisSection}>
                      <h3><span className={s.iconWrapper}><Icons.Field /></span> Content Fields ({conversionState.analysis.fields.length})</h3>
                      <ul className={s.fieldList}>
                        {conversionState.analysis.fields.map((field) => {
                          const internalDomain = (ctx.site as { attributes?: { internal_domain?: string | null } })?.attributes?.internal_domain;
                          const fieldUrl = internalDomain 
                            ? `https://${internalDomain}/schema/item_types/${selectedBlockId}#f${field.id}`
                            : undefined;
                          
                          return (
                            <li key={field.id} className={s.fieldListItemClickable}>
                              <a 
                                href={fieldUrl}
                                target="_blank"
                                rel="noopener noreferrer"
                                className={s.fieldListLink}
                              >
                                <div className={s.fieldInfo}>
                                  <strong>{field.label}</strong>
                                  <span className={s.fieldApiKey}><Icons.Code /> {field.apiKey}</span>
                                </div>
                                <div className={s.fieldMeta}>
                                  <span className={s.fieldType}>{field.fieldType}</span>
                                  {field.localized && <span className={s.badge}>Localized</span>}
                                </div>
                              </a>
                            </li>
                          );
                        })}
                      </ul>
                    </div>
                  </Section>

                  <div className={s.actions}>
                    <Button onClick={handleReset} buttonType="muted">
                      Cancel
                    </Button>
                    <Button
                      onClick={handleConvert}
                      buttonType={debugOptions.enabled ? 'primary' : 'negative'}
                      disabled={conversionState.analysis.modularContentFields.length === 0}
                    >
                      {debugOptions.enabled ? 'Convert (Debug Mode)' : 'Convert Block to Model'}
                    </Button>
                  </div>
                </div>
              </div>
            )}

            {/* Converting State */}
            {conversionState.status === 'converting' && (
              <div className={s.card}>
                <div className={s.converting}>
                  <div className={s.progressHeader}>
                    <h2>{debugOptions.enabled ? 'Converting (Debug)...' : 'Converting...'}</h2>
                  </div>

                  <div className={s.progressContainer}>
                    <div className={s.progressSteps}>
                      <span>Step {conversionState.progress.currentStep} of {conversionState.progress.totalSteps}</span>
                      <span>{Math.round(conversionState.progress.percentage)}%</span>
                    </div>
                    <div className={s.progressBar}>
                      <div
                        className={s.progressFill}
                        style={{ width: `${conversionState.progress.percentage}%` }}
                      />
                    </div>
                    <p className={s.currentStep}>
                      {conversionState.progress.stepDescription}
                    </p>
                    {conversionState.progress.details && (
                      <p className={s.progressDetails}>{conversionState.progress.details}</p>
                    )}
                  </div>
                  
                  <Spinner size={32} />
                </div>
              </div>
            )}

            {/* Success State */}
            {conversionState.status === 'success' && (
              <div className={s.card}>
                <div className={s.success}>
                  <div className={s.successIcon}><Icons.Check /></div>
                  <h2 className={s.successTitle}>Conversion Complete!</h2>
                  
                  <div className={s.successStats}>
                    <div className={s.statItem}>
                      <span className={s.statValue}>{conversionState.result.migratedRecords}</span>
                      <span className={s.statLabel}>Records Migrated</span>
                    </div>
                    <div className={s.statDivider} />
                    <div className={s.statItem}>
                      <span className={s.statValue}>{conversionState.result.convertedFields}</span>
                      <span className={s.statLabel}>Fields Converted</span>
                    </div>
                  </div>

                  <div className={s.successDetails}>
                    <p>
                      New Model Created: <strong>{conversionState.result.newModelApiKey}</strong>
                    </p>
                    {debugOptions.enabled && (
                       <p className={s.progressDebugNote}>
                        Debug Mode: Original fields preserved. Suffix: {debugSuffix}
                      </p>
                    )}
                  </div>

                  <div className={s.actions} style={{ justifyContent: 'center' }}>
                    {!debugOptions.enabled && !originalBlockDeleted && (
                      <Button 
                        onClick={handleDeleteOriginalBlock} 
                        buttonType="negative"
                        disabled={deletingBlock}
                      >
                        {deletingBlock ? 'Deleting...' : 'Delete Original Block'}
                      </Button>
                    )}
                    <Button onClick={handleReset} buttonType="primary">
                      Convert Another Block
                    </Button>
                  </div>
                </div>
              </div>
            )}

            {/* Error State */}
            {conversionState.status === 'error' && (
              <div className={s.card}>
                <div className={s.error}>
                  <h2>Something went wrong</h2>
                  <p>{conversionState.message}</p>
                  <Button onClick={handleReset} buttonType="primary">
                    Try Again
                  </Button>
                </div>
              </div>
            )}

            {/* Debug Mode Toggle */}
            <div className={s.debugToggleWrapper}>
              <div className={s.debugToggle}>
                <SwitchField
                  id="debug-mode-toggle"
                  name="debug-mode-toggle"
                  label="Advanced Mode"
                  value={debugModeEnabled}
                  onChange={(newValue) => setDebugModeEnabled(newValue)}
                />
              </div>
            </div>

            {debugModeEnabled && (
              <div className={s.debugSection}>
                <div className={s.debugHeader}>
                   <span>🔧</span> Debug Configuration
                </div>
                
                <TextField
                  id="debug-suffix"
                  name="debug-suffix"
                  label="Debug Suffix"
                  hint="Appended to created models/fields to prevent conflicts."
                  value={debugSuffix}
                  onChange={(newValue) => setDebugSuffix(newValue)}
                  placeholder="_dev_001"
                  textInputProps={{
                    monospaced: true,
                  }}
                />
                
                <div style={{ marginTop: 'var(--spacing-m)' }}>
                   <Button
                    buttonType="muted"
                    buttonSize="s"
                    onClick={handleRegenerateSuffix}
                  >
                    Regenerate Suffix
                  </Button>
                </div>

                <div className={s.debugBannerList} style={{ marginTop: 'var(--spacing-m)' }}>
                  <p>• Non-destructive: original fields/blocks preserved</p>
                  <p>• Verbose logging enabled in console</p>
                </div>
              </div>
            )}
          </Form>
        )}
      </div>
    </Canvas>
  );
}
