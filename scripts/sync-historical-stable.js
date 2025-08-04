// scripts/sync-historical-stable.js - More stable historical sync
const { Web3 } = require('web3');
const sqlite3 = require('sqlite3').verbose();
const path = require('path');
const fs = require('fs');
require('dotenv').config();

// Configuration - optimized for stability
const BATCH_SIZE = 20; // Smaller batches to avoid timeouts
const CONCURRENT_BATCHES = 1; // Sequential processing to avoid rate limits
const PROGRESS_INTERVAL = 5000; // Log progress every 5k blocks
const RATE_LIMIT_DELAY = 200; // Increased delay between calls
const MAX_RETRIES = 5; // Retry failed operations
const RETRY_DELAY = 5000; // Wait 5 seconds between retries
const CHECKPOINT_INTERVAL = 1000; // Save progress every 1000 blocks

// Initialize Web3 with timeout settings
const web3 = new Web3(new Web3.providers.HttpProvider(process.env.RPC_URL || 'https://rpc.reactive.network', {
  timeout: 30000, // 30 second timeout
  headers: {
    'Content-Type': 'application/json',
  }
}));

// Use main database directly with busy timeout
const dbPath = process.env.NODE_ENV === 'production' 
  ? '/app/data/burns.db'
  : './burns.db';

const db = new sqlite3.Database(dbPath);

// Set busy timeout to handle concurrent access
db.run("PRAGMA busy_timeout = 30000"); // 30 seconds
db.run("PRAGMA journal_mode = WAL"); // Write-Ahead Logging for better concurrency

// Important addresses
const REACT_TOKEN_ADDRESS = process.env.REACT_TOKEN_ADDRESS || '0x0000000000000000000000000000000000fffFfF';
const SYSTEM_CONTRACT_ADDRESS = '0x0000000000000000000000000000000000fffFfF';

// Statistics tracking
let stats = {
  startTime: Date.now(),
  blocksProcessed: 0,
  transactionsProcessed: 0,
  totalBurned: 0,
  errors: 0,
  lastBlock: 0,
  emptyBlocks: 0,
  blocksWithTx: 0,
  targetBlock: 0,
  retriedOperations: 0,
  checkpointBlock: 0
};

// Graceful error handling
process.on('unhandledRejection', (reason, promise) => {
  console.error('Unhandled Rejection at:', promise, 'reason:', reason);
  // Don't exit, just log and continue
});

// Retry wrapper for resilient operations
async function withRetry(operation, context = 'operation') {
  let lastError;
  
  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    try {
      return await operation();
    } catch (error) {
      lastError = error;
      stats.retriedOperations++;
      
      console.warn(`Attempt ${attempt}/${MAX_RETRIES} failed for ${context}: ${error.message}`);
      
      if (attempt < MAX_RETRIES) {
        // Exponential backoff
        const delay = RETRY_DELAY * Math.pow(2, attempt - 1);
        console.log(`Waiting ${delay}ms before retry...`);
        await new Promise(resolve => setTimeout(resolve, delay));
      }
    }
  }
  
  throw new Error(`Failed after ${MAX_RETRIES} attempts: ${lastError.message}`);
}

// Get the deployment block to stop at
async function getDeploymentBlock() {
  return new Promise((resolve) => {
    db.get('SELECT value FROM metadata WHERE key = ?', ['deployment_block'], (err, row) => {
      if (row) {
        resolve(parseInt(row.value));
      } else {
        resolve(null);
      }
    });
  });
}

// Get checkpoint block (where we left off)
async function getCheckpointBlock() {
  return new Promise((resolve) => {
    db.get('SELECT value FROM metadata WHERE key = ?', ['historical_sync_checkpoint'], (err, row) => {
      if (row) {
        resolve(parseInt(row.value));
      } else {
        resolve(0);
      }
    });
  });
}

// Save checkpoint
async function saveCheckpoint(blockNumber) {
  return new Promise((resolve, reject) => {
    db.run(
      `INSERT OR REPLACE INTO metadata (key, value) VALUES (?, ?)`,
      ['historical_sync_checkpoint', blockNumber.toString()],
      (err) => {
        if (err) reject(err);
        else resolve();
      }
    );
  });
}

// Optimized transaction processing
async function processTransaction(tx, block, receipt) {
  try {
    if (receipt && receipt.gasUsed) {
      const gasUsed = BigInt(receipt.gasUsed);
      let effectiveGasPrice = BigInt(0);
      
      if (receipt.effectiveGasPrice) {
        effectiveGasPrice = BigInt(receipt.effectiveGasPrice);
      } else if (tx.gasPrice) {
        effectiveGasPrice = BigInt(tx.gasPrice);
      } else if (block.baseFeePerGas) {
        const baseFee = BigInt(block.baseFeePerGas);
        const priorityFee = tx.maxPriorityFeePerGas ? BigInt(tx.maxPriorityFeePerGas) : BigInt(0);
        effectiveGasPrice = baseFee + priorityFee;
      }
      
      const burnedWei = gasUsed * effectiveGasPrice;
      const burnedReact = parseFloat(web3.utils.fromWei(burnedWei.toString(), 'ether'));
      
      if (burnedReact > 0) {
        // Simplified burn type detection for speed
        let burnType = 'gas_fee';
        if (tx.to && tx.to.toLowerCase() === SYSTEM_CONTRACT_ADDRESS.toLowerCase()) {
          burnType = 'system_contract_payment';
        } else if (receipt.gasUsed === 21000) {
          burnType = 'gas_fee_transfer';
        } else {
          burnType = 'gas_fee_contract';
        }
        
        return {
          tx_hash: tx.hash,
          block_number: Number(block.number),
          amount: burnedReact.toFixed(18),
          from_address: tx.from,
          timestamp: Number(block.timestamp),
          usd_value: 0,
          burn_type: burnType,
          gas_used: Number(receipt.gasUsed)
        };
      }
    }
    return null;
  } catch (error) {
    stats.errors++;
    return null;
  }
}

// Process a single block with retry logic
async function processSingleBlock(blockNumber) {
  return withRetry(async () => {
    const block = await web3.eth.getBlock(blockNumber, true);
    const burns = [];
    
    if (block && block.transactions && block.transactions.length > 0) {
      stats.blocksWithTx++;
      
      // Process transactions in smaller chunks
      const txChunks = [];
      for (let i = 0; i < block.transactions.length; i += 5) {
        txChunks.push(block.transactions.slice(i, i + 5));
      }
      
      for (const chunk of txChunks) {
        // Process chunk with individual transaction retries
        for (const tx of chunk) {
          try {
            const receipt = await withRetry(
              () => web3.eth.getTransactionReceipt(tx.hash),
              `receipt for ${tx.hash}`
            );
            
            if (receipt) {
              const burn = await processTransaction(tx, block, receipt);
              if (burn) {
                burns.push(burn);
                stats.totalBurned += parseFloat(burn.amount);
              }
              stats.transactionsProcessed++;
            }
          } catch (error) {
            console.error(`Failed to process tx ${tx.hash} after retries:`, error.message);
            stats.errors++;
          }
        }
        
        // Small delay between chunks
        await new Promise(resolve => setTimeout(resolve, 50));
      }
    } else {
      stats.emptyBlocks++;
    }
    
    return burns;
  }, `block ${blockNumber}`);
}

// Process blocks in batch with checkpoint saving
async function processBatch(startBlock, endBlock) {
  const allBurns = [];
  
  try {
    // Process blocks one by one for better error handling
    for (let blockNum = startBlock; blockNum <= endBlock; blockNum++) {
      try {
        // Add delay before each block
        await new Promise(resolve => setTimeout(resolve, RATE_LIMIT_DELAY));
        
        const burns = await processSingleBlock(blockNum);
        allBurns.push(...burns);
        
        stats.blocksProcessed++;
        stats.lastBlock = blockNum;
        
        // Save checkpoint periodically
        if (blockNum % CHECKPOINT_INTERVAL === 0) {
          await saveCheckpoint(blockNum);
          stats.checkpointBlock = blockNum;
          console.log(`Checkpoint saved at block ${blockNum}`);
        }
        
      } catch (error) {
        console.error(`Error processing block ${blockNum}:`, error.message);
        stats.errors++;
        // Continue with next block instead of failing entire batch
      }
    }
    
    // Batch insert burns with retry
    if (allBurns.length > 0) {
      await withRetry(async () => {
        return new Promise((resolve, reject) => {
          db.serialize(() => {
            db.run('BEGIN TRANSACTION', (err) => {
              if (err) {
                reject(err);
                return;
              }
              
              const stmt = db.prepare(`
                INSERT OR IGNORE INTO burns 
                (tx_hash, block_number, amount, from_address, timestamp, usd_value, burn_type, gas_used) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
              `);
              
              let insertCount = 0;
              for (const burn of allBurns) {
                stmt.run([
                  burn.tx_hash,
                  burn.block_number,
                  burn.amount,
                  burn.from_address,
                  burn.timestamp,
                  burn.usd_value,
                  burn.burn_type,
                  burn.gas_used
                ], (err) => {
                  if (err) console.error('Insert error:', err);
                  else insertCount++;
                });
              }
              
              stmt.finalize((err) => {
                if (err) {
                  db.run('ROLLBACK', () => reject(err));
                  return;
                }
                
                db.run('COMMIT', (err) => {
                  if (err) {
                    db.run('ROLLBACK', () => reject(err));
                  } else {
                    console.log(`Inserted ${insertCount} burns from batch`);
                    resolve();
                  }
                });
              });
            });
          });
        });
      }, 'database insert');
    }
    
    // Update progress in metadata
    await saveCheckpoint(stats.lastBlock);
    
  } catch (error) {
    console.error(`Critical error in batch ${startBlock}-${endBlock}:`, error.message);
    stats.errors++;
  }
  
  // Log progress
  if (stats.blocksProcessed % PROGRESS_INTERVAL === 0 || stats.blocksProcessed === stats.targetBlock) {
    const elapsed = (Date.now() - stats.startTime) / 1000;
    const blocksPerSecond = stats.blocksProcessed / elapsed;
    const eta = (stats.targetBlock - stats.lastBlock) / blocksPerSecond;
    
    console.log(`\n=== Progress Update ===`);
    console.log(`Blocks: ${stats.lastBlock.toLocaleString()}/${stats.targetBlock.toLocaleString()} (${(stats.lastBlock/stats.targetBlock*100).toFixed(2)}%)`);
    console.log(`Blocks with TX: ${stats.blocksWithTx.toLocaleString()} | Empty: ${stats.emptyBlocks.toLocaleString()}`);
    console.log(`Transactions: ${stats.transactionsProcessed.toLocaleString()}`);
    console.log(`Total Burned: ${stats.totalBurned.toFixed(4)} REACT`);
    console.log(`Speed: ${blocksPerSecond.toFixed(1)} blocks/sec`);
    console.log(`ETA: ${(eta/3600).toFixed(1)} hours`);
    console.log(`Errors: ${stats.errors} | Retries: ${stats.retriedOperations}`);
    console.log(`Last checkpoint: Block ${stats.checkpointBlock}`);
  }
}

// Main sync function
async function syncHistorical() {
  console.log('=== REACT Burn Historical Sync (STABLE VERSION) ===\n');
  
  try {
    // Get sync range
    const deploymentBlock = await getDeploymentBlock();
    if (!deploymentBlock) {
      console.error('No deployment block found. Please run the main tracker first.');
      process.exit(1);
    }
    
    stats.targetBlock = deploymentBlock;
    
    // Check for existing checkpoint
    const checkpointBlock = await getCheckpointBlock();
    const startFromBlock = checkpointBlock || 0;
    
    if (startFromBlock > 0) {
      console.log(`Resuming from checkpoint: Block ${startFromBlock}`);
      stats.blocksProcessed = startFromBlock; // Adjust stats
    }
    
    console.log(`Target: Sync from block ${startFromBlock} to ${deploymentBlock.toLocaleString()}`);
    console.log(`Starting historical sync...\n`);
    
    // Process in sequential batches for stability
    let currentBlock = startFromBlock;
    
    while (currentBlock < deploymentBlock) {
      const batchEnd = Math.min(currentBlock + BATCH_SIZE - 1, deploymentBlock);
      
      console.log(`Processing batch: ${currentBlock} to ${batchEnd}`);
      await processBatch(currentBlock, batchEnd);
      
      currentBlock = batchEnd + 1;
      
      // Small pause between batches
      await new Promise(resolve => setTimeout(resolve, 1000));
    }
    
    // Final statistics
    const elapsed = (Date.now() - stats.startTime) / 1000;
    console.log('\n=== Historical Sync Complete! ===');
    console.log(`Total time: ${(elapsed/3600).toFixed(2)} hours`);
    console.log(`Blocks processed: ${stats.blocksProcessed.toLocaleString()}`);
    console.log(`Transactions processed: ${stats.transactionsProcessed.toLocaleString()}`);
    console.log(`Total REACT burned: ${stats.totalBurned.toFixed(8)}`);
    console.log(`Errors encountered: ${stats.errors}`);
    console.log(`Operations retried: ${stats.retriedOperations}`);
    
    // Clear checkpoint on successful completion
    await saveCheckpoint(deploymentBlock);
    console.log('Sync completed successfully!');
    
  } catch (error) {
    console.error('Fatal error during sync:', error);
    process.exit(1);
  } finally {
    db.close();
  }
}

// Handle graceful shutdown
process.on('SIGINT', () => {
  console.log('\n\nSync interrupted, saving progress...');
  saveCheckpoint(stats.lastBlock).then(() => {
    console.log(`Progress saved at block ${stats.lastBlock}`);
    db.close();
    process.exit(0);
  });
});

process.on('SIGTERM', () => {
  console.log('\n\nReceived SIGTERM, saving progress...');
  saveCheckpoint(stats.lastBlock).then(() => {
    console.log(`Progress saved at block ${stats.lastBlock}`);
    db.close();
    process.exit(0);
  });
});

// Start sync
syncHistorical();
