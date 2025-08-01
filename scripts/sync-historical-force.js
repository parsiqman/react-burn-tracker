// scripts/sync-historical-force.js - Force historical sync to fill gaps
const { Web3 } = require('web3');
const sqlite3 = require('sqlite3').verbose();
const path = require('path');
const fs = require('fs');
require('dotenv').config();

// Configuration - optimized for speed
const BATCH_SIZE = 50; // Reduced for better stability
const CONCURRENT_BATCHES = 2; // Reduced to avoid database conflicts
const PROGRESS_INTERVAL = 10000; // Log progress every 10k blocks
const RATE_LIMIT_DELAY = 100; // Increased delay between calls

// Initialize Web3
const web3 = new Web3(process.env.RPC_URL || 'https://rpc.reactive.network');

// Use main database directly
const dbPath = process.env.NODE_ENV === 'production' 
  ? '/app/data/burns.db'
  : './burns.db';

const db = new sqlite3.Database(dbPath);

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
  skippedBlocks: 0
};

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

// Check if block already exists
async function blockExists(blockNumber) {
  return new Promise((resolve) => {
    db.get('SELECT COUNT(*) as count FROM burns WHERE block_number = ?', [blockNumber], (err, row) => {
      resolve(row && row.count > 0);
    });
  });
}

// Get gap information
async function findGaps() {
  return new Promise((resolve) => {
    db.all(`
      SELECT 
        block_number,
        LEAD(block_number) OVER (ORDER BY block_number) as next_block
      FROM (
        SELECT DISTINCT block_number FROM burns ORDER BY block_number
      )
    `, (err, rows) => {
      if (err) {
        console.error('Error finding gaps:', err);
        resolve([]);
        return;
      }
      
      const gaps = [];
      for (const row of rows) {
        if (row.next_block && row.next_block - row.block_number > 1) {
          gaps.push({
            start: row.block_number + 1,
            end: row.next_block - 1,
            size: row.next_block - row.block_number - 1
          });
        }
      }
      resolve(gaps);
    });
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
        
        // Queue for batch insert
        return {
          tx_hash: tx.hash,
          block_number: Number(block.number),
          amount: burnedReact.toFixed(18),
          from_address: tx.from,
          timestamp: Number(block.timestamp),
          usd_value: 0, // Historical price would need separate data
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

// Process blocks in batch
async function processBatch(startBlock, endBlock) {
  const burns = [];
  
  try {
    // Fetch blocks sequentially to avoid batch issues
    const blocks = [];
    
    for (let i = startBlock; i <= endBlock; i++) {
      // Skip blocks we already have
      if (await blockExists(i)) {
        stats.skippedBlocks++;
        stats.blocksProcessed++;
        stats.lastBlock = i;
        continue;
      }
      
      try {
        // Add small delay to avoid rate limiting
        await new Promise(resolve => setTimeout(resolve, RATE_LIMIT_DELAY));
        
        const block = await web3.eth.getBlock(i, true);
        if (block) {
          blocks.push(block);
        }
        stats.blocksProcessed++;
        stats.lastBlock = i;
      } catch (error) {
        console.error(`Error fetching block ${i}:`, error.message);
        stats.errors++;
      }
    }
    
    // Process blocks with transactions
    for (const block of blocks) {
      if (block && block.transactions && block.transactions.length > 0) {
        stats.blocksWithTx++;
        
        // Process transactions sequentially to avoid overwhelming RPC
        for (const tx of block.transactions) {
          try {
            const receipt = await web3.eth.getTransactionReceipt(tx.hash);
            if (receipt) {
              const burn = await processTransaction(tx, block, receipt);
              if (burn) {
                burns.push(burn);
                stats.totalBurned += parseFloat(burn.amount);
              }
              stats.transactionsProcessed++;
            }
          } catch (error) {
            console.error(`Error processing tx ${tx.hash}:`, error.message);
            stats.errors++;
          }
        }
      } else {
        stats.emptyBlocks++;
      }
    }
    
    // Batch insert burns
    if (burns.length > 0) {
      await new Promise((resolve, reject) => {
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
            
            for (const burn of burns) {
              stmt.run([
                burn.tx_hash,
                burn.block_number,
                burn.amount,
                burn.from_address,
                burn.timestamp,
                burn.usd_value,
                burn.burn_type,
                burn.gas_used
              ]);
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
                  resolve();
                }
              });
            });
          });
        });
      });
    }
    
    // Update progress in metadata
    await new Promise((resolve) => {
      db.run(
        `INSERT OR REPLACE INTO metadata (key, value) VALUES (?, ?)`,
        ['historical_sync_block', stats.lastBlock.toString()],
        resolve
      );
    });
    
  } catch (error) {
    console.error(`Error processing batch ${startBlock}-${endBlock}:`, error.message);
    stats.errors++;
  }
  
  // Log progress
  if (stats.blocksProcessed % PROGRESS_INTERVAL === 0) {
    const elapsed = (Date.now() - stats.startTime) / 1000;
    const blocksPerSecond = stats.blocksProcessed / elapsed;
    const eta = (stats.targetBlock - stats.lastBlock) / blocksPerSecond;
    
    console.log(`\n=== Progress Update ===`);
    console.log(`Blocks: ${stats.lastBlock.toLocaleString()}/${stats.targetBlock.toLocaleString()} (${(stats.lastBlock/stats.targetBlock*100).toFixed(2)}%)`);
    console.log(`Blocks with TX: ${stats.blocksWithTx.toLocaleString()} | Empty: ${stats.emptyBlocks.toLocaleString()} | Skipped: ${stats.skippedBlocks.toLocaleString()}`);
    console.log(`Transactions: ${stats.transactionsProcessed.toLocaleString()}`);
    console.log(`Total Burned: ${stats.totalBurned.toFixed(4)} REACT`);
    console.log(`Speed: ${blocksPerSecond.toFixed(1)} blocks/sec`);
    console.log(`ETA: ${(eta/3600).toFixed(1)} hours`);
    console.log(`Errors: ${stats.errors}`);
  }
}

// Main sync function
async function syncHistorical() {
  console.log('=== REACT Burn Historical Sync (FORCE MODE) ===\n');
  
  try {
    // Get sync range
    const deploymentBlock = await getDeploymentBlock();
    if (!deploymentBlock) {
      console.error('No deployment block found. Please run the main tracker first.');
      process.exit(1);
    }
    
    stats.targetBlock = deploymentBlock;
    
    // Find gaps in our data
    console.log('Analyzing gaps in historical data...');
    const gaps = await findGaps();
    
    if (gaps.length === 0) {
      console.log('No gaps found in historical data!');
      
      // Check if we need to sync from 0
      const minBlock = await new Promise((resolve) => {
        db.get('SELECT MIN(block_number) as min FROM burns', (err, row) => {
          resolve(row?.min || deploymentBlock);
        });
      });
      
      if (minBlock > 1000) {
        gaps.push({
          start: 0,
          end: minBlock - 1,
          size: minBlock
        });
      }
    }
    
    console.log(`Found ${gaps.length} gaps to fill:`);
    for (const gap of gaps) {
      console.log(`  - Blocks ${gap.start.toLocaleString()} to ${gap.end.toLocaleString()} (${gap.size.toLocaleString()} blocks)`);
    }
    
    // Process the largest gap (which should be the main one)
    const largestGap = gaps.reduce((max, gap) => gap.size > max.size ? gap : max, { size: 0 });
    
    if (largestGap.size > 0) {
      console.log(`\nProcessing largest gap: blocks ${largestGap.start} to ${largestGap.end}`);
      console.log('This will take several hours...\n');
      
      // Process in batches with concurrency
      const promises = [];
      let currentBatch = largestGap.start;
      
      while (currentBatch <= largestGap.end) {
        // Wait if we have too many concurrent batches
        if (promises.length >= CONCURRENT_BATCHES) {
          await Promise.race(promises);
          // Remove completed promises
          for (let i = promises.length - 1; i >= 0; i--) {
            if (promises[i].resolved) {
              promises.splice(i, 1);
            }
          }
        }
        
        // Create new batch
        const batchEnd = Math.min(currentBatch + BATCH_SIZE - 1, largestGap.end);
        const batchPromise = processBatch(currentBatch, batchEnd);
        batchPromise.resolved = false;
        batchPromise.then(() => { batchPromise.resolved = true; });
        promises.push(batchPromise);
        
        currentBatch = batchEnd + 1;
      }
      
      // Wait for all remaining batches
      await Promise.all(promises);
    }
    
    // Final statistics
    const elapsed = (Date.now() - stats.startTime) / 1000;
    console.log('\n=== Historical Sync Complete! ===');
    console.log(`Total time: ${(elapsed/3600).toFixed(2)} hours (${(elapsed/60).toFixed(1)} minutes)`);
    console.log(`Blocks processed: ${stats.blocksProcessed.toLocaleString()}`);
    console.log(`Blocks skipped (already had data): ${stats.skippedBlocks.toLocaleString()}`);
    console.log(`Blocks with transactions: ${stats.blocksWithTx.toLocaleString()}`);
    console.log(`Empty blocks: ${stats.emptyBlocks.toLocaleString()}`);
    console.log(`Transactions processed: ${stats.transactionsProcessed.toLocaleString()}`);
    console.log(`Total REACT burned: ${stats.totalBurned.toFixed(8)}`);
    console.log(`Average burn per TX: ${(stats.totalBurned/stats.transactionsProcessed).toFixed(8)} REACT`);
    console.log(`Errors: ${stats.errors}`);
    
    // Get final database stats
    const dbStats = await new Promise((resolve) => {
      db.get(
        `SELECT COUNT(*) as count, SUM(CAST(amount AS REAL)) as total FROM burns`,
        (err, row) => resolve(row)
      );
    });
    
    console.log(`\nDatabase now contains:`);
    console.log(`- ${dbStats.count.toLocaleString()} burn records`);
    console.log(`- ${dbStats.total.toFixed(8)} total REACT burned`);
    
  } catch (error) {
    console.error('Fatal error during sync:', error);
  } finally {
    db.close();
  }
}

// Handle graceful shutdown
process.on('SIGINT', () => {
  console.log('\n\nSync interrupted, saving progress...');
  db.run(
    `INSERT OR REPLACE INTO metadata (key, value) VALUES (?, ?)`,
    ['historical_sync_block', stats.lastBlock.toString()],
    () => {
      console.log(`Progress saved at block ${stats.lastBlock}`);
      db.close();
      process.exit(0);
    }
  );
});

// Start sync
syncHistorical();
