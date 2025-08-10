// scripts/scan-all-native-holders.js
// Comprehensive scanner for ALL REACT native token holders on Reactive Network

const { Web3 } = require('web3');
const sqlite3 = require('sqlite3').verbose();
const path = require('path');
const fs = require('fs');
require('dotenv').config();

// Configuration
const BATCH_SIZE = 100; // Number of blocks to process at once
const BALANCE_CHECK_BATCH = 50; // Number of addresses to check balance at once
const MIN_BALANCE = 0.000001; // Minimum balance to be considered a holder
const CHECKPOINT_INTERVAL = 1000; // Save progress every N blocks
const RPC_DELAY = 100; // Delay between RPC calls in ms

// Initialize Web3
const web3 = new Web3(process.env.RPC_URL || 'https://rpc.reactive.network');

// Database path
const dbPath = process.env.NODE_ENV === 'production' 
  ? '/app/data/burns.db'
  : './burns.db';

const db = new sqlite3.Database(dbPath);

// Set database optimizations
db.run("PRAGMA journal_mode = WAL");
db.run("PRAGMA busy_timeout = 30000");
db.run("PRAGMA cache_size = 10000");

// Statistics
let stats = {
  startTime: Date.now(),
  blocksProcessed: 0,
  transactionsProcessed: 0,
  uniqueAddresses: new Set(),
  holdersFound: 0,
  totalSupply: 0,
  lastCheckpoint: 0,
  errors: 0
};

// Get or set scan checkpoint
async function getCheckpoint() {
  return new Promise((resolve) => {
    db.get('SELECT value FROM metadata WHERE key = ?', ['native_holder_scan_checkpoint'], (err, row) => {
      resolve(row ? parseInt(row.value) : 0);
    });
  });
}

async function saveCheckpoint(blockNumber) {
  return new Promise((resolve) => {
    db.run(
      'INSERT OR REPLACE INTO metadata (key, value) VALUES (?, ?)',
      ['native_holder_scan_checkpoint', blockNumber.toString()],
      resolve
    );
  });
}

// Extract all addresses from a block
async function extractAddressesFromBlock(blockNumber) {
  const addresses = new Set();
  
  try {
    // Add delay to avoid rate limiting
    await new Promise(resolve => setTimeout(resolve, RPC_DELAY));
    
    // Get block with transactions
    const block = await web3.eth.getBlock(blockNumber, true);
    
    if (block && block.transactions) {
      // Process each transaction
      for (const tx of block.transactions) {
        // Add from address
        if (tx.from) {
          addresses.add(tx.from.toLowerCase());
        }
        
        // Add to address (if not contract creation)
        if (tx.to) {
          addresses.add(tx.to.toLowerCase());
        }
        
        // If contract creation (to is null), get contract address from receipt
        if (!tx.to && tx.hash) {
          try {
            const receipt = await web3.eth.getTransactionReceipt(tx.hash);
            if (receipt && receipt.contractAddress) {
              addresses.add(receipt.contractAddress.toLowerCase());
            }
          } catch (err) {
            // Skip if can't get receipt
          }
        }
        
        stats.transactionsProcessed++;
      }
      
      // Also check for any internal transactions by getting receipts
      // This catches addresses that received REACT through internal transactions
      for (const tx of block.transactions) {
        try {
          const receipt = await web3.eth.getTransactionReceipt(tx.hash);
          if (receipt && receipt.logs) {
            for (const log of receipt.logs) {
              // Add any address found in logs
              if (log.address) {
                addresses.add(log.address.toLowerCase());
              }
              
              // Try to decode addresses from topics (often contain addresses)
              for (const topic of log.topics) {
                if (topic && topic.length === 66) { // Address topics are 32 bytes (64 hex + 0x)
                  try {
                    // Extract potential address (last 40 hex chars)
                    const potentialAddress = '0x' + topic.slice(-40);
                    if (web3.utils.isAddress(potentialAddress)) {
                      addresses.add(potentialAddress.toLowerCase());
                    }
                  } catch (err) {
                    // Not an address, skip
                  }
                }
              }
            }
          }
        } catch (err) {
          // Skip if can't get receipt
        }
      }
    }
    
    // Also get the miner/validator address
    if (block && block.miner) {
      addresses.add(block.miner.toLowerCase());
    }
    
  } catch (error) {
    console.error(`Error processing block ${blockNumber}:`, error.message);
    stats.errors++;
  }
  
  return addresses;
}

// Check balances for a batch of addresses
async function checkBalances(addresses, blockNumber = 'latest') {
  const holders = [];
  
  for (const address of addresses) {
    try {
      // Skip system addresses
      if (address === '0x0000000000000000000000000000000000000000' ||
          address === '0x000000000000000000000000000000000000dead') {
        continue;
      }
      
      // Add delay to avoid rate limiting
      await new Promise(resolve => setTimeout(resolve, 50));
      
      // Get native REACT balance
      const balanceWei = await web3.eth.getBalance(address, blockNumber);
      const balance = web3.utils.fromWei(balanceWei, 'ether');
      const balanceNumeric = parseFloat(balance);
      
      // Only track addresses with meaningful balance
      if (balanceNumeric >= MIN_BALANCE) {
        // Check if address is a contract
        const code = await web3.eth.getCode(address);
        const isContract = code !== '0x' && code !== '0x0';
        
        holders.push({
          address,
          balance,
          balanceNumeric,
          isContract,
          blockNumber: typeof blockNumber === 'number' ? blockNumber : await web3.eth.getBlockNumber()
        });
        
        stats.totalSupply += balanceNumeric;
      }
    } catch (error) {
      console.error(`Error checking balance for ${address}:`, error.message);
      stats.errors++;
    }
  }
  
  return holders;
}

// Save holders to database
async function saveHolders(holders) {
  if (holders.length === 0) return;
  
  return new Promise((resolve, reject) => {
    db.serialize(() => {
      db.run('BEGIN TRANSACTION');
      
      const stmt = db.prepare(`
        INSERT OR REPLACE INTO holders (
          address, balance, balance_numeric, is_contract,
          first_seen_block, first_seen_timestamp,
          last_updated_block, last_updated_timestamp,
          tx_count, updated_at
        ) VALUES (
          ?, ?, ?, ?,
          COALESCE((SELECT first_seen_block FROM holders WHERE address = ?), ?),
          COALESCE((SELECT first_seen_timestamp FROM holders WHERE address = ?), ?),
          ?, ?,
          (SELECT COUNT(*) FROM burns WHERE from_address = ?),
          CURRENT_TIMESTAMP
        )
      `);
      
      const timestamp = Math.floor(Date.now() / 1000);
      
      for (const holder of holders) {
        stmt.run(
          holder.address,
          holder.balance,
          holder.balanceNumeric,
          holder.isContract ? 1 : 0,
          holder.address, holder.blockNumber,
          holder.address, timestamp,
          holder.blockNumber,
          timestamp,
          holder.address
        );
      }
      
      stmt.finalize();
      
      db.run('COMMIT', (err) => {
        if (err) {
          db.run('ROLLBACK');
          reject(err);
        } else {
          stats.holdersFound += holders.length;
          resolve();
        }
      });
    });
  });
}

// Main scanning function
async function scanAllHolders() {
  console.log('=== Reactive Network Native Token Holder Scan ===\n');
  console.log('This will scan the ENTIRE blockchain to find ALL REACT holders.');
  console.log('This process will take several hours depending on chain size.\n');
  
  try {
    // Get current block number
    const currentBlock = await web3.eth.getBlockNumber();
    console.log(`Current block height: ${currentBlock.toLocaleString()}\n`);
    
    // Get checkpoint (where we left off)
    const checkpoint = await getCheckpoint();
    const startBlock = checkpoint > 0 ? checkpoint : 0;
    
    if (startBlock > 0) {
      console.log(`Resuming from checkpoint: Block ${startBlock}\n`);
    } else {
      console.log('Starting fresh scan from genesis block\n');
    }
    
    // Process in batches
    let processedBlocks = startBlock;
    
    while (processedBlocks <= currentBlock) {
      const batchEnd = Math.min(processedBlocks + BATCH_SIZE, currentBlock);
      const batchAddresses = new Set();
      
      console.log(`Processing blocks ${processedBlocks} to ${batchEnd}...`);
      
      // Extract addresses from batch of blocks
      for (let blockNum = processedBlocks; blockNum <= batchEnd; blockNum++) {
        const blockAddresses = await extractAddressesFromBlock(blockNum);
        blockAddresses.forEach(addr => {
          batchAddresses.add(addr);
          stats.uniqueAddresses.add(addr);
        });
        
        stats.blocksProcessed++;
        
        // Show progress every 100 blocks
        if (stats.blocksProcessed % 100 === 0) {
          const progress = (processedBlocks / currentBlock * 100).toFixed(2);
          const elapsed = (Date.now() - stats.startTime) / 1000;
          const blocksPerSec = stats.blocksProcessed / elapsed;
          const eta = (currentBlock - processedBlocks) / blocksPerSec;
          
          console.log(`Progress: ${progress}% | Blocks/sec: ${blocksPerSec.toFixed(1)} | ETA: ${(eta/3600).toFixed(1)}h | Unique addresses: ${stats.uniqueAddresses.size}`);
        }
      }
      
      // Check balances for new addresses found in this batch
      if (batchAddresses.size > 0) {
        console.log(`Checking balances for ${batchAddresses.size} addresses...`);
        
        const addressArray = Array.from(batchAddresses);
        
        // Process addresses in smaller chunks for balance checking
        for (let i = 0; i < addressArray.length; i += BALANCE_CHECK_BATCH) {
          const chunk = addressArray.slice(i, i + BALANCE_CHECK_BATCH);
          const holders = await checkBalances(chunk);
          
          if (holders.length > 0) {
            await saveHolders(holders);
            console.log(`Found ${holders.length} holders in this chunk`);
          }
        }
      }
      
      // Update checkpoint
      processedBlocks = batchEnd + 1;
      if (processedBlocks % CHECKPOINT_INTERVAL === 0 || processedBlocks >= currentBlock) {
        await saveCheckpoint(batchEnd);
        stats.lastCheckpoint = batchEnd;
        console.log(`Checkpoint saved at block ${batchEnd}`);
      }
    }
    
    // Final pass: Check ALL unique addresses for current balance
    console.log('\n=== Final Balance Check ===');
    console.log(`Checking current balance for all ${stats.uniqueAddresses.size} unique addresses...`);
    
    const allAddresses = Array.from(stats.uniqueAddresses);
    let finalHolders = 0;
    
    for (let i = 0; i < allAddresses.length; i += BALANCE_CHECK_BATCH) {
      const chunk = allAddresses.slice(i, i + BALANCE_CHECK_BATCH);
      const holders = await checkBalances(chunk, 'latest');
      
      if (holders.length > 0) {
        await saveHolders(holders);
        finalHolders += holders.length;
      }
      
      // Progress update
      if (i % 500 === 0) {
        const progress = (i / allAddresses.length * 100).toFixed(1);
        console.log(`Balance check progress: ${progress}% | Holders found: ${finalHolders}`);
      }
    }
    
    // Save final statistics
    await new Promise((resolve) => {
      db.run(
        'INSERT OR REPLACE INTO metadata (key, value) VALUES (?, ?)',
        ['last_full_holder_scan', Math.floor(Date.now() / 1000).toString()],
        resolve
      );
    });
    
    // Get final holder count from database
    const holderCount = await new Promise((resolve) => {
      db.get(
        'SELECT COUNT(*) as count FROM holders WHERE balance_numeric >= ?',
        [MIN_BALANCE],
        (err, row) => resolve(row ? row.count : 0)
      );
    });
    
    // Print final statistics
    const elapsed = (Date.now() - stats.startTime) / 1000;
    console.log('\n=== Scan Complete! ===');
    console.log(`Total time: ${(elapsed/3600).toFixed(2)} hours`);
    console.log(`Blocks processed: ${stats.blocksProcessed.toLocaleString()}`);
    console.log(`Transactions processed: ${stats.transactionsProcessed.toLocaleString()}`);
    console.log(`Unique addresses found: ${stats.uniqueAddresses.size.toLocaleString()}`);
    console.log(`Token holders (balance ≥ ${MIN_BALANCE}): ${holderCount.toLocaleString()}`);
    console.log(`Total supply tracked: ${stats.totalSupply.toFixed(2)} REACT`);
    console.log(`Errors encountered: ${stats.errors}`);
    
    // Get distribution stats
    const distribution = await new Promise((resolve) => {
      db.get(`
        SELECT 
          COUNT(CASE WHEN balance_numeric >= 1 THEN 1 END) as above_1,
          COUNT(CASE WHEN balance_numeric >= 100 THEN 1 END) as above_100,
          COUNT(CASE WHEN balance_numeric >= 1000 THEN 1 END) as above_1000,
          COUNT(CASE WHEN balance_numeric >= 10000 THEN 1 END) as above_10000
        FROM holders
        WHERE balance_numeric >= ${MIN_BALANCE}
      `, (err, row) => resolve(row));
    });
    
    console.log('\n=== Holder Distribution ===');
    console.log(`Holders with ≥ 1 REACT: ${distribution.above_1.toLocaleString()}`);
    console.log(`Holders with ≥ 100 REACT: ${distribution.above_100.toLocaleString()}`);
    console.log(`Holders with ≥ 1,000 REACT: ${distribution.above_1000.toLocaleString()}`);
    console.log(`Holders with ≥ 10,000 REACT: ${distribution.above_10000.toLocaleString()}`);
    
    console.log('\n✅ All holder data has been updated!');
    console.log('You can now view holder analytics at: /holders.html');
    
  } catch (error) {
    console.error('Fatal error during scan:', error);
    process.exit(1);
  } finally {
    db.close();
  }
}

// Handle graceful shutdown
process.on('SIGINT', async () => {
  console.log('\n\nScan interrupted. Saving progress...');
  
  if (stats.lastCheckpoint < stats.blocksProcessed) {
    await saveCheckpoint(stats.blocksProcessed);
    console.log(`Progress saved at block ${stats.blocksProcessed}`);
  }
  
  db.close();
  process.exit(0);
});

// Command line options
const args = process.argv.slice(2);
if (args.includes('--help')) {
  console.log('Usage: node scan-all-native-holders.js [options]');
  console.log('Options:');
  console.log('  --reset    Start fresh scan from block 0');
  console.log('  --help     Show this help message');
  process.exit(0);
}

if (args.includes('--reset')) {
  console.log('Resetting scan checkpoint...');
  db.run('DELETE FROM metadata WHERE key = ?', ['native_holder_scan_checkpoint'], () => {
    console.log('Checkpoint reset. Starting fresh scan.\n');
    scanAllHolders();
  });
} else {
  scanAllHolders();
}
