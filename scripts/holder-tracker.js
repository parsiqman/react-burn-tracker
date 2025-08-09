// holder-tracker.js - Service for tracking REACT token holders
const { Web3 } = require('web3');
const sqlite3 = require('sqlite3').verbose();
require('dotenv').config();

// REACT Token ABI - minimal ABI for balance and Transfer events
const REACT_TOKEN_ABI = [
  {
    "constant": true,
    "inputs": [{"name": "_owner", "type": "address"}],
    "name": "balanceOf",
    "outputs": [{"name": "balance", "type": "uint256"}],
    "type": "function"
  },
  {
    "anonymous": false,
    "inputs": [
      {"indexed": true, "name": "from", "type": "address"},
      {"indexed": true, "name": "to", "type": "address"},
      {"indexed": false, "name": "value", "type": "uint256"}
    ],
    "name": "Transfer",
    "type": "event"
  }
];

class HolderTracker {
  constructor(db, web3) {
    this.db = db;
    this.web3 = web3;
    this.updateQueue = [];
    this.isProcessing = false;
    this.lastScanBlock = 0;
    this.BATCH_SIZE = 50; // Process 50 addresses at a time
    this.MIN_BALANCE = 0.000001; // Minimum balance to track
    this.REACT_TOKEN_ADDRESS = process.env.REACT_TOKEN_ADDRESS || '0x0000000000000000000000000000000000fffFfF';
    
    // Initialize REACT token contract
    this.reactToken = new web3.eth.Contract(REACT_TOKEN_ABI, this.REACT_TOKEN_ADDRESS);
  }

  // Initialize the holder tracking
  async initialize() {
    // Get last scanned block from metadata
    return new Promise((resolve) => {
      this.db.get('SELECT value FROM metadata WHERE key = ?', ['last_holder_scan_block'], (err, row) => {
        if (row) {
          this.lastScanBlock = parseInt(row.value);
        }
        resolve();
      });
    });
  }

  // Scan for all REACT token holders by analyzing Transfer events
  async scanForHolders(fromBlock = 0, toBlock = 'latest') {
    console.log(`Scanning for REACT holders from block ${fromBlock} to ${toBlock}`);
    
    if (toBlock === 'latest') {
      toBlock = await this.web3.eth.getBlockNumber();
    }

    const addresses = new Set();
    
    // First, get addresses from existing burns table
    await new Promise((resolve) => {
      this.db.all('SELECT DISTINCT from_address FROM burns', (err, rows) => {
        if (rows) {
          rows.forEach(row => addresses.add(row.from_address.toLowerCase()));
        }
        resolve();
      });
    });

    console.log(`Found ${addresses.size} addresses from burns table`);

    // Now scan Transfer events to find all addresses that have ever held REACT
    try {
      const CHUNK_SIZE = 5000; // Process events in chunks
      
      for (let startBlock = fromBlock; startBlock <= toBlock; startBlock += CHUNK_SIZE) {
        const endBlock = Math.min(startBlock + CHUNK_SIZE - 1, toBlock);
        
        console.log(`Scanning Transfer events from block ${startBlock} to ${endBlock}`);
        
        try {
          // Get all Transfer events in this range
          const events = await this.reactToken.getPastEvents('Transfer', {
            fromBlock: startBlock,
            toBlock: endBlock
          });
          
          console.log(`Found ${events.length} Transfer events in blocks ${startBlock}-${endBlock}`);
          
          // Add all addresses involved in transfers
          events.forEach(event => {
            if (event.returnValues.from) {
              addresses.add(event.returnValues.from.toLowerCase());
            }
            if (event.returnValues.to) {
              addresses.add(event.returnValues.to.toLowerCase());
            }
          });
          
        } catch (error) {
          console.error(`Error scanning blocks ${startBlock}-${endBlock}:`, error.message);
          
          // If chunk is too large, try smaller chunks
          if (CHUNK_SIZE > 1000) {
            const SMALLER_CHUNK = 1000;
            for (let smallStart = startBlock; smallStart <= endBlock; smallStart += SMALLER_CHUNK) {
              const smallEnd = Math.min(smallStart + SMALLER_CHUNK - 1, endBlock);
              try {
                const events = await this.reactToken.getPastEvents('Transfer', {
                  fromBlock: smallStart,
                  toBlock: smallEnd
                });
                
                events.forEach(event => {
                  if (event.returnValues.from) {
                    addresses.add(event.returnValues.from.toLowerCase());
                  }
                  if (event.returnValues.to) {
                    addresses.add(event.returnValues.to.toLowerCase());
                  }
                });
              } catch (smallError) {
                console.error(`Error in smaller chunk ${smallStart}-${smallEnd}:`, smallError.message);
              }
            }
          }
        }
        
        // Progress update
        console.log(`Total unique addresses found so far: ${addresses.size}`);
        
        // Small delay to avoid rate limiting
        await new Promise(resolve => setTimeout(resolve, 200));
      }
    } catch (error) {
      console.error('Error scanning for Transfer events:', error);
    }

    console.log(`Found ${addresses.size} total unique addresses to check`);

    // Filter out zero address and dead addresses
    addresses.delete('0x0000000000000000000000000000000000000000');
    addresses.delete('0x000000000000000000000000000000000000dead');

    // Update balances for all found addresses
    await this.updateTokenBalancesForAddresses(Array.from(addresses), toBlock);
    
    // Save scan progress
    await this.saveScanProgress(toBlock);
    
    return addresses.size;
  }

  // Update REACT token balances for a list of addresses
  async updateTokenBalancesForAddresses(addresses, blockNumber) {
    console.log(`Updating REACT token balances for ${addresses.length} addresses...`);
    
    const timestamp = Math.floor(Date.now() / 1000);
    const updates = [];
    let processedCount = 0;
    
    // Process addresses in batches
    for (let i = 0; i < addresses.length; i += this.BATCH_SIZE) {
      const batch = addresses.slice(i, i + this.BATCH_SIZE);
      
      const balancePromises = batch.map(async (address) => {
        try {
          // Get REACT token balance (not ETH balance!)
          const balanceWei = await this.reactToken.methods.balanceOf(address).call(
            {},
            blockNumber === 'latest' ? undefined : blockNumber
          );
          
          const balance = this.web3.utils.fromWei(balanceWei, 'ether');
          const balanceNumeric = parseFloat(balance);
          
          // Skip if balance is below minimum threshold
          if (balanceNumeric < this.MIN_BALANCE) {
            return null;
          }
          
          // Check if address is a contract
          const code = await this.web3.eth.getCode(address);
          const isContract = code !== '0x' && code !== '0x0';
          
          // Get transaction count for this address from burns table
          const txCount = await this.getTxCountForAddress(address);
          
          processedCount++;
          
          return {
            address,
            balance,
            balanceNumeric,
            isContract,
            txCount,
            blockNumber: typeof blockNumber === 'number' ? blockNumber : await this.web3.eth.getBlockNumber(),
            timestamp
          };
        } catch (error) {
          console.error(`Error getting REACT balance for ${address}:`, error.message);
          return null;
        }
      });
      
      const results = await Promise.all(balancePromises);
      const validResults = results.filter(r => r !== null);
      updates.push(...validResults);
      
      // Progress update
      if (processedCount % 500 === 0 || processedCount === addresses.length) {
        console.log(`Processed ${processedCount}/${addresses.length} addresses, found ${updates.length} with balance > ${this.MIN_BALANCE}`);
      }
      
      // Small delay between batches to avoid rate limiting
      await new Promise(resolve => setTimeout(resolve, 100));
    }
    
    console.log(`Found ${updates.length} addresses with REACT balance > ${this.MIN_BALANCE}`);
    
    // Batch insert/update in database
    if (updates.length > 0) {
      await this.batchUpdateHolders(updates);
    }
    
    return updates.length;
  }

  // Get transaction count for an address from burns table
  async getTxCountForAddress(address) {
    return new Promise((resolve) => {
      this.db.get(
        'SELECT COUNT(*) as count FROM burns WHERE from_address = ?',
        [address.toLowerCase()],
        (err, row) => {
          resolve(row ? row.count : 0);
        }
      );
    });
  }

  // Batch update holders in database
  async batchUpdateHolders(updates) {
    if (updates.length === 0) return;
    
    return new Promise((resolve, reject) => {
      this.db.serialize(() => {
        this.db.run('BEGIN TRANSACTION');
        
        const stmt = this.db.prepare(`
          INSERT OR REPLACE INTO holders (
            address, balance, balance_numeric, is_contract, tx_count,
            first_seen_block, first_seen_timestamp,
            last_updated_block, last_updated_timestamp, updated_at
          ) VALUES (
            ?, ?, ?, ?, ?,
            COALESCE((SELECT first_seen_block FROM holders WHERE address = ?), ?),
            COALESCE((SELECT first_seen_timestamp FROM holders WHERE address = ?), ?),
            ?, ?, CURRENT_TIMESTAMP
          )
        `);
        
        for (const update of updates) {
          stmt.run(
            update.address.toLowerCase(),
            update.balance,
            update.balanceNumeric,
            update.isContract ? 1 : 0,
            update.txCount,
            update.address.toLowerCase(), update.blockNumber,
            update.address.toLowerCase(), update.timestamp,
            update.blockNumber,
            update.timestamp
          );
        }
        
        stmt.finalize();
        
        this.db.run('COMMIT', (err) => {
          if (err) {
            this.db.run('ROLLBACK');
            reject(err);
          } else {
            console.log(`Updated ${updates.length} holder balances in database`);
            resolve();
          }
        });
      });
    });
  }

  // Save holder statistics snapshot
  async saveHolderSnapshot(blockNumber) {
    const timestamp = Math.floor(Date.now() / 1000);
    
    // Get holder statistics
    const stats = await this.getHolderStatistics();
    
    return new Promise((resolve, reject) => {
      this.db.run(`
        INSERT INTO holder_history (
          timestamp, block_number, total_holders,
          holders_above_1, holders_above_10, holders_above_100,
          holders_above_1000, holders_above_10000,
          top_10_balance, top_50_balance, top_100_balance, total_supply
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `, [
        timestamp,
        blockNumber,
        stats.totalHolders,
        stats.holdersAbove1,
        stats.holdersAbove10,
        stats.holdersAbove100,
        stats.holdersAbove1000,
        stats.holdersAbove10000,
        stats.top10Balance,
        stats.top50Balance,
        stats.top100Balance,
        stats.totalSupply
      ], (err) => {
        if (err) reject(err);
        else resolve();
      });
    });
  }

  // Get holder statistics
  async getHolderStatistics() {
    return new Promise((resolve) => {
      this.db.all(`
        SELECT 
          COUNT(*) as totalHolders,
          SUM(balance_numeric) as totalSupply,
          SUM(CASE WHEN balance_numeric > 1 THEN 1 ELSE 0 END) as holdersAbove1,
          SUM(CASE WHEN balance_numeric > 10 THEN 1 ELSE 0 END) as holdersAbove10,
          SUM(CASE WHEN balance_numeric > 100 THEN 1 ELSE 0 END) as holdersAbove100,
          SUM(CASE WHEN balance_numeric > 1000 THEN 1 ELSE 0 END) as holdersAbove1000,
          SUM(CASE WHEN balance_numeric > 10000 THEN 1 ELSE 0 END) as holdersAbove10000
        FROM holders
        WHERE balance_numeric > 0
      `, async (err, rows) => {
        if (err || !rows || rows.length === 0) {
          resolve({});
          return;
        }
        
        const baseStats = rows[0];
        
        // Get top holder balances
        this.db.all(
          'SELECT balance_numeric FROM holders ORDER BY balance_numeric DESC LIMIT 100',
          (err, topHolders) => {
            if (err || !topHolders) {
              resolve(baseStats);
              return;
            }
            
            let top10Balance = 0, top50Balance = 0, top100Balance = 0;
            
            topHolders.forEach((holder, index) => {
              if (index < 10) top10Balance += holder.balance_numeric;
              if (index < 50) top50Balance += holder.balance_numeric;
              if (index < 100) top100Balance += holder.balance_numeric;
            });
            
            resolve({
              ...baseStats,
              top10Balance,
              top50Balance,
              top100Balance
            });
          }
        );
      });
    });
  }

  // Save scan progress
  async saveScanProgress(blockNumber) {
    return new Promise((resolve) => {
      this.db.run(
        'INSERT OR REPLACE INTO metadata (key, value) VALUES (?, ?)',
        ['last_holder_scan_block', blockNumber.toString()],
        resolve
      );
    });
  }

  // Quick update for known active addresses (called after each new block)
  async quickUpdateActiveAddresses(addresses) {
    if (addresses.length === 0) return;
    
    const uniqueAddresses = [...new Set(addresses.map(a => a.toLowerCase()))];
    await this.updateTokenBalancesForAddresses(uniqueAddresses, 'latest');
  }

  // Scan recent blocks for new holders (more efficient for live updates)
  async scanRecentBlocks(fromBlock, toBlock) {
    console.log(`Scanning recent blocks ${fromBlock} to ${toBlock} for new REACT holders`);
    
    try {
      const events = await this.reactToken.getPastEvents('Transfer', {
        fromBlock: fromBlock,
        toBlock: toBlock
      });
      
      const addresses = new Set();
      events.forEach(event => {
        if (event.returnValues.from) {
          addresses.add(event.returnValues.from.toLowerCase());
        }
        if (event.returnValues.to) {
          addresses.add(event.returnValues.to.toLowerCase());
        }
      });
      
      // Remove zero and dead addresses
      addresses.delete('0x0000000000000000000000000000000000000000');
      addresses.delete('0x000000000000000000000000000000000000dead');
      
      if (addresses.size > 0) {
        console.log(`Found ${addresses.size} addresses in recent blocks to update`);
        await this.updateTokenBalancesForAddresses(Array.from(addresses), 'latest');
      }
      
      return addresses.size;
    } catch (error) {
      console.error('Error scanning recent blocks:', error);
      return 0;
    }
  }
}

module.exports = HolderTracker;
