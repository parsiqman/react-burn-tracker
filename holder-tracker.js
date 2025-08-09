// holder-tracker.js - Service for tracking REACT token holders
const { Web3 } = require('web3');
const sqlite3 = require('sqlite3').verbose();
require('dotenv').config();

class HolderTracker {
  constructor(db, web3) {
    this.db = db;
    this.web3 = web3;
    this.updateQueue = [];
    this.isProcessing = false;
    this.lastScanBlock = 0;
    this.BATCH_SIZE = 100; // Process 100 addresses at a time
    this.MIN_BALANCE = 0.000001; // Minimum balance to track (1 wei in REACT)
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

  // Scan transactions to find all addresses that have interacted with REACT
  async scanForHolders(fromBlock = 0, toBlock = 'latest') {
    console.log(`Scanning for holders from block ${fromBlock} to ${toBlock}`);
    
    if (toBlock === 'latest') {
      toBlock = await this.web3.eth.getBlockNumber();
    }

    const addresses = new Set();
    
    // Get all unique addresses from burns table
    await new Promise((resolve) => {
      this.db.all('SELECT DISTINCT from_address FROM burns', (err, rows) => {
        if (rows) {
          rows.forEach(row => addresses.add(row.from_address.toLowerCase()));
        }
        resolve();
      });
    });

    // Process blocks in chunks to find all addresses that received/sent REACT
    const CHUNK_SIZE = 1000;
    for (let i = fromBlock; i <= toBlock; i += CHUNK_SIZE) {
      const endBlock = Math.min(i + CHUNK_SIZE - 1, toBlock);
      
      try {
        // Get all transactions in this range
        const blocks = await this.scanBlockRange(i, endBlock);
        
        for (const block of blocks) {
          if (block && block.transactions) {
            for (const tx of block.transactions) {
              if (tx.from) addresses.add(tx.from.toLowerCase());
              if (tx.to) addresses.add(tx.to.toLowerCase());
            }
          }
        }
        
        console.log(`Scanned blocks ${i} to ${endBlock}, found ${addresses.size} unique addresses`);
      } catch (error) {
        console.error(`Error scanning blocks ${i}-${endBlock}:`, error);
      }
    }

    // Update balances for all found addresses
    await this.updateBalancesForAddresses(Array.from(addresses), toBlock);
    
    // Save scan progress
    await this.saveScanProgress(toBlock);
    
    return addresses.size;
  }

  // Scan a range of blocks
  async scanBlockRange(startBlock, endBlock) {
    const blocks = [];
    const batchPromises = [];
    
    for (let i = startBlock; i <= endBlock; i++) {
      batchPromises.push(
        this.web3.eth.getBlock(i, true).catch(err => {
          console.error(`Error fetching block ${i}:`, err.message);
          return null;
        })
      );
      
      // Process in batches of 10
      if (batchPromises.length >= 10 || i === endBlock) {
        const batchResults = await Promise.all(batchPromises);
        blocks.push(...batchResults.filter(b => b !== null));
        batchPromises.length = 0;
        
        // Small delay to avoid rate limiting
        await new Promise(resolve => setTimeout(resolve, 100));
      }
    }
    
    return blocks;
  }

  // Update balances for a list of addresses
  async updateBalancesForAddresses(addresses, blockNumber) {
    console.log(`Updating balances for ${addresses.length} addresses...`);
    
    const timestamp = Math.floor(Date.now() / 1000);
    const updates = [];
    
    // Process addresses in batches
    for (let i = 0; i < addresses.length; i += this.BATCH_SIZE) {
      const batch = addresses.slice(i, i + this.BATCH_SIZE);
      
      const balancePromises = batch.map(async (address) => {
        try {
          // Get balance
          const balanceWei = await this.web3.eth.getBalance(address, blockNumber);
          const balance = this.web3.utils.fromWei(balanceWei, 'ether');
          const balanceNumeric = parseFloat(balance);
          
          // Skip if balance is below minimum threshold
          if (balanceNumeric < this.MIN_BALANCE) {
            return null;
          }
          
          // Check if address is a contract
          const code = await this.web3.eth.getCode(address);
          const isContract = code !== '0x' && code !== '0x0';
          
          // Get transaction count for this address
          const txCount = await this.getTxCountForAddress(address);
          
          return {
            address,
            balance,
            balanceNumeric,
            isContract,
            txCount,
            blockNumber,
            timestamp
          };
        } catch (error) {
          console.error(`Error getting balance for ${address}:`, error.message);
          return null;
        }
      });
      
      const results = await Promise.all(balancePromises);
      updates.push(...results.filter(r => r !== null));
      
      // Small delay between batches
      await new Promise(resolve => setTimeout(resolve, 200));
      
      if (i % 1000 === 0) {
        console.log(`Processed ${i}/${addresses.length} addresses`);
      }
    }
    
    // Batch insert/update in database
    await this.batchUpdateHolders(updates);
    
    return updates.length;
  }

  // Get transaction count for an address from burns table
  async getTxCountForAddress(address) {
    return new Promise((resolve) => {
      this.db.get(
        'SELECT COUNT(*) as count FROM burns WHERE from_address = ?',
        [address],
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
            update.address,
            update.balance,
            update.balanceNumeric,
            update.isContract ? 1 : 0,
            update.txCount,
            update.address, update.blockNumber,
            update.address, update.timestamp,
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
            console.log(`Updated ${updates.length} holder balances`);
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
    await this.updateBalancesForAddresses(uniqueAddresses, 'latest');
  }
}

module.exports = HolderTracker;
