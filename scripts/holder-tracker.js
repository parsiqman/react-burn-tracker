// holder-tracker.js - Service for tracking REACT native token holders
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
    this.BATCH_SIZE = 50;
    this.MIN_BALANCE = 0.000001;
    // Note: REACT is the native token, not an ERC20
    this.REACT_TOKEN_ADDRESS = process.env.REACT_TOKEN_ADDRESS || '0x0000000000000000000000000000000000fffFfF';
  }

  async initialize() {
    return new Promise((resolve) => {
      this.db.get('SELECT value FROM metadata WHERE key = ?', ['last_holder_scan_block'], (err, row) => {
        if (row) {
          this.lastScanBlock = parseInt(row.value);
        }
        resolve();
      });
    });
  }

  async scanForHolders(fromBlock = 0, toBlock = 'latest') {
    console.log(`Scanning for REACT holders from block ${fromBlock} to ${toBlock}`);
    
    if (toBlock === 'latest') {
      toBlock = await this.web3.eth.getBlockNumber();
    }

    const addresses = new Set();
    
    // Get addresses from burns table
    await new Promise((resolve) => {
      this.db.all('SELECT DISTINCT from_address FROM burns', (err, rows) => {
        if (rows) {
          rows.forEach(row => addresses.add(row.from_address.toLowerCase()));
        }
        resolve();
      });
    });

    console.log(`Found ${addresses.size} addresses from burns table`);

    // Also scan recent blocks for all transaction addresses
    if (toBlock - fromBlock < 10000) {
      // Only scan if range is reasonable
      try {
        for (let blockNum = fromBlock; blockNum <= toBlock; blockNum += 100) {
          const endBlock = Math.min(blockNum + 99, toBlock);
          
          for (let i = blockNum; i <= endBlock; i++) {
            try {
              const block = await this.web3.eth.getBlock(i, true);
              if (block && block.transactions) {
                block.transactions.forEach(tx => {
                  if (tx.from) addresses.add(tx.from.toLowerCase());
                  if (tx.to) addresses.add(tx.to.toLowerCase());
                });
              }
            } catch (err) {
              // Skip blocks that fail
            }
          }
        }
      } catch (error) {
        console.log('Error scanning blocks for addresses:', error.message);
      }
    }

    // Filter out zero and system addresses
    addresses.delete('0x0000000000000000000000000000000000000000');
    addresses.delete('0x000000000000000000000000000000000000dead');

    await this.updateNativeBalancesForAddresses(Array.from(addresses), toBlock);
    await this.saveScanProgress(toBlock);
    
    return addresses.size;
  }

  async updateNativeBalancesForAddresses(addresses, blockNumber) {
    console.log(`Updating REACT native balances for ${addresses.length} addresses...`);
    
    const timestamp = Math.floor(Date.now() / 1000);
    const updates = [];
    let processedCount = 0;
    
    for (let i = 0; i < addresses.length; i += this.BATCH_SIZE) {
      const batch = addresses.slice(i, i + this.BATCH_SIZE);
      
      const balancePromises = batch.map(async (address) => {
        try {
          // Get native REACT balance (since REACT is the native token)
          const balanceWei = await this.web3.eth.getBalance(
            address, 
            blockNumber === 'latest' ? undefined : blockNumber
          );
          
          const balance = this.web3.utils.fromWei(balanceWei, 'ether');
          const balanceNumeric = parseFloat(balance);
          
          if (balanceNumeric < this.MIN_BALANCE) {
            return null;
          }
          
          // Check if address is a contract
          const code = await this.web3.eth.getCode(address);
          const isContract = code !== '0x' && code !== '0x0';
          
          // Get transaction count from burns table
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
          console.error(`Error getting balance for ${address}:`, error.message);
          return null;
        }
      });
      
      const results = await Promise.all(balancePromises);
      const validResults = results.filter(r => r !== null);
      updates.push(...validResults);
      
      if (processedCount % 500 === 0 || processedCount === addresses.length) {
        console.log(`Processed ${processedCount}/${addresses.length} addresses, found ${updates.length} with balance > ${this.MIN_BALANCE}`);
      }
      
      await new Promise(resolve => setTimeout(resolve, 100));
    }
    
    console.log(`Found ${updates.length} addresses with REACT balance > ${this.MIN_BALANCE}`);
    
    if (updates.length > 0) {
      await this.batchUpdateHolders(updates);
    }
    
    return updates.length;
  }

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
            console.log(`Updated ${updates.length} holder balances`);
            resolve();
          }
        });
      });
    });
  }

  async saveHolderSnapshot(blockNumber) {
    const timestamp = Math.floor(Date.now() / 1000);
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
        stats.totalHolders || 0,
        stats.holdersAbove1 || 0,
        stats.holdersAbove10 || 0,
        stats.holdersAbove100 || 0,
        stats.holdersAbove1000 || 0,
        stats.holdersAbove10000 || 0,
        stats.top10Balance || 0,
        stats.top50Balance || 0,
        stats.top100Balance || 0,
        stats.totalSupply || 0
      ], (err) => {
        if (err) reject(err);
        else resolve();
      });
    });
  }

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

  async saveScanProgress(blockNumber) {
    return new Promise((resolve) => {
      this.db.run(
        'INSERT OR REPLACE INTO metadata (key, value) VALUES (?, ?)',
        ['last_holder_scan_block', blockNumber.toString()],
        resolve
      );
    });
  }

  async quickUpdateActiveAddresses(addresses) {
    if (addresses.length === 0) return;
    
    const uniqueAddresses = [...new Set(addresses.map(a => a.toLowerCase()))];
    await this.updateNativeBalancesForAddresses(uniqueAddresses, 'latest');
  }

  async scanRecentBlocks(fromBlock, toBlock) {
    console.log(`Scanning recent blocks ${fromBlock} to ${toBlock} for activity`);
    
    try {
      const addresses = new Set();
      
      for (let i = fromBlock; i <= toBlock; i++) {
        try {
          const block = await this.web3.eth.getBlock(i, true);
          if (block && block.transactions) {
            block.transactions.forEach(tx => {
              if (tx.from) addresses.add(tx.from.toLowerCase());
              if (tx.to) addresses.add(tx.to.toLowerCase());
            });
          }
        } catch (err) {
          // Skip failed blocks
        }
      }
      
      if (addresses.size > 0) {
        await this.updateNativeBalancesForAddresses(Array.from(addresses), 'latest');
      }
      
      return addresses.size;
    } catch (error) {
      console.error('Error scanning recent blocks:', error);
      return 0;
    }
  }
}

module.exports = HolderTracker;
