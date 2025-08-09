// scripts/init-holders.js - Initialize holder tracking
const { Web3 } = require('web3');
const sqlite3 = require('sqlite3').verbose();
const path = require('path');
require('dotenv').config();

const HolderTracker = require('../holder-tracker');

// Database path
const dbPath = process.env.NODE_ENV === 'production' 
  ? '/app/data/burns.db'
  : './burns.db';

const db = new sqlite3.Database(dbPath);

// Initialize Web3
const web3 = new Web3(process.env.RPC_URL || 'https://rpc.reactive.network');

console.log('=== REACT Holder Initialization ===\n');

// Create tables if they don't exist
function initializeTables() {
  return new Promise((resolve, reject) => {
    db.serialize(() => {
      // Create holders table
      db.run(`
        CREATE TABLE IF NOT EXISTS holders (
          address TEXT PRIMARY KEY,
          balance TEXT NOT NULL,
          balance_numeric REAL NOT NULL,
          first_seen_block INTEGER,
          first_seen_timestamp INTEGER,
          last_updated_block INTEGER,
          last_updated_timestamp INTEGER,
          tx_count INTEGER DEFAULT 0,
          is_contract BOOLEAN DEFAULT 0,
          label TEXT,
          created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
          updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
      `);

      // Create holder history table
      db.run(`
        CREATE TABLE IF NOT EXISTS holder_history (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          timestamp INTEGER NOT NULL,
          block_number INTEGER NOT NULL,
          total_holders INTEGER NOT NULL,
          holders_above_1 INTEGER DEFAULT 0,
          holders_above_10 INTEGER DEFAULT 0,
          holders_above_100 INTEGER DEFAULT 0,
          holders_above_1000 INTEGER DEFAULT 0,
          holders_above_10000 INTEGER DEFAULT 0,
          top_10_balance REAL DEFAULT 0,
          top_50_balance REAL DEFAULT 0,
          top_100_balance REAL DEFAULT 0,
          total_supply REAL DEFAULT 0,
          created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
      `);

      // Create known addresses table
      db.run(`
        CREATE TABLE IF NOT EXISTS known_addresses (
          address TEXT PRIMARY KEY,
          label TEXT NOT NULL,
          category TEXT,
          description TEXT,
          created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
      `);

      // Create indexes
      db.run('CREATE INDEX IF NOT EXISTS idx_balance_numeric ON holders(balance_numeric DESC)');
      db.run('CREATE INDEX IF NOT EXISTS idx_tx_count ON holders(tx_count DESC)');
      db.run('CREATE INDEX IF NOT EXISTS idx_last_updated ON holders(last_updated_timestamp)');
      db.run('CREATE INDEX IF NOT EXISTS idx_is_contract ON holders(is_contract)');
      db.run('CREATE INDEX IF NOT EXISTS idx_history_timestamp ON holder_history(timestamp DESC)');
      db.run('CREATE INDEX IF NOT EXISTS idx_history_block ON holder_history(block_number DESC)', (err) => {
        if (err) reject(err);
        else resolve();
      });
    });
  });
}

// Add some known addresses
function addKnownAddresses() {
  const knownAddresses = [
    {
      address: '0x0000000000000000000000000000000000000000',
      label: 'Null Address',
      category: 'system',
      description: 'Burn address'
    },
    {
      address: '0x0000000000000000000000000000000000fffFfF',
      label: 'System Contract',
      category: 'system',
      description: 'Reactive Network system contract'
    },
    {
      address: '0x000000000000000000000000000000000000dEaD',
      label: 'Dead Address',
      category: 'system',
      description: 'Common burn address'
    },
    // Add more known addresses as you discover them
    // You can add exchange addresses, team wallets, etc.
  ];

  const stmt = db.prepare(`
    INSERT OR IGNORE INTO known_addresses (address, label, category, description)
    VALUES (?, ?, ?, ?)
  `);

  knownAddresses.forEach(addr => {
    stmt.run(addr.address.toLowerCase(), addr.label, addr.category, addr.description);
  });

  stmt.finalize();
}

// Main initialization
async function initialize() {
  try {
    console.log('Initializing database tables...');
    await initializeTables();
    console.log('✓ Tables created/verified\n');

    console.log('Adding known addresses...');
    addKnownAddresses();
    console.log('✓ Known addresses added\n');

    console.log('Initializing holder tracker...');
    const tracker = new HolderTracker(db, web3);
    await tracker.initialize();
    console.log('✓ Tracker initialized\n');

    // Get current block
    const currentBlock = await web3.eth.getBlockNumber();
    console.log(`Current block: ${currentBlock}\n`);

    // Check if we should do a full scan or incremental
    const lastScan = await new Promise((resolve) => {
      db.get('SELECT value FROM metadata WHERE key = ?', ['last_holder_scan_block'], (err, row) => {
        resolve(row ? parseInt(row.value) : 0);
      });
    });

    if (lastScan === 0) {
      console.log('No previous scan found. Starting initial holder scan...');
      console.log('This will take some time to scan all historical blocks.\n');
      
      // For initial scan, you might want to start from a more recent block
      // to avoid scanning millions of empty blocks
      const startBlock = Math.max(0, currentBlock - 100000); // Last 100k blocks
      
      console.log(`Scanning from block ${startBlock} to ${currentBlock}...`);
      const holderCount = await tracker.scanForHolders(startBlock, currentBlock);
      
      console.log(`\n✓ Found ${holderCount} unique addresses`);
      
      // Save initial snapshot
      await tracker.saveHolderSnapshot(currentBlock);
      console.log('✓ Initial holder snapshot saved\n');
      
    } else {
      console.log(`Last scan was at block ${lastScan}`);
      console.log(`Scanning new blocks from ${lastScan} to ${currentBlock}...`);
      
      const holderCount = await tracker.scanForHolders(lastScan, currentBlock);
      console.log(`✓ Updated ${holderCount} holder balances\n`);
      
      // Save new snapshot
      await tracker.saveHolderSnapshot(currentBlock);
      console.log('✓ New holder snapshot saved\n');
    }

    // Get final statistics
    const stats = await tracker.getHolderStatistics();
    console.log('=== Final Statistics ===');
    console.log(`Total holders: ${stats.totalHolders}`);
    console.log(`Total supply tracked: ${stats.totalSupply?.toFixed(2)} REACT`);
    console.log(`Holders > 1 REACT: ${stats.holdersAbove1}`);
    console.log(`Holders > 100 REACT: ${stats.holdersAbove100}`);
    console.log(`Holders > 10,000 REACT: ${stats.holdersAbove10000}`);
    console.log(`Top 10 balance: ${stats.top10Balance?.toFixed(2)} REACT`);
    
    const top10Percent = stats.totalSupply > 0 
      ? (stats.top10Balance / stats.totalSupply * 100).toFixed(2)
      : 0;
    console.log(`Top 10 concentration: ${top10Percent}%`);

    console.log('\n✅ Holder initialization complete!');
    console.log('\nYou can now view holder analytics at: /holders.html');

  } catch (error) {
    console.error('Error during initialization:', error);
  } finally {
    db.close();
  }
}

// Run initialization
initialize();
