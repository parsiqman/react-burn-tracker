// scripts/scan-all-holders.js - Comprehensive REACT token holder scan
const { Web3 } = require('web3');
const sqlite3 = require('sqlite3').verbose();
const path = require('path');
require('dotenv').config();

const HolderTracker = require('./holder-tracker');

// Database path
const dbPath = process.env.NODE_ENV === 'production' 
  ? '/app/data/burns.db'
  : './burns.db';

const db = new sqlite3.Database(dbPath);

// Initialize Web3
const web3 = new Web3(process.env.RPC_URL || 'https://rpc.reactive.network');

console.log('=== REACT Token Holder Comprehensive Scan ===\n');
console.log('This will scan ALL Transfer events to find every REACT token holder.');
console.log('This process may take 30-60 minutes depending on network activity.\n');

// Main scan function
async function scanAllHolders() {
  try {
    // Initialize tracker
    console.log('Initializing holder tracker...');
    const tracker = new HolderTracker(db, web3);
    await tracker.initialize();
    console.log('✓ Tracker initialized\n');

    // Get current block
    const currentBlock = await web3.eth.getBlockNumber();
    console.log(`Current block: ${currentBlock}\n`);

    // Determine scan range
    let startBlock = 0;
    
    // Check if we have a previous scan checkpoint
    const lastFullScan = await new Promise((resolve) => {
      db.get('SELECT value FROM metadata WHERE key = ?', ['last_full_holder_scan_block'], (err, row) => {
        resolve(row ? parseInt(row.value) : 0);
      });
    });

    if (lastFullScan > 0) {
      console.log(`Previous full scan found at block ${lastFullScan}`);
      const choice = process.argv[2];
      
      if (choice === '--continue') {
        startBlock = lastFullScan;
        console.log(`Continuing from block ${startBlock}\n`);
      } else if (choice === '--full') {
        startBlock = 0;
        console.log('Starting full scan from block 0\n');
      } else {
        console.log('\nOptions:');
        console.log('  --continue : Continue from last scan block');
        console.log('  --full     : Start fresh from block 0');
        console.log('  --recent   : Only scan last 100,000 blocks');
        console.log('\nRun with one of these options.');
        process.exit(0);
      }
    } else if (process.argv[2] === '--recent') {
      startBlock = Math.max(0, currentBlock - 100000);
      console.log(`Scanning recent blocks from ${startBlock}\n`);
    }

    // Start the scan
    console.log(`Starting scan from block ${startBlock} to ${currentBlock}`);
    console.log('This will:');
    console.log('1. Find all addresses from Transfer events');
    console.log('2. Query current REACT balance for each address');
    console.log('3. Store holder data in database\n');
    
    const startTime = Date.now();
    
    // Perform the scan
    const holderCount = await tracker.scanForHolders(startBlock, currentBlock);
    
    const elapsed = (Date.now() - startTime) / 1000;
    console.log(`\n✓ Scan completed in ${(elapsed / 60).toFixed(2)} minutes`);
    console.log(`✓ Found ${holderCount} unique addresses\n`);
    
    // Save scan checkpoint
    await new Promise((resolve) => {
      db.run(
        'INSERT OR REPLACE INTO metadata (key, value) VALUES (?, ?)',
        ['last_full_holder_scan_block', currentBlock.toString()],
        resolve
      );
    });
    
    // Save snapshot
    console.log('Saving holder snapshot...');
    await tracker.saveHolderSnapshot(currentBlock);
    console.log('✓ Snapshot saved\n');
    
    // Get final statistics
    const stats = await tracker.getHolderStatistics();
    
    console.log('=== Final Statistics ===');
    console.log(`Total holders with balance: ${stats.totalHolders || 0}`);
    console.log(`Total supply tracked: ${(stats.totalSupply || 0).toFixed(2)} REACT`);
    console.log(`Holders > 1 REACT: ${stats.holdersAbove1 || 0}`);
    console.log(`Holders > 100 REACT: ${stats.holdersAbove100 || 0}`);
    console.log(`Holders > 1,000 REACT: ${stats.holdersAbove1000 || 0}`);
    console.log(`Holders > 10,000 REACT: ${stats.holdersAbove10000 || 0}`);
    
    if (stats.totalSupply > 0) {
      console.log(`\nConcentration Metrics:`);
      console.log(`Top 10 holders control: ${((stats.top10Balance / stats.totalSupply) * 100).toFixed(2)}%`);
      console.log(`Top 50 holders control: ${((stats.top50Balance / stats.totalSupply) * 100).toFixed(2)}%`);
      console.log(`Top 100 holders control: ${((stats.top100Balance / stats.totalSupply) * 100).toFixed(2)}%`);
    }
    
    // Get top 10 holders
    console.log('\n=== Top 10 Holders ===');
    db.all(
      `SELECT address, balance_numeric, is_contract 
       FROM holders 
       ORDER BY balance_numeric DESC 
       LIMIT 10`,
      (err, rows) => {
        if (!err && rows) {
          rows.forEach((row, index) => {
            const type = row.is_contract ? '[Contract]' : '[EOA]';
            console.log(`${index + 1}. ${row.address.substring(0, 10)}... : ${row.balance_numeric.toFixed(2)} REACT ${type}`);
          });
        }
        
        console.log('\n✅ All holder data has been updated!');
        console.log('You can now view complete holder analytics at: /holders.html');
        
        db.close();
      }
    );
    
  } catch (error) {
    console.error('Error during scan:', error);
    db.close();
    process.exit(1);
  }
}

// Handle interruption
process.on('SIGINT', () => {
  console.log('\n\nScan interrupted. Progress has been saved.');
  console.log('Run with --continue to resume from where you left off.');
  db.close();
  process.exit(0);
});

// Run the scan
scanAllHolders();
