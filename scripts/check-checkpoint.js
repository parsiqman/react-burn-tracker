// scripts/check-checkpoint.js - Check and fix checkpoint issues
const sqlite3 = require('sqlite3').verbose();
const path = require('path');
require('dotenv').config();

const dbPath = process.env.NODE_ENV === 'production' 
  ? '/app/data/burns.db'
  : './burns.db';

const db = new sqlite3.Database(dbPath);

console.log('=== Checking Checkpoint Status ===\n');
console.log('Database path:', dbPath);

// Check all metadata entries
db.all('SELECT * FROM metadata', (err, rows) => {
  if (err) {
    console.error('Error reading metadata:', err);
    return;
  }
  
  console.log('\nAll metadata entries:');
  rows.forEach(row => {
    console.log(`  ${row.key}: ${row.value}`);
  });
  
  // Look for any sync-related keys
  const syncKeys = rows.filter(row => 
    row.key.includes('sync') || 
    row.key.includes('historical') || 
    row.key.includes('checkpoint')
  );
  
  if (syncKeys.length > 0) {
    console.log('\nSync-related metadata:');
    syncKeys.forEach(row => {
      console.log(`  ${row.key}: ${row.value}`);
    });
  }
  
  // Check current burn statistics
  db.get(
    `SELECT 
      COUNT(*) as total_burns,
      MIN(block_number) as min_block,
      MAX(block_number) as max_block,
      COUNT(DISTINCT from_address) as unique_addresses
     FROM burns`,
    (err, stats) => {
      if (err) {
        console.error('Error reading burn stats:', err);
        return;
      }
      
      console.log('\nCurrent database statistics:');
      console.log(`  Total burns: ${stats.total_burns}`);
      console.log(`  Block range: ${stats.min_block} to ${stats.max_block}`);
      console.log(`  Unique addresses: ${stats.unique_addresses}`);
      
      // Find the most likely checkpoint value
      let checkpointBlock = 0;
      
      // Check for different possible checkpoint keys
      const possibleKeys = [
        'historical_sync_block',
        'historical_sync_checkpoint', 
        'sync_checkpoint',
        'last_sync_block'
      ];
      
      possibleKeys.forEach(key => {
        const row = rows.find(r => r.key === key);
        if (row && parseInt(row.value) > checkpointBlock) {
          checkpointBlock = parseInt(row.value);
          console.log(`\nFound checkpoint under key "${key}": ${checkpointBlock}`);
        }
      });
      
      // If no checkpoint found but we have data, use max block
      if (checkpointBlock === 0 && stats.max_block > 0) {
        checkpointBlock = stats.max_block;
        console.log(`\nNo checkpoint found, but database has data up to block ${checkpointBlock}`);
      }
      
      if (checkpointBlock > 0) {
        console.log('\n🔧 Fixing checkpoint...');
        
        // Set the checkpoint to the correct key that our new sync script expects
        db.run(
          'INSERT OR REPLACE INTO metadata (key, value) VALUES (?, ?)',
          ['historical_sync_checkpoint', checkpointBlock.toString()],
          (err) => {
            if (err) {
              console.error('Error setting checkpoint:', err);
            } else {
              console.log(`✅ Checkpoint set to block ${checkpointBlock}`);
              console.log('\nYour sync should now resume from this block!');
            }
            
            db.close();
          }
        );
      } else {
        console.log('\n⚠️  No existing sync progress found.');
        console.log('The sync will start from block 0.');
        db.close();
      }
    }
  );
});
