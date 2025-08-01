// scripts/sync-historical-debug.js - Debug version
const { Web3 } = require('web3');
const sqlite3 = require('sqlite3').verbose();
const path = require('path');
const fs = require('fs');
require('dotenv').config();

console.log('=== DEBUG INFO ===');
console.log('NODE_ENV:', process.env.NODE_ENV);
console.log('Current directory:', process.cwd());
console.log('__dirname:', __dirname);

// Configuration
const BATCH_SIZE = 50;
const CONCURRENT_BATCHES = 2;
const PROGRESS_INTERVAL = 10000;
const RATE_LIMIT_DELAY = 100;

// Initialize Web3
const web3 = new Web3(process.env.RPC_URL || 'https://rpc.reactive.network');

// Database path logic
const dbPath = process.env.NODE_ENV === 'production' 
  ? '/app/data/burns.db'
  : './burns.db';

console.log('Database path:', dbPath);
console.log('Database exists:', fs.existsSync(dbPath));

// Check directory
const dbDir = path.dirname(dbPath);
console.log('Database directory:', dbDir);
console.log('Directory exists:', fs.existsSync(dbDir));

if (fs.existsSync(dbDir)) {
  console.log('Directory contents:', fs.readdirSync(dbDir));
}

// Try to connect
console.log('Attempting to connect to database...');

const db = new sqlite3.Database(dbPath, sqlite3.OPEN_READWRITE, (err) => {
  if (err) {
    console.error('Database connection error:', err);
    console.error('Error code:', err.code);
    console.error('Error errno:', err.errno);
    process.exit(1);
  }
  
  console.log('Database connected successfully!');
  
  // Test query
  console.log('Testing metadata query...');
  
  db.get('SELECT value FROM metadata WHERE key = ?', ['deployment_block'], (err, row) => {
    console.log('Deployment block query error:', err);
    console.log('Deployment block query result:', row);
    
    if (!row) {
      console.log('No deployment block found! Checking all metadata...');
      
      db.all('SELECT * FROM metadata', (err2, rows) => {
        console.log('All metadata error:', err2);
        console.log('All metadata rows:', rows);
        process.exit(1);
      });
    } else {
      console.log('Deployment block found:', row.value);
      process.exit(0);
    }
  });
});

// Add timeout to prevent hanging
setTimeout(() => {
  console.error('Debug script timed out after 10 seconds');
  process.exit(1);
}, 10000);
