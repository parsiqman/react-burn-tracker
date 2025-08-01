// scripts/fix-empty-db.js - Remove empty database and restart
const fs = require('fs');
const path = require('path');

const dbPath = '/app/data/burns.db';

console.log('=== Database Fix Script ===');

// Check if database exists
if (fs.existsSync(dbPath)) {
  const stats = fs.statSync(dbPath);
  console.log('Database exists, size:', stats.size, 'bytes');
  
  // If database is suspiciously small (less than 100KB), it's probably empty
  if (stats.size < 100000) {
    console.log('Database appears to be empty, removing it...');
    try {
      fs.unlinkSync(dbPath);
      console.log('Empty database removed successfully');
      console.log('The worker should now see the correct database from the shared disk');
    } catch (err) {
      console.error('Error removing database:', err);
    }
  } else {
    console.log('Database size looks normal, not removing');
  }
} else {
  console.log('No database file found');
}

console.log('\nNow run the historical sync again and it should work!');
process.exit(0);
