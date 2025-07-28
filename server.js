// server.js - Production Backend for REACT Burn Tracker
const express = require('express');
const cors = require('cors');
const { Web3 } = require('web3');
const sqlite3 = require('sqlite3').verbose();
const WebSocket = require('ws');
const path = require('path');
require('dotenv').config();

const app = express();
const PORT = process.env.PORT || 3000;

// Middleware
app.use(cors());
app.use(express.json());
app.use(express.static('public'));

// Initialize Web3
const web3 = new Web3(process.env.RPC_URL || 'https://rpc.reactive.network');

// Initialize SQLite Database - use in-memory for free tier
// Note: Data will be lost on restart. Upgrade to paid tier for persistence.
const db = new sqlite3.Database(':memory:');
console.log('Using in-memory database (data will reset on restart)');

// REACT Token Contract Configuration
const REACT_TOKEN_ADDRESS = process.env.REACT_TOKEN_ADDRESS || '0x0000000000000000000000000000000000fffFfF';
const BURN_ADDRESS = '0x0000000000000000000000000000000000000000';

// ABI for Transfer event (standard ERC20)
const TRANSFER_EVENT_ABI = {
  anonymous: false,
  inputs: [
    { indexed: true, name: 'from', type: 'address' },
    { indexed: true, name: 'to', type: 'address' },
    { indexed: false, name: 'value', type: 'uint256' }
  ],
  name: 'Transfer',
  type: 'event'
};

// Price fetching (you'll need to implement based on your price source)
let currentPrice = 0.0234; // Default price
async function updateTokenPrice() {
  try {
    // TODO: Implement actual price fetching from DEX or price oracle
    // For now, using a placeholder
    // const price = await fetchPriceFromDEX();
    // currentPrice = price;
  } catch (error) {
    console.error('Error updating price:', error);
  }
}

// Update price every 5 minutes
setInterval(updateTokenPrice, 5 * 60 * 1000);

// Create HTTP server
const server = require('http').createServer(app);

// Initialize WebSocket server on the same server
const wss = new WebSocket.Server({ server });

// WebSocket connection handling
wss.on('connection', (ws) => {
  console.log('New WebSocket client connected');
  
  ws.on('close', () => {
    console.log('WebSocket client disconnected');
  });
});

// Broadcast to all connected clients
function broadcast(data) {
  wss.clients.forEach(client => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(JSON.stringify(data));
    }
  });
}

// Initialize tables with proper callbacks
function initializeDatabase(callback) {
  db.serialize(() => {
    // Create burns table
    db.run(`
      CREATE TABLE IF NOT EXISTS burns (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        tx_hash TEXT UNIQUE NOT NULL,
        block_number INTEGER NOT NULL,
        amount TEXT NOT NULL,
        from_address TEXT NOT NULL,
        timestamp INTEGER NOT NULL,
        usd_value REAL,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
      )
    `);

    // Create metadata table
    db.run(`
      CREATE TABLE IF NOT EXISTS metadata (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
      )
    `);

    // Create indexes
    db.run('CREATE INDEX IF NOT EXISTS idx_timestamp ON burns(timestamp)');
    db.run('CREATE INDEX IF NOT EXISTS idx_block ON burns(block_number)', callback);
  });
}

// Function to process burn events
async function processBurnEvent(event) {
  try {
    const { transactionHash, blockNumber, returnValues } = event;
    const { from, value } = returnValues;
    
    // Get block timestamp
    const block = await web3.eth.getBlock(blockNumber);
    const timestamp = block.timestamp;
    
    // Convert value from Wei to token units
    // Check if REACT uses different decimals - adjust if needed
    const decimals = 18; // Adjust this if REACT uses different decimals
    const amount = parseFloat(web3.utils.fromWei(value, 'ether'));
    const usdValue = amount * currentPrice;
    
    console.log(`Burn detected: ${amount} REACT from ${from} in tx ${transactionHash}`);
    
    // Store in database
    db.run(
      `INSERT OR IGNORE INTO burns (tx_hash, block_number, amount, from_address, timestamp, usd_value) 
       VALUES (?, ?, ?, ?, ?, ?)`,
      [transactionHash, blockNumber, amount.toFixed(18), from, timestamp, usdValue],
      (err) => {
        if (err) {
          console.error('Error inserting burn:', err);
          return;
        }
        
        // Broadcast to WebSocket clients
        broadcast({
          type: 'new_burn',
          data: {
            txHash: transactionHash,
            block: blockNumber,
            amount: amount,
            from: from,
            timestamp: timestamp * 1000,
            usdValue: usdValue
          }
        });
      }
    );
  } catch (error) {
    console.error('Error processing burn event:', error);
  }
}

// Subscribe to new blocks and track gas fees
async function subscribeToBlocks() {
  try {
    const subscription = await web3.eth.subscribe('newBlockHeaders');
    
    subscription.on('data', async (blockHeader) => {
      try {
        // Get full block with transactions
        const block = await web3.eth.getBlock(blockHeader.number, true);
        
        if (block && block.transactions && block.transactions.length > 0) {
          console.log(`Processing block ${block.number} with ${block.transactions.length} transactions`);
          
          for (const tx of block.transactions) {
            // Get transaction receipt for actual gas used
            const receipt = await web3.eth.getTransactionReceipt(tx.hash);
            
            if (receipt) {
              // Calculate burned REACT: gasUsed * gasPrice
              const gasUsed = BigInt(receipt.gasUsed);
              const gasPrice = BigInt(tx.gasPrice || '0');
              const burnedWei = gasUsed * gasPrice;
              
              // Convert from Wei to REACT (18 decimals)
              const burnedReact = parseFloat(web3.utils.fromWei(burnedWei.toString(), 'ether'));
              const usdValue = burnedReact * currentPrice;
              
              console.log(`Transaction ${tx.hash}: burned ${burnedReact} REACT`);
              
              // Store in database
              db.run(
                `INSERT OR IGNORE INTO burns (tx_hash, block_number, amount, from_address, timestamp, usd_value) 
                 VALUES (?, ?, ?, ?, ?, ?)`,
                [tx.hash, block.number, burnedReact.toFixed(18), tx.from, block.timestamp, usdValue],
                (err) => {
                  if (err && !err.message.includes('UNIQUE constraint failed')) {
                    console.error('Error inserting burn:', err);
                    return;
                  }
                  
                  // Broadcast to WebSocket clients
                  broadcast({
                    type: 'new_burn',
                    data: {
                      txHash: tx.hash,
                      block: block.number,
                      amount: burnedReact,
                      from: tx.from,
                      timestamp: block.timestamp * 1000,
                      usdValue: usdValue
                    }
                  });
                }
              );
            }
          }
        }
      } catch (error) {
        console.error('Error processing block:', error);
      }
    });
    
    subscription.on('error', (error) => {
      console.error('Block subscription error:', error);
      // Retry subscription after 5 seconds
      setTimeout(subscribeToBlocks, 5000);
    });
    
    console.log('Subscribed to new blocks for fee tracking');
  } catch (error) {
    console.error('Error subscribing to blocks:', error);
    // Retry after 5 seconds
    setTimeout(subscribeToBlocks, 5000);
  }
}

// API Endpoints

// Get deployment info
app.get('/api/deployment-info', (req, res) => {
  db.all('SELECT * FROM metadata WHERE key IN (?, ?)', ['deployment_block', 'deployment_time'], (err, rows) => {
    if (err) {
      return res.status(500).json({ error: err.message });
    }
    
    const metadata = {};
    rows.forEach(row => {
      metadata[row.key] = row.value;
    });
    
    res.json({
      deploymentBlock: metadata.deployment_block ? parseInt(metadata.deployment_block) : null,
      deploymentTime: metadata.deployment_time || null
    });
  });
});

// Get total burns
app.get('/api/stats/total', (req, res) => {
  db.get(
    `SELECT 
      COUNT(*) as total_transactions,
      SUM(CAST(amount AS REAL)) as total_burned,
      SUM(usd_value) as total_usd_value
     FROM burns`,
    (err, row) => {
      if (err) {
        return res.status(500).json({ error: err.message });
      }
      res.json({
        totalTransactions: row.total_transactions || 0,
        totalBurned: row.total_burned || 0,
        totalUsdValue: row.total_usd_value || 0,
        currentPrice: currentPrice
      });
    }
  );
});

// Get 24h stats
app.get('/api/stats/24h', (req, res) => {
  const dayAgo = Math.floor(Date.now() / 1000) - (24 * 60 * 60);
  
  db.get(
    `SELECT 
      COUNT(*) as transactions_24h,
      SUM(CAST(amount AS REAL)) as burned_24h
     FROM burns
     WHERE timestamp > ?`,
    [dayAgo],
    (err, row) => {
      if (err) {
        return res.status(500).json({ error: err.message });
      }
      res.json({
        transactions24h: row.transactions_24h || 0,
        burned24h: row.burned_24h || 0
      });
    }
  );
});

// Get hourly burn rate
app.get('/api/stats/burn-rate', (req, res) => {
  const hourAgo = Math.floor(Date.now() / 1000) - 3600;
  
  db.get(
    `SELECT SUM(CAST(amount AS REAL)) as hourly_burn
     FROM burns
     WHERE timestamp > ?`,
    [hourAgo],
    (err, row) => {
      if (err) {
        return res.status(500).json({ error: err.message });
      }
      res.json({
        burnRate: row.hourly_burn || 0
      });
    }
  );
});

// Get chart data
app.get('/api/chart/:period', (req, res) => {
  const { period } = req.params;
  let startTime, groupBy;
  
  const now = Math.floor(Date.now() / 1000);
  
  switch(period) {
    case '24h':
      startTime = now - (24 * 60 * 60);
      groupBy = '%Y-%m-%d %H:00';
      break;
    case '7d':
      startTime = now - (7 * 24 * 60 * 60);
      groupBy = '%Y-%m-%d';
      break;
    case '30d':
      startTime = now - (30 * 24 * 60 * 60);
      groupBy = '%Y-%m-%d';
      break;
    default:
      startTime = 0;
      groupBy = '%Y-%m-%d';
  }
  
  db.all(
    `SELECT 
      strftime('${groupBy}', datetime(timestamp, 'unixepoch')) as period,
      SUM(CAST(amount AS REAL)) as total_burned
     FROM burns
     WHERE timestamp > ?
     GROUP BY period
     ORDER BY period`,
    [startTime],
    (err, rows) => {
      if (err) {
        return res.status(500).json({ error: err.message });
      }
      res.json(rows);
    }
  );
});

// Get recent transactions
app.get('/api/transactions/recent', (req, res) => {
  const limit = parseInt(req.query.limit) || 10;
  
  db.all(
    `SELECT tx_hash, block_number, amount, from_address, timestamp
     FROM burns
     ORDER BY timestamp DESC
     LIMIT ?`,
    [limit],
    (err, rows) => {
      if (err) {
        return res.status(500).json({ error: err.message });
      }
      res.json(rows);
    }
  );
});

// Sync historical data (optional endpoint for initial data load)
app.post('/api/sync/historical', async (req, res) => {
  const { fromBlock, toBlock } = req.body;
  
  try {
    const contract = new web3.eth.Contract([TRANSFER_EVENT_ABI], REACT_TOKEN_ADDRESS);
    
    const events = await contract.getPastEvents('Transfer', {
      filter: { to: BURN_ADDRESS },
      fromBlock: fromBlock || 0,
      toBlock: toBlock || 'latest'
    });
    
    let processed = 0;
    for (const event of events) {
      await processBurnEvent(event);
      processed++;
    }
    
    res.json({ 
      success: true, 
      message: `Processed ${processed} historical burn events` 
    });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Health check
app.get('/api/health', (req, res) => {
  res.json({ 
    status: 'ok', 
    timestamp: new Date().toISOString(),
    websocket: wss.clients.size + ' clients connected'
  });
});

// Start server
server.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
  
  // Initialize database first, then start services
  initializeDatabase(() => {
    console.log('Database initialized');
    subscribeToBurnEvents();
    updateTokenPrice();
  });
});

// Graceful shutdown
process.on('SIGTERM', () => {
  console.log('SIGTERM received, closing server...');
  db.close();
  wss.close();
  server.close();
  process.exit(0);
});
