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

// Important Reactive Network addresses
const REACT_TOKEN_ADDRESS = process.env.REACT_TOKEN_ADDRESS || '0x0000000000000000000000000000000000fffFfF';
const SYSTEM_CONTRACT_ADDRESS = '0x0000000000000000000000000000000000fffFfF'; // System contract & callback proxy
const CALLBACK_PROXY_ADDRESS = '0x0000000000000000000000000000000000fffFfF'; // Same as system contract

// ABI for tracking various events
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

// ABI for debt settlement events (coverDebt calls)
const DEBT_SETTLED_EVENT_ABI = {
  anonymous: false,
  inputs: [
    { indexed: true, name: 'contract', type: 'address' },
    { indexed: false, name: 'amount', type: 'uint256' }
  ],
  name: 'DebtSettled',
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
    // Create burns table - now with burn_type field
    db.run(`
      CREATE TABLE IF NOT EXISTS burns (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        tx_hash TEXT UNIQUE NOT NULL,
        block_number INTEGER NOT NULL,
        amount TEXT NOT NULL,
        from_address TEXT NOT NULL,
        timestamp INTEGER NOT NULL,
        usd_value REAL,
        burn_type TEXT NOT NULL,
        gas_used INTEGER,
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
    db.run('CREATE INDEX IF NOT EXISTS idx_block ON burns(block_number)');
    db.run('CREATE INDEX IF NOT EXISTS idx_burn_type ON burns(burn_type)', callback);
  });
}

// Track regular transaction gas fees
async function processRegularTransaction(tx, block) {
  try {
    const receipt = await web3.eth.getTransactionReceipt(tx.hash);
    
    if (receipt) {
      // Calculate burned REACT: gasUsed * gasPrice
      const gasUsed = BigInt(receipt.gasUsed);
      
      // Try different ways to get gas price
      let gasPrice = BigInt(0);
      let effectiveGasPrice = BigInt(0);
      
      // For Reactive Network, we need to check multiple price fields
      // Priority: effectiveGasPrice > gasPrice > baseFeePerGas + priorityFee
      
      // Method 1: Receipt's effectiveGasPrice (most accurate)
      if (receipt.effectiveGasPrice) {
        effectiveGasPrice = BigInt(receipt.effectiveGasPrice);
      }
      // Method 2: Transaction gasPrice
      else if (tx.gasPrice) {
        effectiveGasPrice = BigInt(tx.gasPrice);
      }
      // Method 3: Calculate from base fee + priority fee (EIP-1559)
      else if (block.baseFeePerGas) {
        const baseFee = BigInt(block.baseFeePerGas);
        const priorityFee = tx.maxPriorityFeePerGas ? BigInt(tx.maxPriorityFeePerGas) : BigInt(0);
        effectiveGasPrice = baseFee + priorityFee;
      }
      
      const burnedWei = gasUsed * effectiveGasPrice;
      
      // Convert from Wei to REACT (18 decimals)
      const burnedReact = parseFloat(web3.utils.fromWei(burnedWei.toString(), 'ether'));
      const usdValue = burnedReact * currentPrice;
      
      // Log for debugging
      if (burnedWei === 0n && gasUsed > 0n) {
        console.log(`Zero fee tx ${tx.hash}: gasUsed=${gasUsed}, effectiveGasPrice=${effectiveGasPrice}, type=${tx.type}`);
      } else if (burnedReact > 0) {
        console.log(`Fee burn tx ${tx.hash}: ${burnedReact} REACT (gasUsed=${gasUsed}, gasPrice=${effectiveGasPrice})`);
      }
      
      // Store in database
      db.run(
        `INSERT OR IGNORE INTO burns (tx_hash, block_number, amount, from_address, timestamp, usd_value, burn_type, gas_used) 
         VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
        [tx.hash, Number(block.number), burnedReact.toFixed(18), tx.from, Number(block.timestamp), usdValue, 'gas_fee', Number(receipt.gasUsed)],
        (err) => {
          if (err && !err.message.includes('UNIQUE constraint failed')) {
            console.error('Error inserting gas fee burn:', err);
            return;
          }
        }
      );
      
      return burnedReact;
    }
    return 0;
  } catch (error) {
    console.error('Error processing transaction:', error);
    return 0;
  }
}

// Track RVM transactions and debt settlements
async function trackSystemContractEvents(fromBlock) {
  try {
    // Monitor transfers to system contract (RVM payments and deposits)
    const transferEvents = await web3.eth.getPastLogs({
      fromBlock: fromBlock,
      toBlock: 'latest',
      address: REACT_TOKEN_ADDRESS,
      topics: [
        web3.utils.sha3('Transfer(address,address,uint256)'),
        null, // from: any address
        web3.utils.padLeft(SYSTEM_CONTRACT_ADDRESS.toLowerCase(), 64) // to: system contract
      ]
    });

    for (const event of transferEvents) {
      try {
        // Decode the transfer event
        const decodedEvent = web3.eth.abi.decodeLog(
          TRANSFER_EVENT_ABI.inputs,
          event.data,
          event.topics.slice(1)
        );
        
        const amount = parseFloat(web3.utils.fromWei(decodedEvent.value, 'ether'));
        const usdValue = amount * currentPrice;
        
        // Get block info for timestamp
        const block = await web3.eth.getBlock(event.blockNumber);
        
        // Determine burn type based on method called
        const tx = await web3.eth.getTransaction(event.transactionHash);
        let burnType = 'rvm_payment';
        
        if (tx.input && tx.input.includes('depositTo')) {
          burnType = 'rvm_deposit';
        } else if (tx.input && tx.input.includes('coverDebt')) {
          burnType = 'debt_settlement';
        }
        
        db.run(
          `INSERT OR IGNORE INTO burns (tx_hash, block_number, amount, from_address, timestamp, usd_value, burn_type, gas_used) 
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
          [event.transactionHash, Number(event.blockNumber), amount.toFixed(18), decodedEvent.from, Number(block.timestamp), usdValue, burnType, 0]
        );
        
      } catch (error) {
        console.error('Error processing system contract event:', error);
      }
    }
  } catch (error) {
    // Only log if it's not the validation error we're seeing
    if (!error.message.includes('must pass "filter" validation')) {
      console.error('Error tracking system contract events:', error);
    }
  }
}

// Poll for new blocks instead of subscribing
async function pollBlocks() {
  let lastProcessedBlock = null;
  
  // Get initial block from metadata
  db.get('SELECT value FROM metadata WHERE key = ?', ['last_processed_block'], async (err, row) => {
    if (row) {
      lastProcessedBlock = BigInt(row.value);
    }
    
    // Start polling
    setInterval(async () => {
      try {
        const currentBlock = await web3.eth.getBlockNumber();
        
        // If this is our first run, set the last processed block
        if (lastProcessedBlock === null) {
          lastProcessedBlock = currentBlock - 1n;
          db.run('INSERT OR REPLACE INTO metadata (key, value) VALUES (?, ?)', 
            ['last_processed_block', lastProcessedBlock.toString()]);
        }
        
        // Process any new blocks
        while (lastProcessedBlock < currentBlock) {
          lastProcessedBlock++;
          
          try {
            // Get full block with transactions
            const block = await web3.eth.getBlock(lastProcessedBlock, true);
            
            if (block && block.transactions && block.transactions.length > 0) {
              console.log(`Processing block ${Number(block.number)} with ${block.transactions.length} transactions`);
              
              let totalBurnedInBlock = 0;
              
              // Process regular transactions for gas fees
              for (const tx of block.transactions) {
                const burned = await processRegularTransaction(tx, block);
                totalBurnedInBlock += burned;
              }
              
              // Track system contract events for RVM and callbacks
              await trackSystemContractEvents(block.number);
              
              // Update last processed block
              db.run('INSERT OR REPLACE INTO metadata (key, value) VALUES (?, ?)', 
                ['last_processed_block', lastProcessedBlock.toString()]);
              
              // Get updated totals
              db.get(
                `SELECT COUNT(*) as count, SUM(CAST(amount AS REAL)) as total FROM burns`,
                (err, row) => {
                  if (!err && row) {
                    // Broadcast update to clients
                    broadcast({
                      type: 'block_processed',
                      data: {
                        blockNumber: Number(block.number),
                        transactionCount: block.transactions.length,
                        burnedInBlock: totalBurnedInBlock,
                        totalBurned: row.total || 0,
                        totalTransactions: row.count || 0
                      }
                    });
                  }
                }
              );
            }
          } catch (error) {
            console.error(`Error processing block ${lastProcessedBlock}:`, error);
            // Don't update lastProcessedBlock so we retry this block
            lastProcessedBlock--;
            break;
          }
        }
      } catch (error) {
        console.error('Error in block polling:', error);
      }
    }, 3000); // Poll every 3 seconds
    
    console.log('Started block polling for comprehensive burn tracking');
  });
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

// Get total burns with breakdown by type
app.get('/api/stats/total', (req, res) => {
  db.all(
    `SELECT 
      burn_type,
      COUNT(*) as count,
      SUM(CAST(amount AS REAL)) as total
     FROM burns
     GROUP BY burn_type`,
    (err, typeBreakdown) => {
      if (err) {
        return res.status(500).json({ error: err.message });
      }
      
      db.get(
        `SELECT 
          COUNT(*) as total_transactions,
          SUM(CAST(amount AS REAL)) as total_burned,
          SUM(usd_value) as total_usd_value
         FROM burns`,
        (err, totals) => {
          if (err) {
            return res.status(500).json({ error: err.message });
          }
          
          res.json({
            totalTransactions: totals.total_transactions || 0,
            totalBurned: totals.total_burned || 0,
            totalUsdValue: totals.total_usd_value || 0,
            currentPrice: currentPrice,
            breakdown: typeBreakdown || []
          });
        }
      );
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
      SUM(CAST(amount AS REAL)) as total_burned,
      burn_type
     FROM burns
     WHERE timestamp > ?
     GROUP BY period, burn_type
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

// Get recent transactions with type info
app.get('/api/transactions/recent', (req, res) => {
  const limit = parseInt(req.query.limit) || 10;
  
  db.all(
    `SELECT tx_hash, block_number, amount, from_address, timestamp, burn_type, gas_used
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

// Get burn type statistics
app.get('/api/stats/burn-types', (req, res) => {
  db.all(
    `SELECT 
      burn_type,
      COUNT(*) as count,
      SUM(CAST(amount AS REAL)) as total,
      AVG(CAST(amount AS REAL)) as average
     FROM burns
     GROUP BY burn_type
     ORDER BY total DESC`,
    (err, rows) => {
      if (err) {
        return res.status(500).json({ error: err.message });
      }
      res.json(rows);
    }
  );
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
    
    // Set deployment block if first time
    web3.eth.getBlockNumber().then(currentBlock => {
      db.get('SELECT value FROM metadata WHERE key = ?', ['deployment_block'], (err, row) => {
        if (!row) {
          const deploymentTime = new Date().toISOString();
          db.run('INSERT INTO metadata (key, value) VALUES (?, ?)', ['deployment_block', currentBlock.toString()]);
          db.run('INSERT INTO metadata (key, value) VALUES (?, ?)', ['deployment_time', deploymentTime]);
          console.log(`First deployment! Starting tracking from block ${currentBlock}`);
        }
      });
    });
    
    pollBlocks();
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
