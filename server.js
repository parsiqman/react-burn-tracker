// server.js - Production Backend for REACT Burn Tracker
const express = require('express');
const cors = require('cors');
const { Web3 } = require('web3');
const sqlite3 = require('sqlite3').verbose();
const WebSocket = require('ws');
const path = require('path');
const fs = require('fs');
const { spawn } = require('child_process');
require('dotenv').config();

// Check if we should run historical sync
if (process.env.RUN_HISTORICAL_SYNC === 'true') {
  console.log('Starting historical sync on startup...');
  require('./scripts/sync-historical-direct.js');
  return; // Exit after sync
}

const app = express();
const PORT = process.env.PORT || 3000;

// Middleware
app.use(cors());
app.use(express.json());
app.use(express.static('public'));

// Initialize Web3
const web3 = new Web3(process.env.RPC_URL || 'https://rpc.reactive.network');

// Initialize SQLite Database
const dbPath = process.env.NODE_ENV === 'production' 
  ? '/app/data/burns.db'  // Persistent disk on Render
  : './burns.db';         // Local development

// For local development, ensure directory exists
if (process.env.NODE_ENV !== 'production' && !fs.existsSync(path.dirname(dbPath))) {
  fs.mkdirSync(path.dirname(dbPath), { recursive: true });
}

const db = new sqlite3.Database(dbPath);
console.log(`Using persistent database at: ${dbPath}`);

// Global variables to track if sync is running
let syncInProgress = false;
let syncProcess = null;
let syncStartTime = null;
let syncLogs = [];

// Important Reactive Network addresses
const REACT_TOKEN_ADDRESS = process.env.REACT_TOKEN_ADDRESS || '0x0000000000000000000000000000000000fffFfF';
const SYSTEM_CONTRACT_ADDRESS = '0x0000000000000000000000000000000000fffFfF'; // System contract & callback proxy
const CALLBACK_PROXY_ADDRESS = '0x0000000000000000000000000000000000fffFfF'; // Same as system contract

// Cross-chain callback proxy addresses for other chains
const CALLBACK_PROXIES = {
  // Mainnet chains - ACTUAL ADDRESSES FROM DOCS
  '1': '0x1D5267C1bb7D8bA68964dDF3990601BDB7902D76', // Ethereum
  '56': '0xdb81A196A0dF9Ef974C9430495a09B6d535fAc48', // BSC
  '43114': '0x934Ea75496562D4e83E80865c33dbA600644fCDa', // Avalanche
  '8453': '0x0D3E76De6bC44309083cAAFdB49A088B8a250947', // Base
  '42161': '0x4730c58FDA9d78f60c987039aEaB7d261aAd042E', // Arbitrum One
  '146': '0x9299472a6399fd1027ebf067571eb3e3d7837fc4', // Sonic
  '999': '0x9299472a6399fd1027ebf067571eb3e3d7837fc4', // HyperEVM
  // Testnet chains
  '11155111': '0x6E1181FE9C0189b54a60EE1c5588a066567B5c08', // Sepolia
  '421614': '0xd30e1bf9aa95f5d1a96e03e99c91c0fcc2f90b36', // Arbitrum Sepolia
  '43113': '0xB4E890c63c3c8d8b9e63DdCfbBC2c7DC0D2EF57F', // Avalanche Fuji
  '84532': '0xa32b48D8c2A942B9dd87768D1fEd19be36B45660', // Base Sepolia
  '168587773': '0x7bbE7Bd2fbC10E2e7aC28aEaAe1c8453e5C37762' // Blast Sepolia
};

// Create a reverse lookup for quick checking
const ALL_CALLBACK_PROXIES = new Set(Object.values(CALLBACK_PROXIES).map(addr => addr.toLowerCase()));

// Hyperlane Mailbox addresses
const HYPERLANE_MAILBOXES = {
  // Mainnet
  '1': '0x0000000000000000000000000000000000000000', // Ethereum
  '56': '0x0000000000000000000000000000000000000000', // BSC
  '43114': '0x0000000000000000000000000000000000000000', // Avalanche
  '8453': '0x0000000000000000000000000000000000000000', // Base
  '146': '0x0000000000000000000000000000000000000000', // Sonic
  '1597': '0x0000000000000000000000000000000000000000' // Reactive
};

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
      
      // Known cross-chain related method signatures (first 4 bytes of keccak256 hash)
      const crossChainMethods = {
        // Standard methods
        '0x095ea7b3': 'approve',
        '0xa9059cbb': 'transfer',
        '0x23b872dd': 'transferFrom',
        // Reactive Network specific methods
        '0xb90dc8ff': 'reactive_callback', // We saw this in transactions
        '0x90dccff': 'reactive_automation', // The method we're seeing repeatedly
        '0x150b7a02': 'onERC721Received',
        '0xf23a6e61': 'onERC1155Received',
        // Hyperlane methods
        '0xaa12742e': 'dispatch', // Hyperlane dispatch
        '0x8d3e0e1c': 'handle', // Hyperlane handle
        // Common system methods
        '0x2f2ff15d': 'grantRole',
        '0x91d14854': 'hasRole',
        '0xac9650d8': 'multicall',
        '0x': 'empty_data'
      };
      
      // Initialize transaction type and info variables
      let transactionType = 'gas_fee';
      let txInfo = '';
      
      // Analyze transaction for cross-chain activity
      let isLikelyCrossChain = false;
      let crossChainInfo = '';
      
      const toAddress = tx.to ? tx.to.toLowerCase() : '';
      const fromAddress = tx.from ? tx.from.toLowerCase() : '';
      
      // Check if FROM address is a known callback proxy (cross-chain callback coming IN)
      if (ALL_CALLBACK_PROXIES.has(fromAddress)) {
        isLikelyCrossChain = true;
        // Find which chain it's from
        const chainId = Object.entries(CALLBACK_PROXIES).find(([id, addr]) => 
          addr.toLowerCase() === fromAddress
        )?.[0];
        const chainName = {
          '1': 'Ethereum',
          '56': 'BSC',
          '43114': 'Avalanche',
          '8453': 'Base',
          '42161': 'Arbitrum',
          '146': 'Sonic',
          '999': 'HyperEVM'
        }[chainId] || `Chain ${chainId}`;
        crossChainInfo = `[Callback from ${chainName}]`;
      }
      
      // Transactions TO the system contract on Reactive are NOT cross-chain
      // They're just paying for reactive services
      if (toAddress === SYSTEM_CONTRACT_ADDRESS.toLowerCase() && !isLikelyCrossChain) {
        // This is a regular system contract interaction, not cross-chain
        transactionType = 'system_contract_payment';
        crossChainInfo = '[System Contract Payment]';
      }
      
      // Check if it's a Hyperlane mailbox
      if (Object.values(HYPERLANE_MAILBOXES).includes(toAddress)) {
        isLikelyCrossChain = true;
        crossChainInfo = '[Hyperlane Mailbox]';
      }
      
      // Check method signature for cross-chain patterns
      if (tx.input && tx.input.length >= 10) {
        const methodSig = tx.input.substring(0, 10);
        const methodName = crossChainMethods[methodSig];
        
        if (methodName && ['dispatch', 'handle', 'reactive_callback'].includes(methodName)) {
          isLikelyCrossChain = true;
          crossChainInfo += ` [${methodName}]`;
        }
        
        // Log unknown method signatures for analysis
        if (!crossChainMethods[methodSig] && methodSig !== '0x') {
          console.log(`Unknown method: ${methodSig} in tx ${tx.hash} to ${toAddress}`);
        }
      }
      
      // Special handling for the automated address we're seeing
      const AUTOMATION_ADDRESS = '0xaa24633108fd1d87371c55e6d7f4fa00cdeb26';
      
      if (fromAddress === AUTOMATION_ADDRESS.toLowerCase() && toAddress === SYSTEM_CONTRACT_ADDRESS.toLowerCase()) {
        // This is the known automation contract
        transactionType = 'system_contract_payment';
        crossChainInfo = '[RSC Automation]';
      }
      
      // Determine transaction type based on analysis
      if (gasUsed === 21000n) {
        // Simple transfer (21000 gas)
        if (tx.input && tx.input !== '0x' && tx.input.length > 2) {
          const methodSig = tx.input.substring(0, 10);
          const methodName = crossChainMethods[methodSig] || 'unknown';
          transactionType = isLikelyCrossChain ? 'cross_chain_transfer' : 'gas_fee_with_data';
          txInfo = `(method: ${methodSig} - ${methodName}) ${crossChainInfo}`;
        } else if (tx.to && tx.to.toLowerCase() === tx.from.toLowerCase()) {
          transactionType = 'gas_fee_self';
          txInfo = '(self-transaction)';
        } else {
          transactionType = 'gas_fee_transfer';
          txInfo = '(simple transfer)';
        }
      } else {
        // Contract interaction - likely RVM or system payment
        if (isLikelyCrossChain) {
          transactionType = 'cross_chain_contract';
        } else if (toAddress === SYSTEM_CONTRACT_ADDRESS.toLowerCase()) {
          transactionType = 'system_contract_payment';
        } else if (toAddress.startsWith('0x00000000')) {
          transactionType = 'special_contract_call';
        } else {
          transactionType = 'gas_fee_contract';
        }
        
        // Include method signature if available
        if (tx.input && tx.input.length >= 10) {
          const methodSig = tx.input.substring(0, 10);
          const methodName = crossChainMethods[methodSig] || 'unknown';
          txInfo = `${crossChainInfo} method: ${methodName} (${gasUsed} gas)`;
        } else {
          txInfo = `${crossChainInfo} (${gasUsed} gas)`;
        }
      }
      
      // Get transaction logs to detect cross-chain events
      try {
        const logs = await web3.eth.getTransactionReceipt(tx.hash).then(r => r.logs);
        if (logs && logs.length > 0) {
          // Look for cross-chain event signatures
          for (const log of logs) {
            // Common cross-chain event topics
            if (log.topics[0]) {
              const eventSig = log.topics[0];
              // Log interesting events
              if (log.address && log.address.toLowerCase() !== REACT_TOKEN_ADDRESS.toLowerCase()) {
                console.log(`Cross-chain event in ${tx.hash}: contract=${log.address}, event=${eventSig.substring(0, 10)}`);
                transactionType = 'cross_chain_activity';
                txInfo += ' [Cross-chain Event]';
                break;
              }
            }
          }
        }
      } catch (err) {
        // Ignore log fetch errors
      }
      
      // Log interesting patterns
      if (burnedReact === 0.00303820 || Math.abs(burnedReact - 0.003038196) < 0.000001) {
        console.log(`Standard fee tx ${tx.hash}: to=${tx.to}, from=${tx.from}, type=${transactionType}, info=${txInfo}`);
      }
      
      // Store in database with additional info
      db.run(
        `INSERT OR IGNORE INTO burns (tx_hash, block_number, amount, from_address, timestamp, usd_value, burn_type, gas_used) 
         VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
        [tx.hash, Number(block.number), burnedReact.toFixed(18), tx.from, Number(block.timestamp), usdValue, transactionType, Number(receipt.gasUsed)],
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

// Get total burns with breakdown by type and unique addresses
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
          SUM(usd_value) as total_usd_value,
          COUNT(DISTINCT from_address) as unique_addresses
         FROM burns`,
        (err, totals) => {
          if (err) {
            return res.status(500).json({ error: err.message });
          }
          
          res.json({
            totalTransactions: totals.total_transactions || 0,
            totalBurned: totals.total_burned || 0,
            totalUsdValue: totals.total_usd_value || 0,
            uniqueAddresses: totals.unique_addresses || 0,
            currentPrice: currentPrice,
            breakdown: typeBreakdown || []
          });
        }
      );
    }
  );
});

// Get 24h stats with unique addresses
app.get('/api/stats/24h', (req, res) => {
  const dayAgo = Math.floor(Date.now() / 1000) - (24 * 60 * 60);
  
  db.get(
    `SELECT 
      COUNT(*) as transactions_24h,
      SUM(CAST(amount AS REAL)) as burned_24h,
      COUNT(DISTINCT from_address) as unique_addresses_24h
     FROM burns
     WHERE timestamp > ?`,
    [dayAgo],
    (err, row) => {
      if (err) {
        return res.status(500).json({ error: err.message });
      }
      res.json({
        transactions24h: row.transactions_24h || 0,
        burned24h: row.burned_24h || 0,
        uniqueAddresses24h: row.unique_addresses_24h || 0
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

// Get transaction analysis
app.get('/api/analysis/patterns', (req, res) => {
  db.all(
    `SELECT 
      burn_type,
      amount,
      COUNT(*) as count,
      COUNT(DISTINCT from_address) as unique_addresses
     FROM burns
     WHERE amount > 0
     GROUP BY burn_type, amount
     ORDER BY count DESC
     LIMIT 20`,
    (err, patterns) => {
      if (err) {
        return res.status(500).json({ error: err.message });
      }
      
      // Get most active addresses
      db.all(
        `SELECT 
          from_address,
          COUNT(*) as tx_count,
          SUM(CAST(amount AS REAL)) as total_burned
         FROM burns
         GROUP BY from_address
         ORDER BY tx_count DESC
         LIMIT 10`,
        (err, addresses) => {
          if (err) {
            return res.status(500).json({ error: err.message });
          }
          
          res.json({
            patterns: patterns,
            topAddresses: addresses
          });
        }
      );
    }
  );
});

// Get cross-chain activity analysis
app.get('/api/analysis/cross-chain', async (req, res) => {
  try {
    // Get recent blocks to analyze
    const currentBlock = await web3.eth.getBlockNumber();
    const fromBlock = currentBlock - 100n; // Last 100 blocks
    
    const crossChainActivity = {
      systemContractCalls: 0,
      methodSignatures: {},
      activeContracts: new Set(),
      potentialBridges: []
    };
    
    // Analyze recent transactions
    for (let i = fromBlock; i <= currentBlock; i++) {
      const block = await web3.eth.getBlock(i, true);
      if (block && block.transactions) {
        for (const tx of block.transactions) {
          // Check for system contract interactions
          if (tx.to && tx.to.toLowerCase() === SYSTEM_CONTRACT_ADDRESS.toLowerCase()) {
            crossChainActivity.systemContractCalls++;
          }
          
          // Track method signatures
          if (tx.input && tx.input.length > 10) {
            const methodSig = tx.input.substring(0, 10);
            crossChainActivity.methodSignatures[methodSig] = (crossChainActivity.methodSignatures[methodSig] || 0) + 1;
          }
          
          // Track contract addresses
          if (tx.to && tx.to !== '0x0000000000000000000000000000000000000000') {
            crossChainActivity.activeContracts.add(tx.to.toLowerCase());
          }
        }
      }
    }
    
    res.json({
      blocksAnalyzed: Number(currentBlock - fromBlock + 1n),
      systemContractActivity: crossChainActivity.systemContractCalls,
      uniqueContracts: crossChainActivity.activeContracts.size,
      topMethodSignatures: Object.entries(crossChainActivity.methodSignatures)
        .sort((a, b) => b[1] - a[1])
        .slice(0, 10)
        .map(([sig, count]) => ({ signature: sig, count })),
      contracts: Array.from(crossChainActivity.activeContracts).slice(0, 20)
    });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Admin endpoint to trigger historical sync
app.post('/api/admin/sync-historical', (req, res) => {
  // Simple authentication - you should improve this!
  const adminToken = req.headers['x-admin-token'];
  if (adminToken !== process.env.ADMIN_TOKEN) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  if (syncInProgress) {
    return res.status(409).json({ 
      error: 'Sync already in progress',
      startTime: syncStartTime,
      duration: Date.now() - syncStartTime
    });
  }

  // Start the sync process
  syncInProgress = true;
  syncStartTime = Date.now();
  syncLogs = [];

  syncProcess = spawn('node', ['scripts/sync-historical-direct.js'], {
    cwd: process.cwd(),
    env: process.env
  });

  syncProcess.stdout.on('data', (data) => {
    const log = data.toString();
    console.log('[SYNC]', log);
    syncLogs.push({ type: 'stdout', message: log, timestamp: new Date() });
  });

  syncProcess.stderr.on('data', (data) => {
    const log = data.toString();
    console.error('[SYNC ERROR]', log);
    syncLogs.push({ type: 'stderr', message: log, timestamp: new Date() });
  });

  syncProcess.on('close', (code) => {
    syncInProgress = false;
    syncProcess = null;
    const duration = Date.now() - syncStartTime;
    console.log(`Historical sync completed with code ${code} in ${duration}ms`);
    syncLogs.push({ 
      type: 'complete', 
      message: `Sync completed with code ${code}`, 
      duration,
      timestamp: new Date() 
    });
  });

  res.json({ 
    message: 'Historical sync started',
    startTime: syncStartTime
  });
});

// Endpoint to check sync status
app.get('/api/admin/sync-status', (req, res) => {
  // Simple authentication
  const adminToken = req.headers['x-admin-token'];
  if (adminToken !== process.env.ADMIN_TOKEN) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  if (syncInProgress) {
    res.json({
      status: 'running',
      startTime: syncStartTime,
      duration: Date.now() - syncStartTime,
      recentLogs: syncLogs.slice(-20) // Last 20 log entries
    });
  } else {
    res.json({
      status: 'idle',
      lastRunTime: syncStartTime,
      lastRunDuration: syncStartTime ? Date.now() - syncStartTime : null,
      recentLogs: syncLogs.slice(-20)
    });
  }
});

// Endpoint to stop sync if needed
app.post('/api/admin/sync-stop', (req, res) => {
  const adminToken = req.headers['x-admin-token'];
  if (adminToken !== process.env.ADMIN_TOKEN) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  if (!syncInProgress || !syncProcess) {
    return res.status(400).json({ error: 'No sync in progress' });
  }

  syncProcess.kill('SIGTERM');
  syncInProgress = false;
  
  res.json({ message: 'Sync process terminated' });
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
