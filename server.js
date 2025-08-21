// server.js - Production Backend for REACT Burn Tracker with Holder Analytics and CoinGecko Price API
const express = require('express');
const cors = require('cors');
const { Web3 } = require('web3');
const sqlite3 = require('sqlite3').verbose();
const WebSocket = require('ws');
const path = require('path');
const fs = require('fs');
require('dotenv').config();

// Import HolderTracker
const HolderTracker = require('./scripts/holder-tracker');

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

// Set database optimizations for better concurrency
db.run("PRAGMA journal_mode = WAL");
db.run("PRAGMA busy_timeout = 30000");
db.run("PRAGMA cache_size = 10000");
db.run("PRAGMA synchronous = NORMAL");

// Global variables to track if sync is running
let syncInProgress = false;
let syncStartTime = null;
let syncLogs = [];
let syncStats = null;
let syncInterval = null;
let syncPaused = false;

// Holder tracker instance
let holderTracker = null;

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

// Known addresses database for labeling
const KNOWN_ADDRESSES = {
  '0xaa24633108fd1d87371c55e6d7f4fa00cdeb26': {
    label: 'RSC Automation Bot',
    type: 'bot',
    description: 'Reactive Smart Contract automation service'
  },
  // Add more known addresses as discovered
};

// Price tracking variables
let currentPrice = 0;
let priceData = {
  price: 0,
  marketCap: 0,
  priceChange24h: 0,
  circulatingSupply: 0,
  totalSupply: 0,
  rank: 0,
  lastUpdated: null
};

// CoinGecko API integration
async function updateTokenPrice() {
  try {
    console.log('Fetching REACT price from CoinGecko...');
    
    // Fetch price data from CoinGecko
    const priceResponse = await fetch(
      'https://api.coingecko.com/api/v3/simple/price?ids=reactive-network&vs_currencies=usd&include_market_cap=true&include_24hr_change=true'
    );
    
    if (!priceResponse.ok) {
      throw new Error(`CoinGecko API error: ${priceResponse.status}`);
    }
    
    const priceJson = await priceResponse.json();
    
    if (priceJson['reactive-network']) {
      priceData.price = priceJson['reactive-network'].usd || 0;
      priceData.marketCap = priceJson['reactive-network'].usd_market_cap || 0;
      priceData.priceChange24h = priceJson['reactive-network'].usd_24h_change || 0;
      currentPrice = priceData.price;
      
      console.log(`REACT price updated: $${currentPrice} (${priceData.priceChange24h >= 0 ? '+' : ''}${priceData.priceChange24h.toFixed(2)}%)`);
    }
    
    // Fetch detailed coin info for more data (optional, can be disabled if rate limited)
    try {
      const detailsResponse = await fetch(
        'https://api.coingecko.com/api/v3/coins/reactive-network?localization=false&tickers=false&market_data=true&community_data=false&developer_data=false'
      );
      
      if (detailsResponse.ok) {
        const detailsJson = await detailsResponse.json();
        
        if (detailsJson.market_data) {
          priceData.rank = detailsJson.market_cap_rank || 0;
          priceData.circulatingSupply = detailsJson.market_data.circulating_supply || 0;
          priceData.totalSupply = detailsJson.market_data.total_supply || priceData.circulatingSupply;
        }
      }
    } catch (detailError) {
      // Non-critical error, just log it
      console.log('Could not fetch detailed coin info:', detailError.message);
    }
    
    priceData.lastUpdated = new Date().toISOString();
    
    // Store price in database for historical tracking (optional)
    db.run(
      'INSERT OR REPLACE INTO metadata (key, value) VALUES (?, ?)',
      ['current_react_price', JSON.stringify(priceData)],
      (err) => {
        if (err) console.error('Error storing price data:', err);
      }
    );
    
  } catch (error) {
    console.error('Error updating REACT price:', error.message);
    
    // Try to load last known price from database as fallback
    db.get('SELECT value FROM metadata WHERE key = ?', ['current_react_price'], (err, row) => {
      if (!err && row) {
        try {
          const savedData = JSON.parse(row.value);
          priceData = savedData;
          currentPrice = savedData.price;
          console.log('Using last known price from database: $' + currentPrice);
        } catch (parseError) {
          console.error('Error parsing saved price data:', parseError);
        }
      }
    });
  }
}

// Update price on startup and every 60 seconds (be mindful of rate limits)
updateTokenPrice();
setInterval(updateTokenPrice, 60 * 1000); // Every minute

// Create HTTP server
const server = require('http').createServer(app);

// Initialize WebSocket server on the same server
const wss = new WebSocket.Server({ server });

// WebSocket connection handling
wss.on('connection', (ws) => {
  console.log('New WebSocket client connected');
  
  // Send current price data to new client
  ws.send(JSON.stringify({
    type: 'price_update',
    data: priceData
  }));
  
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

// Broadcast price updates to all clients
function broadcastPriceUpdate() {
  broadcast({
    type: 'price_update',
    data: priceData
  });
}

// Initialize tables with proper callbacks - UPDATED WITH HOLDER TABLES
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

    // Create indexes for burns
    db.run('CREATE INDEX IF NOT EXISTS idx_timestamp ON burns(timestamp)');
    db.run('CREATE INDEX IF NOT EXISTS idx_block ON burns(block_number)');
    db.run('CREATE INDEX IF NOT EXISTS idx_burn_type ON burns(burn_type)');
    db.run('CREATE INDEX IF NOT EXISTS idx_from_address ON burns(from_address)');

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

    // Create holder_history table
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

    // Create known_addresses table
    db.run(`
      CREATE TABLE IF NOT EXISTS known_addresses (
        address TEXT PRIMARY KEY,
        label TEXT NOT NULL,
        category TEXT,
        description TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
      )
    `);

    // Create indexes for holder tables
    db.run('CREATE INDEX IF NOT EXISTS idx_balance_numeric ON holders(balance_numeric DESC)');
    db.run('CREATE INDEX IF NOT EXISTS idx_tx_count ON holders(tx_count DESC)');
    db.run('CREATE INDEX IF NOT EXISTS idx_last_updated ON holders(last_updated_timestamp)');
    db.run('CREATE INDEX IF NOT EXISTS idx_is_contract ON holders(is_contract)');
    db.run('CREATE INDEX IF NOT EXISTS idx_history_timestamp ON holder_history(timestamp DESC)');
    db.run('CREATE INDEX IF NOT EXISTS idx_history_block ON holder_history(block_number DESC)', callback);
  });
}

// Initialize holder tracker
function initializeHolderTracker() {
  holderTracker = new HolderTracker(db, web3);
  holderTracker.initialize().then(() => {
    console.log('Holder tracker initialized');
    
    // Update holder balances every hour
    setInterval(async () => {
      try {
        console.log('Updating holder balances...');
        const currentBlock = await web3.eth.getBlockNumber();
        
        // Get addresses that were active in recent blocks
        const recentAddresses = await new Promise((resolve) => {
          db.all(
            'SELECT DISTINCT from_address FROM burns WHERE block_number > ?',
            [currentBlock - 1000],
            (err, rows) => {
              resolve(rows ? rows.map(r => r.from_address) : []);
            }
          );
        });
        
        // Quick update for recent active addresses
        if (recentAddresses.length > 0) {
          await holderTracker.quickUpdateActiveAddresses(recentAddresses);
        }
        
        // Full scan every 6 hours
        const lastFullScan = await new Promise((resolve) => {
          db.get('SELECT value FROM metadata WHERE key = ?', ['last_full_holder_scan'], (err, row) => {
            resolve(row ? parseInt(row.value) : 0);
          });
        });
        
        const sixHoursAgo = Math.floor(Date.now() / 1000) - (6 * 60 * 60);
        if (lastFullScan < sixHoursAgo) {
          await holderTracker.scanForHolders(holderTracker.lastScanBlock, currentBlock);
          await holderTracker.saveHolderSnapshot(currentBlock);
          
          db.run(
            'INSERT OR REPLACE INTO metadata (key, value) VALUES (?, ?)',
            ['last_full_holder_scan', Math.floor(Date.now() / 1000).toString()]
          );
        }
      } catch (error) {
        console.error('Error updating holders:', error);
      }
    }, 60 * 60 * 1000); // Every hour
  }).catch(error => {
    console.error('Error initializing holder tracker:', error);
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
      
      // Check for known addresses
      const knownAddress = KNOWN_ADDRESSES[fromAddress];
      if (knownAddress) {
        txInfo = `[${knownAddress.label}]`;
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
      
      // Store in database with additional info - use currentPrice for USD value
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
          [
            { type: 'address', name: 'from', indexed: true },
            { type: 'address', name: 'to', indexed: true },
            { type: 'uint256', name: 'value', indexed: false }
          ],
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
              const activeAddresses = [];
              
              // Process regular transactions for gas fees
              for (const tx of block.transactions) {
                const burned = await processRegularTransaction(tx, block);
                totalBurnedInBlock += burned;
                
                // Collect active addresses for holder updates
                if (tx.from) activeAddresses.push(tx.from.toLowerCase());
                if (tx.to) activeAddresses.push(tx.to.toLowerCase());
              }
              
              // Track system contract events for RVM and callbacks
              await trackSystemContractEvents(block.number);
              
              // Update holder balances for active addresses
              if (holderTracker && activeAddresses.length > 0) {
                const uniqueAddresses = [...new Set(activeAddresses)];
                holderTracker.quickUpdateActiveAddresses(uniqueAddresses).catch(console.error);
              }
              
              // Update last processed block
              db.run('INSERT OR REPLACE INTO metadata (key, value) VALUES (?, ?)', 
                ['last_processed_block', lastProcessedBlock.toString()]);
              
              // Get updated totals
              db.get(
                `SELECT COUNT(*) as count, SUM(CAST(amount AS REAL)) as total FROM burns`,
                (err, row) => {
                  if (!err && row) {
                    // Broadcast update to clients including price data
                    broadcast({
                      type: 'block_processed',
                      data: {
                        blockNumber: Number(block.number),
                        transactionCount: block.transactions.length,
                        burnedInBlock: totalBurnedInBlock,
                        totalBurned: row.total || 0,
                        totalTransactions: row.count || 0,
                        currentPrice: currentPrice,
                        priceData: priceData
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

// Historical sync implementation directly in server
async function processHistoricalBatch(startBlock, endBlock) {
  const burns = [];
  
  try {
    // Process blocks one by one for stability
    for (let blockNum = startBlock; blockNum <= endBlock; blockNum++) {
      // Check if sync is paused
      if (syncPaused) {
        return { burns, lastBlock: blockNum - 1 };
      }
      
      try {
        // Add delay to avoid rate limiting
        await new Promise(resolve => setTimeout(resolve, 150));
        
        const block = await web3.eth.getBlock(blockNum, true);
        
        if (block && block.transactions && block.transactions.length > 0) {
          syncStats.blocksWithTx++;
          
          // Process transactions in smaller chunks
          for (let i = 0; i < block.transactions.length; i += 5) {
            const txChunk = block.transactions.slice(i, i + 5);
            
            for (const tx of txChunk) {
              try {
                const receipt = await web3.eth.getTransactionReceipt(tx.hash);
                
                if (receipt && receipt.gasUsed) {
                  const gasUsed = BigInt(receipt.gasUsed);
                  let effectiveGasPrice = BigInt(0);
                  
                  if (receipt.effectiveGasPrice) {
                    effectiveGasPrice = BigInt(receipt.effectiveGasPrice);
                  } else if (tx.gasPrice) {
                    effectiveGasPrice = BigInt(tx.gasPrice);
                  } else if (block.baseFeePerGas) {
                    const baseFee = BigInt(block.baseFeePerGas);
                    const priorityFee = tx.maxPriorityFeePerGas ? BigInt(tx.maxPriorityFeePerGas) : BigInt(0);
                    effectiveGasPrice = baseFee + priorityFee;
                  }
                  
                  const burnedWei = gasUsed * effectiveGasPrice;
                  const burnedReact = parseFloat(web3.utils.fromWei(burnedWei.toString(), 'ether'));
                  
                  if (burnedReact > 0) {
                    let burnType = 'gas_fee';
                    if (tx.to && tx.to.toLowerCase() === SYSTEM_CONTRACT_ADDRESS.toLowerCase()) {
                      burnType = 'system_contract_payment';
                    } else if (receipt.gasUsed === 21000) {
                      burnType = 'gas_fee_transfer';
                    } else {
                      burnType = 'gas_fee_contract';
                    }
                    
                    // For historical data, we don't have the price at that time
                    // So we use 0 for USD value (could be enhanced with historical price data)
                    burns.push({
                      tx_hash: tx.hash,
                      block_number: Number(block.number),
                      amount: burnedReact.toFixed(18),
                      from_address: tx.from,
                      timestamp: Number(block.timestamp),
                      usd_value: 0,
                      burn_type: burnType,
                      gas_used: Number(receipt.gasUsed)
                    });
                    
                    syncStats.totalBurned += burnedReact;
                  }
                  
                  syncStats.transactionsProcessed++;
                }
              } catch (error) {
                console.error(`Error processing tx ${tx.hash}:`, error.message);
                syncStats.errors++;
              }
            }
            
            // Small delay between chunks
            await new Promise(resolve => setTimeout(resolve, 50));
          }
        } else {
          syncStats.emptyBlocks++;
        }
        
        syncStats.blocksProcessed++;
        syncStats.lastBlock = blockNum;
        
      } catch (error) {
        console.error(`Error fetching block ${blockNum}:`, error.message);
        syncStats.errors++;
        
        // Retry once after a delay
        await new Promise(resolve => setTimeout(resolve, 5000));
        try {
          const block = await web3.eth.getBlock(blockNum, false);
          if (block) {
            syncStats.blocksProcessed++;
            syncStats.lastBlock = blockNum;
          }
        } catch (retryError) {
          console.error(`Retry failed for block ${blockNum}:`, retryError.message);
        }
      }
    }
    
    return { burns, lastBlock: endBlock };
    
  } catch (error) {
    console.error(`Critical error in batch ${startBlock}-${endBlock}:`, error.message);
    syncStats.errors++;
    return { burns, lastBlock: startBlock - 1 };
  }
}

// Main historical sync loop
async function runHistoricalSync() {
  if (!syncStats) return;
  
  try {
    // Get deployment block and checkpoint
    const deploymentBlock = await new Promise((resolve) => {
      db.get('SELECT value FROM metadata WHERE key = ?', ['deployment_block'], (err, row) => {
        resolve(row ? parseInt(row.value) : null);
      });
    });
    
    if (!deploymentBlock) {
      throw new Error('No deployment block found');
    }
    
    const checkpointBlock = await new Promise((resolve) => {
      db.get('SELECT value FROM metadata WHERE key = ?', ['historical_sync_checkpoint'], (err, row) => {
        resolve(row ? parseInt(row.value) : 0);
      });
    });
    
    syncStats.targetBlock = deploymentBlock;
    let currentBlock = checkpointBlock;
    
    console.log(`Starting sync from block ${currentBlock} to ${deploymentBlock}`);
    syncLogs.push({ 
      type: 'stdout', 
      message: `Starting sync from block ${currentBlock} to ${deploymentBlock}`, 
      timestamp: new Date() 
    });
    
    // Process in small batches
    const BATCH_SIZE = 10; // Very small batches for stability
    
    while (currentBlock < deploymentBlock && syncInProgress && !syncPaused) {
      const batchEnd = Math.min(currentBlock + BATCH_SIZE - 1, deploymentBlock);
      
      console.log(`Processing batch: blocks ${currentBlock} to ${batchEnd}`);
      
      const { burns, lastBlock } = await processHistoricalBatch(currentBlock, batchEnd);
      
      // Insert burns in smaller transactions
      if (burns.length > 0) {
        const CHUNK_SIZE = 100;
        for (let i = 0; i < burns.length; i += CHUNK_SIZE) {
          const chunk = burns.slice(i, i + CHUNK_SIZE);
          
          await new Promise((resolve, reject) => {
            db.serialize(() => {
              db.run('BEGIN IMMEDIATE TRANSACTION');
              
              const stmt = db.prepare(`
                INSERT OR IGNORE INTO burns 
                (tx_hash, block_number, amount, from_address, timestamp, usd_value, burn_type, gas_used) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
              `);
              
              for (const burn of chunk) {
                stmt.run([
                  burn.tx_hash,
                  burn.block_number,
                  burn.amount,
                  burn.from_address,
                  burn.timestamp,
                  burn.usd_value,
                  burn.burn_type,
                  burn.gas_used
                ]);
              }
              
              stmt.finalize();
              
              db.run('COMMIT', (err) => {
                if (err) {
                  db.run('ROLLBACK');
                  reject(err);
                } else {
                  resolve();
                }
              });
            });
          });
        }
      }
      
      // Update checkpoint
      currentBlock = lastBlock + 1;
      await new Promise((resolve) => {
        db.run(
          'INSERT OR REPLACE INTO metadata (key, value) VALUES (?, ?)',
          ['historical_sync_checkpoint', lastBlock.toString()],
          resolve
        );
      });
      
      // Update progress
      if (syncStats.blocksProcessed % 1000 === 0 || currentBlock >= deploymentBlock) {
        const elapsed = (Date.now() - syncStats.startTime) / 1000;
        const blocksPerSecond = syncStats.blocksProcessed / elapsed;
        const eta = (deploymentBlock - lastBlock) / blocksPerSecond;
        
        const progressMsg = `Progress: ${lastBlock}/${deploymentBlock} (${(lastBlock/deploymentBlock*100).toFixed(2)}%) - ${blocksPerSecond.toFixed(1)} blocks/sec - ETA: ${(eta/3600).toFixed(1)}h`;
        console.log(progressMsg);
        
        syncLogs.push({ type: 'stdout', message: progressMsg, timestamp: new Date() });
        if (syncLogs.length > 100) syncLogs.shift();
      }
      
      // Small pause between batches
      await new Promise(resolve => setTimeout(resolve, 100));
    }
    
    // Sync completed
    const elapsed = (Date.now() - syncStats.startTime) / 1000;
    const completeMsg = `Sync completed! Processed ${syncStats.blocksProcessed} blocks in ${(elapsed/3600).toFixed(2)} hours`;
    console.log(completeMsg);
    
    syncLogs.push({ 
      type: 'complete', 
      message: completeMsg, 
      duration: elapsed * 1000,
      timestamp: new Date() 
    });
    
  } catch (error) {
    console.error('Fatal error in historical sync:', error);
    syncLogs.push({ type: 'stderr', message: error.message, timestamp: new Date() });
  } finally {
    syncInProgress = false;
    if (syncInterval) {
      clearInterval(syncInterval);
      syncInterval = null;
    }
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

// Get current price info
app.get('/api/price', (req, res) => {
  res.json({
    price: currentPrice,
    ...priceData
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
          COUNT(DISTINCT from_address) as unique_addresses
         FROM burns`,
        (err, totals) => {
          if (err) {
            return res.status(500).json({ error: err.message });
          }
          
          // Calculate total USD value using current price
          const totalUsdValue = (totals.total_burned || 0) * currentPrice;
          
          res.json({
            totalTransactions: totals.total_transactions || 0,
            totalBurned: totals.total_burned || 0,
            totalUsdValue: totalUsdValue,
            uniqueAddresses: totals.unique_addresses || 0,
            currentPrice: currentPrice,
            priceData: priceData,
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
        uniqueAddresses24h: row.unique_addresses_24h || 0,
        usdValue24h: (row.burned_24h || 0) * currentPrice
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
        burnRate: row.hourly_burn || 0,
        burnRateUsd: (row.hourly_burn || 0) * currentPrice
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

// Get address transaction distribution with full analysis
app.get('/api/analysis/distribution', (req, res) => {
  db.all(
    `SELECT from_address, COUNT(*) as tx_count
     FROM burns
     GROUP BY from_address
     ORDER BY tx_count DESC`,
    (err, rows) => {
      if (err) {
        return res.status(500).json({ error: err.message });
      }
      
      // Convert to distribution object
      const distribution = {};
      rows.forEach(row => {
        distribution[row.from_address] = row.tx_count;
      });
      
      // Calculate statistics
      const txCounts = rows.map(r => r.tx_count);
      const totalAddresses = txCounts.length;
      const totalTransactions = txCounts.reduce((sum, count) => sum + count, 0);
      
      // Calculate Gini coefficient
      function calculateGini(values) {
        const sorted = [...values].sort((a, b) => a - b);
        const n = sorted.length;
        const sum = sorted.reduce((a, b) => a + b, 0);
        
        let giniSum = 0;
        for (let i = 0; i < n; i++) {
          giniSum += (2 * (i + 1) - n - 1) * sorted[i];
        }
        
        return giniSum / (n * sum);
      }
      
      const gini = calculateGini(txCounts);
      
      // Calculate percentile metrics
      const sortedCounts = [...txCounts].sort((a, b) => b - a); // Descending order
      
      // Top 10% control
      const top10Index = Math.ceil(totalAddresses * 0.1);
      const top10Sum = sortedCounts.slice(0, top10Index).reduce((sum, count) => sum + count, 0);
      const top10Percent = (top10Sum / totalTransactions) * 100;
      
      // Bottom 50% control
      const bottom50Index = Math.floor(totalAddresses * 0.5);
      const bottom50Sum = sortedCounts.slice(-bottom50Index).reduce((sum, count) => sum + count, 0);
      const bottom50Percent = (bottom50Sum / totalTransactions) * 100;
      
      // Generate insights
      const insights = [];
      
      // Calculate various percentile points
      const percentiles = [1, 5, 10, 20];
      percentiles.forEach(p => {
        const index = Math.ceil(totalAddresses * (p / 100));
        const sum = sortedCounts.slice(0, index).reduce((sum, count) => sum + count, 0);
        const percent = (sum / totalTransactions * 100).toFixed(1);
        insights.push(`Top ${p}% of addresses generate ${percent}% of all transactions`);
      });
      
      // Add concentration insight based on Gini
      if (gini > 0.7) {
        insights.push(`Very high concentration (Gini = ${gini.toFixed(3)}) - activity is dominated by few addresses`);
      } else if (gini > 0.5) {
        insights.push(`Moderate concentration (Gini = ${gini.toFixed(3)}) - typical for many blockchain networks`);
      } else {
        insights.push(`Low concentration (Gini = ${gini.toFixed(3)}) - activity is well distributed across addresses`);
      }
      
      // Add insight about whales
      const whaleThreshold = 1000; // Addresses with 1000+ transactions
      const whales = sortedCounts.filter(count => count >= whaleThreshold).length;
      if (whales > 0) {
        const whaleTx = sortedCounts.filter(count => count >= whaleThreshold).reduce((sum, count) => sum + count, 0);
        const whalePercent = (whaleTx / totalTransactions * 100).toFixed(1);
        insights.push(`${whales} whale addresses (${whaleThreshold}+ transactions) account for ${whalePercent}% of all activity`);
      }
      
      res.json({
        distribution: distribution,
        summary: {
          totalAddresses: totalAddresses,
          totalTransactions: totalTransactions,
          gini: gini,
          top10Percent: top10Percent,
          bottom50Percent: bottom50Percent,
          avgTransactionsPerAddress: (totalTransactions / totalAddresses).toFixed(2),
          medianTransactions: sortedCounts[Math.floor(totalAddresses / 2)] || 0
        },
        insights: insights
      });
    }
  );
});

// Get count of addresses with 100+ transactions
app.get('/api/stats/active-addresses', (req, res) => {
  const threshold = parseInt(req.query.threshold) || 100;
  
  db.get(
    `SELECT COUNT(*) as active_addresses
     FROM (
       SELECT from_address, COUNT(*) as tx_count
       FROM burns
       GROUP BY from_address
       HAVING tx_count >= ?
     )`,
    [threshold],
    (err, row) => {
      if (err) {
        return res.status(500).json({ error: err.message });
      }
      
      // Also get some additional interesting stats
      db.all(
        `SELECT from_address, COUNT(*) as tx_count, SUM(CAST(amount AS REAL)) as total_burned
         FROM burns
         GROUP BY from_address
         HAVING tx_count >= ?
         ORDER BY tx_count DESC
         LIMIT 10`,
        [threshold],
        (err, topAddresses) => {
          if (err) {
            return res.status(500).json({ error: err.message });
          }
          
          res.json({
            activeAddresses: row.active_addresses || 0,
            threshold: threshold,
            topActiveAddresses: topAddresses || []
          });
        }
      );
    }
  );
});

// POWER USERS ENDPOINTS

// Get detailed power users analysis
app.get('/api/power-users/detailed', async (req, res) => {
  const threshold = parseInt(req.query.threshold) || 100;
  const sortBy = req.query.sort || 'tx_count';
  const filter = req.query.filter || 'all';
  
  try {
    // First get the power users with aggregated data
    const powerUsersQuery = `
      SELECT 
        from_address as address,
        COUNT(*) as tx_count,
        SUM(CAST(amount AS REAL)) as total_burned,
        AVG(CAST(amount AS REAL)) as avg_burn_per_tx,
        MIN(timestamp) as first_seen,
        MAX(timestamp) as last_seen,
        COUNT(DISTINCT DATE(timestamp, 'unixepoch')) as active_days,
        GROUP_CONCAT(DISTINCT burn_type) as burn_types,
        SUM(CASE WHEN burn_type = 'gas_fee_transfer' THEN 1 ELSE 0 END) as transfer_count,
        SUM(CASE WHEN burn_type = 'gas_fee_contract' THEN 1 ELSE 0 END) as contract_count,
        SUM(CASE WHEN burn_type = 'system_contract_payment' THEN 1 ELSE 0 END) as system_contract_count,
        SUM(CASE WHEN burn_type LIKE '%cross_chain%' THEN 1 ELSE 0 END) as cross_chain_count,
        SUM(gas_used) as total_gas_used,
        AVG(gas_used) as avg_gas_used
      FROM burns
      GROUP BY from_address
      HAVING tx_count >= ?
    `;
    
    db.all(powerUsersQuery, [threshold], async (err, users) => {
      if (err) {
        return res.status(500).json({ error: err.message });
      }
      
      // Enhance user data with additional analysis
      const enhancedUsers = await Promise.all(users.map(async (user) => {
        // Calculate transaction frequency (average time between transactions)
        const timeSpan = user.last_seen - user.first_seen;
        const txFrequency = timeSpan > 0 ? timeSpan / user.tx_count : 0;
        
        // Get transaction type breakdown
        const typeBreakdown = {};
        
        // Get detailed type counts
        await new Promise((resolve) => {
          db.all(
            `SELECT burn_type, COUNT(*) as count 
             FROM burns 
             WHERE from_address = ? 
             GROUP BY burn_type`,
            [user.address],
            (err, typeRows) => {
              if (!err && typeRows) {
                typeRows.forEach(row => {
                  typeBreakdown[row.burn_type] = row.count;
                });
              }
              resolve();
            }
          );
        });
        
        // Get hourly activity pattern
        const hourlyPattern = new Array(24).fill(0);
        await new Promise((resolve) => {
          db.all(
            `SELECT strftime('%H', datetime(timestamp, 'unixepoch')) as hour, COUNT(*) as count
             FROM burns
             WHERE from_address = ?
             GROUP BY hour`,
            [user.address],
            (err, hourRows) => {
              if (!err && hourRows) {
                hourRows.forEach(row => {
                  hourlyPattern[parseInt(row.hour)] = row.count;
                });
              }
              resolve();
            }
          );
        });
        
        // Check known addresses
        const knownInfo = KNOWN_ADDRESSES[user.address.toLowerCase()];
        
        // Analyze behavior patterns
        const contractRatio = user.contract_count / user.tx_count;
        const isAutomated = txFrequency < 3600 && user.tx_count > 500;
        const isDexTrader = contractRatio > 0.5;
        const isCrossChainUser = user.cross_chain_count > 0;
        const isRscUser = user.system_contract_count > user.tx_count * 0.3;
        
        return {
          ...user,
          tx_frequency: txFrequency,
          type_breakdown: typeBreakdown,
          hourly_pattern: hourlyPattern,
          known_label: knownInfo?.label || null,
          known_type: knownInfo?.type || null,
          contract_interactions: user.contract_count + user.system_contract_count,
          behaviors: {
            automated: isAutomated,
            dex_trader: isDexTrader,
            cross_chain: isCrossChainUser,
            rsc_user: isRscUser
          }
        };
      }));
      
      // Apply filtering
      let filteredUsers = enhancedUsers;
      if (filter !== 'all') {
        filteredUsers = enhancedUsers.filter(user => {
          switch(filter) {
            case 'automated':
              return user.behaviors.automated;
            case 'dex':
              return user.behaviors.dex_trader;
            case 'cross-chain':
              return user.behaviors.cross_chain;
            case 'contracts':
              return user.contract_interactions > user.tx_count * 0.5;
            default:
              return true;
          }
        });
      }
      
      // Apply sorting
      filteredUsers.sort((a, b) => {
        switch(sortBy) {
          case 'total_burned':
            return b.total_burned - a.total_burned;
          case 'avg_burn':
            return b.avg_burn_per_tx - a.avg_burn_per_tx;
          case 'first_seen':
            return a.first_seen - b.first_seen;
          case 'last_seen':
            return b.last_seen - a.last_seen;
          case 'tx_count':
          default:
            return b.tx_count - a.tx_count;
        }
      });
      
      // Calculate summary statistics
      const totalPowerUsers = filteredUsers.length;
      const totalBurnedByPowerUsers = filteredUsers.reduce((sum, user) => sum + user.total_burned, 0);
      const totalTxByPowerUsers = filteredUsers.reduce((sum, user) => sum + user.tx_count, 0);
      
      // Get network totals for comparison
      const networkTotals = await new Promise((resolve) => {
        db.get(
          `SELECT 
            COUNT(*) as total_transactions,
            SUM(CAST(amount AS REAL)) as total_burned
           FROM burns`,
          (err, row) => {
            resolve(row || { total_transactions: 0, total_burned: 0 });
          }
        );
      });
      
      const percentOfTotalActivity = networkTotals.total_transactions > 0 
        ? (totalTxByPowerUsers / networkTotals.total_transactions) * 100 
        : 0;
        
      const avgTransactionsPerPowerUser = totalPowerUsers > 0 
        ? totalTxByPowerUsers / totalPowerUsers 
        : 0;
      
      res.json({
        users: filteredUsers,
        summary: {
          totalPowerUsers: totalPowerUsers,
          totalBurnedByPowerUsers: totalBurnedByPowerUsers,
          percentOfTotalActivity: percentOfTotalActivity,
          avgTransactionsPerPowerUser: avgTransactionsPerPowerUser,
          threshold: threshold,
          filter: filter,
          sortBy: sortBy
        }
      });
    });
  } catch (error) {
    console.error('Error in power users analysis:', error);
    res.status(500).json({ error: 'Failed to analyze power users' });
  }
});

// Get specific user's recent transactions
app.get('/api/power-users/:address/transactions', (req, res) => {
  const address = req.params.address;
  const limit = parseInt(req.query.limit) || 10;
  
  db.all(
    `SELECT tx_hash, block_number, amount, timestamp, burn_type, gas_used
     FROM burns
     WHERE from_address = ?
     ORDER BY timestamp DESC
     LIMIT ?`,
    [address, limit],
    (err, rows) => {
      if (err) {
        return res.status(500).json({ error: err.message });
      }
      res.json(rows);
    }
  );
});

// Get power users behavior analysis
app.get('/api/power-users/behaviors', async (req, res) => {
  try {
    // Analyze different behavior patterns
    const behaviors = await new Promise((resolve) => {
      db.all(
        `SELECT 
          from_address,
          COUNT(*) as tx_count,
          SUM(CAST(amount AS REAL)) as total_burned,
          MIN(timestamp) as first_seen,
          MAX(timestamp) as last_seen,
          COUNT(DISTINCT DATE(timestamp, 'unixepoch')) as active_days,
          SUM(CASE WHEN burn_type LIKE '%cross_chain%' THEN 1 ELSE 0 END) as cross_chain_count,
          SUM(CASE WHEN burn_type = 'system_contract_payment' THEN 1 ELSE 0 END) as rsc_count,
          SUM(CASE WHEN burn_type = 'gas_fee_contract' THEN 1 ELSE 0 END) as contract_count
        FROM burns
        GROUP BY from_address
        HAVING tx_count >= 100`,
        (err, rows) => {
          if (err) {
            resolve([]);
            return;
          }
          
          const analysis = {
            bots: [],
            dexTraders: [],
            crossChainUsers: [],
            rscUsers: [],
            whales: []
          };
          
          rows.forEach(user => {
            const timeSpan = user.last_seen - user.first_seen;
            const avgTimeBetweenTx = timeSpan / user.tx_count;
            
            // Bot detection
            if (avgTimeBetweenTx < 3600 && user.tx_count > 500) {
              analysis.bots.push(user.from_address);
            }
            
            // DEX trader detection
            if (user.contract_count > user.tx_count * 0.5) {
              analysis.dexTraders.push(user.from_address);
            }
            
            // Cross-chain user detection
            if (user.cross_chain_count > 0) {
              analysis.crossChainUsers.push(user.from_address);
            }
            
            // RSC heavy user detection
            if (user.rsc_count > user.tx_count * 0.3) {
              analysis.rscUsers.push(user.from_address);
            }
            
            // Whale detection
            if (user.total_burned > 100) {
              analysis.whales.push(user.from_address);
            }
          });
          
          resolve(analysis);
        }
      );
    });
    
    res.json(behaviors);
  } catch (error) {
    console.error('Error analyzing behaviors:', error);
    res.status(500).json({ error: 'Failed to analyze behaviors' });
  }
});

// === HOLDER API ENDPOINTS ===

// Get holder overview statistics
app.get('/api/holders/overview', async (req, res) => {
  try {
    // Get basic statistics
    const stats = await new Promise((resolve, reject) => {
      db.get(`
        SELECT 
          COUNT(*) as total_holders,
          COUNT(CASE WHEN balance_numeric > 0 THEN 1 END) as active_holders,
          SUM(balance_numeric) as total_supply,
          AVG(balance_numeric) as avg_balance,
          MAX(balance_numeric) as max_balance,
          COUNT(CASE WHEN is_contract = 1 THEN 1 END) as contract_holders,
          COUNT(CASE WHEN is_contract = 0 THEN 1 END) as eoa_holders
        FROM holders
        WHERE balance_numeric > 0.000001
      `, (err, row) => {
        if (err) reject(err);
        else resolve(row);
      });
    });

    // Get distribution metrics
    const distribution = await new Promise((resolve, reject) => {
      db.get(`
        SELECT 
          SUM(CASE WHEN balance_numeric >= 0.01 AND balance_numeric < 1 THEN 1 ELSE 0 END) as micro_holders,
          SUM(CASE WHEN balance_numeric >= 1 AND balance_numeric < 10 THEN 1 ELSE 0 END) as small_holders,
          SUM(CASE WHEN balance_numeric >= 10 AND balance_numeric < 100 THEN 1 ELSE 0 END) as medium_holders,
          SUM(CASE WHEN balance_numeric >= 100 AND balance_numeric < 1000 THEN 1 ELSE 0 END) as large_holders,
          SUM(CASE WHEN balance_numeric >= 1000 AND balance_numeric < 10000 THEN 1 ELSE 0 END) as xl_holders,
          SUM(CASE WHEN balance_numeric >= 10000 THEN 1 ELSE 0 END) as whale_holders
        FROM holders
        WHERE balance_numeric > 0
      `, (err, row) => {
        if (err) reject(err);
        else resolve(row);
      });
    });

    // Get top 10 balance concentration
    const topHolders = await new Promise((resolve, reject) => {
      db.all(
        'SELECT balance_numeric FROM holders ORDER BY balance_numeric DESC LIMIT 100',
        (err, rows) => {
          if (err) reject(err);
          else resolve(rows);
        }
      );
    });

    let top10Balance = 0, top20Balance = 0, top50Balance = 0, top100Balance = 0;
    topHolders.forEach((holder, index) => {
      if (index < 10) top10Balance += holder.balance_numeric;
      if (index < 20) top20Balance += holder.balance_numeric;
      if (index < 50) top50Balance += holder.balance_numeric;
      if (index < 100) top100Balance += holder.balance_numeric;
    });

    const totalSupply = stats.total_supply || 1; // Avoid division by zero

    res.json({
      overview: {
        totalHolders: stats.total_holders || 0,
        activeHolders: stats.active_holders || 0,
        totalSupply: totalSupply,
        avgBalance: stats.avg_balance || 0,
        maxBalance: stats.max_balance || 0,
        contractHolders: stats.contract_holders || 0,
        eoaHolders: stats.eoa_holders || 0
      },
      distribution: {
        ranges: {
          '0.01-1': distribution.micro_holders || 0,
          '1-10': distribution.small_holders || 0,
          '10-100': distribution.medium_holders || 0,
          '100-1K': distribution.large_holders || 0,
          '1K-10K': distribution.xl_holders || 0,
          '10K+': distribution.whale_holders || 0
        },
        concentration: {
          top10: (top10Balance / totalSupply * 100).toFixed(2),
          top20: (top20Balance / totalSupply * 100).toFixed(2),
          top50: (top50Balance / totalSupply * 100).toFixed(2),
          top100: (top100Balance / totalSupply * 100).toFixed(2)
        }
      }
    });
  } catch (error) {
    console.error('Error getting holder overview:', error);
    res.status(500).json({ error: 'Failed to get holder overview' });
  }
});

// Get top holders (rich list)
app.get('/api/holders/top', async (req, res) => {
  const limit = Math.min(parseInt(req.query.limit) || 100, 500);
  const offset = parseInt(req.query.offset) || 0;
  const excludeContracts = req.query.excludeContracts === 'true';
  
  try {
    const whereClause = excludeContracts 
      ? 'WHERE balance_numeric > 0 AND is_contract = 0' 
      : 'WHERE balance_numeric > 0';
    
    const holders = await new Promise((resolve, reject) => {
      db.all(`
        SELECT 
          h.address,
          h.balance,
          h.balance_numeric,
          h.is_contract,
          h.tx_count,
          h.first_seen_timestamp,
          h.last_updated_timestamp,
          ka.label,
          ka.category
        FROM holders h
        LEFT JOIN known_addresses ka ON h.address = ka.address
        ${whereClause}
        ORDER BY h.balance_numeric DESC
        LIMIT ? OFFSET ?
      `, [limit, offset], (err, rows) => {
        if (err) reject(err);
        else resolve(rows);
      });
    });

    // Get total supply for percentage calculation
    const totalSupply = await new Promise((resolve, reject) => {
      db.get(
        'SELECT SUM(balance_numeric) as total FROM holders WHERE balance_numeric > 0',
        (err, row) => {
          if (err) reject(err);
          else resolve(row.total || 0);
        }
      );
    });

    // Calculate rank and percentage for each holder
    const enrichedHolders = holders.map((holder, index) => ({
      rank: offset + index + 1,
      address: holder.address,
      balance: holder.balance,
      balance_numeric: holder.balance_numeric,
      percentage: ((holder.balance_numeric / totalSupply) * 100).toFixed(4),
      is_contract: holder.is_contract === 1,
      tx_count: holder.tx_count,
      label: holder.label,
      category: holder.category,
      first_seen: holder.first_seen_timestamp,
      last_updated: holder.last_updated_timestamp
    }));

    res.json({
      holders: enrichedHolders,
      totalSupply: totalSupply,
      hasMore: holders.length === limit
    });
  } catch (error) {
    console.error('Error getting top holders:', error);
    res.status(500).json({ error: 'Failed to get top holders' });
  }
});

// Get holder details by address
app.get('/api/holders/:address', async (req, res) => {
  const address = req.params.address.toLowerCase();
  
  try {
    // Get holder info
    const holder = await new Promise((resolve, reject) => {
      db.get(`
        SELECT 
          h.*,
          ka.label,
          ka.category,
          ka.description
        FROM holders h
        LEFT JOIN known_addresses ka ON h.address = ka.address
        WHERE h.address = ?
      `, [address], (err, row) => {
        if (err) reject(err);
        else resolve(row);
      });
    });

    if (!holder) {
      return res.status(404).json({ error: 'Address not found' });
    }

    // Get rank
    const rank = await new Promise((resolve, reject) => {
      db.get(
        'SELECT COUNT(*) + 1 as rank FROM holders WHERE balance_numeric > ?',
        [holder.balance_numeric],
        (err, row) => {
          if (err) reject(err);
          else resolve(row.rank);
        }
      );
    });

    // Get recent transactions
    const recentTxs = await new Promise((resolve, reject) => {
      db.all(`
        SELECT 
          tx_hash,
          block_number,
          amount,
          timestamp,
          burn_type
        FROM burns
        WHERE from_address = ?
        ORDER BY timestamp DESC
        LIMIT 20
      `, [address], (err, rows) => {
        if (err) reject(err);
        else resolve(rows);
      });
    });

    // Get transaction statistics
    const txStats = await new Promise((resolve, reject) => {
      db.get(`
        SELECT 
          COUNT(*) as total_txs,
          SUM(CAST(amount AS REAL)) as total_burned,
          AVG(CAST(amount AS REAL)) as avg_burn,
          MIN(timestamp) as first_tx,
          MAX(timestamp) as last_tx
        FROM burns
        WHERE from_address = ?
      `, [address], (err, row) => {
        if (err) reject(err);
        else resolve(row);
      });
    });

    res.json({
      address: holder.address,
      balance: holder.balance,
      balance_numeric: holder.balance_numeric,
      rank: rank,
      is_contract: holder.is_contract === 1,
      label: holder.label,
      category: holder.category,
      description: holder.description,
      first_seen: holder.first_seen_timestamp,
      last_updated: holder.last_updated_timestamp,
      transactions: {
        count: txStats.total_txs || 0,
        total_burned: txStats.total_burned || 0,
        avg_burn: txStats.avg_burn || 0,
        first_tx: txStats.first_tx,
        last_tx: txStats.last_tx,
        recent: recentTxs
      }
    });
  } catch (error) {
    console.error('Error getting holder details:', error);
    res.status(500).json({ error: 'Failed to get holder details' });
  }
});

// Get holder growth history
app.get('/api/holders/history', async (req, res) => {
  const days = parseInt(req.query.days) || 30;
  const since = Math.floor(Date.now() / 1000) - (days * 24 * 60 * 60);
  
  try {
    const history = await new Promise((resolve, reject) => {
      db.all(`
        SELECT 
          timestamp,
          block_number,
          total_holders,
          holders_above_1,
          holders_above_10,
          holders_above_100,
          holders_above_1000,
          holders_above_10000,
          top_10_balance,
          top_50_balance,
          top_100_balance,
          total_supply
        FROM holder_history
        WHERE timestamp > ?
        ORDER BY timestamp ASC
      `, [since], (err, rows) => {
        if (err) reject(err);
        else resolve(rows);
      });
    });

    res.json(history);
  } catch (error) {
    console.error('Error getting holder history:', error);
    res.status(500).json({ error: 'Failed to get holder history' });
  }
});

// Search holders by address or label
app.get('/api/holders/search', async (req, res) => {
  const query = req.query.q;
  if (!query || query.length < 3) {
    return res.status(400).json({ error: 'Query must be at least 3 characters' });
  }

  try {
    const results = await new Promise((resolve, reject) => {
      db.all(`
        SELECT 
          h.address,
          h.balance,
          h.balance_numeric,
          h.is_contract,
          ka.label,
          ka.category
        FROM holders h
        LEFT JOIN known_addresses ka ON h.address = ka.address
        WHERE h.address LIKE ? OR ka.label LIKE ?
        ORDER BY h.balance_numeric DESC
        LIMIT 20
      `, [`%${query}%`, `%${query}%`], (err, rows) => {
        if (err) reject(err);
        else resolve(rows);
      });
    });

    res.json(results);
  } catch (error) {
    console.error('Error searching holders:', error);
    res.status(500).json({ error: 'Failed to search holders' });
  }
});

// Admin endpoint to trigger holder scan
app.post('/api/admin/scan-holders', async (req, res) => {
  const adminToken = req.headers['x-admin-token'];
  if (adminToken !== process.env.ADMIN_TOKEN) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  const fromBlock = parseInt(req.body.fromBlock) || 0;
  const toBlock = req.body.toBlock || 'latest';

  if (!holderTracker) {
    return res.status(500).json({ error: 'Holder tracker not initialized' });
  }

  // Run scan in background
  holderTracker.scanForHolders(fromBlock, toBlock)
    .then(count => {
      console.log(`Holder scan completed, found ${count} addresses`);
    })
    .catch(error => {
      console.error('Holder scan failed:', error);
    });

  res.json({ 
    message: 'Holder scan started',
    fromBlock,
    toBlock
  });
});

// Admin endpoint to update specific addresses
app.post('/api/admin/update-holders', async (req, res) => {
  const adminToken = req.headers['x-admin-token'];
  if (adminToken !== process.env.ADMIN_TOKEN) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  const addresses = req.body.addresses;
  if (!addresses || !Array.isArray(addresses)) {
    return res.status(400).json({ error: 'Invalid addresses array' });
  }

  if (!holderTracker) {
    return res.status(500).json({ error: 'Holder tracker not initialized' });
  }

  try {
    const count = await holderTracker.updateBalancesForAddresses(addresses, 'latest');
    res.json({ 
      message: 'Holder balances updated',
      updated: count
    });
  } catch (error) {
    console.error('Error updating holders:', error);
    res.status(500).json({ error: 'Failed to update holders' });
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

  // Initialize sync stats
  syncStats = {
    startTime: Date.now(),
    blocksProcessed: 0,
    transactionsProcessed: 0,
    totalBurned: 0,
    errors: 0,
    lastBlock: 0,
    emptyBlocks: 0,
    blocksWithTx: 0,
    targetBlock: 0
  };

  // Start the sync process
  syncInProgress = true;
  syncStartTime = Date.now();
  syncLogs = [];
  syncPaused = false;

  // Run sync in background
  runHistoricalSync().catch(error => {
    console.error('Historical sync failed:', error);
    syncLogs.push({ type: 'stderr', message: `Fatal error: ${error.message}`, timestamp: new Date() });
    syncInProgress = false;
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

  if (syncInProgress && syncStats) {
    const elapsed = (Date.now() - syncStats.startTime) / 1000;
    const blocksPerSecond = syncStats.blocksProcessed / elapsed || 0;
    
    res.json({
      status: 'running',
      startTime: syncStartTime,
      duration: Date.now() - syncStartTime,
      stats: {
        blocksProcessed: syncStats.blocksProcessed,
        currentBlock: syncStats.lastBlock,
        targetBlock: syncStats.targetBlock,
        progress: syncStats.targetBlock > 0 ? (syncStats.lastBlock / syncStats.targetBlock * 100).toFixed(2) + '%' : '0%',
        blocksPerSecond: blocksPerSecond.toFixed(1),
        transactionsProcessed: syncStats.transactionsProcessed,
        totalBurned: syncStats.totalBurned.toFixed(4),
        errors: syncStats.errors
      },
      recentLogs: syncLogs.slice(-20)
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

  if (!syncInProgress) {
    return res.status(400).json({ error: 'No sync in progress' });
  }

  syncPaused = true;
  syncInProgress = false;
  
  // Save current progress
  if (syncStats && syncStats.lastBlock > 0) {
    db.run(
      'INSERT OR REPLACE INTO metadata (key, value) VALUES (?, ?)',
      ['historical_sync_checkpoint', syncStats.lastBlock.toString()]
    );
  }
  
  res.json({ message: 'Sync process stopped' });
});

// Health check
app.get('/api/health', (req, res) => {
  res.json({ 
    status: 'ok', 
    timestamp: new Date().toISOString(),
    websocket: wss.clients.size + ' clients connected',
    price: {
      current: currentPrice,
      lastUpdated: priceData.lastUpdated
    }
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
    
    // Initialize holder tracker
    initializeHolderTracker();
    
    pollBlocks();
  });
});

// Graceful shutdown
process.on('SIGTERM', () => {
  console.log('SIGTERM received, closing server...');
  
  // Stop sync if running
  if (syncInProgress) {
    syncPaused = true;
    syncInProgress = false;
    
    // Save checkpoint
    if (syncStats && syncStats.lastBlock > 0) {
      db.run(
        'INSERT OR REPLACE INTO metadata (key, value) VALUES (?, ?)',
        ['historical_sync_checkpoint', syncStats.lastBlock.toString()],
        () => {
          db.close();
          wss.close();
          server.close();
          process.exit(0);
        }
      );
    } else {
      db.close();
      wss.close();
      server.close();
      process.exit(0);
    }
  } else {
    db.close();
    wss.close();
    server.close();
    process.exit(0);
  }
});
