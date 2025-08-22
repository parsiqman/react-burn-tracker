// server-cleaned.js - Streamlined REACT Burn Tracker Backend
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
  ? '/app/data/burns.db'
  : './burns.db';

const db = new sqlite3.Database(dbPath);
console.log(`Using database at: ${dbPath}`);

// Database optimizations
db.run("PRAGMA journal_mode = WAL");
db.run("PRAGMA busy_timeout = 30000");
db.run("PRAGMA cache_size = 10000");
db.run("PRAGMA synchronous = NORMAL");

// Global variables
let holderTracker = null;

// Important addresses
const REACT_TOKEN_ADDRESS = process.env.REACT_TOKEN_ADDRESS || '0x0000000000000000000000000000000000fffFfF';
const SYSTEM_CONTRACT_ADDRESS = '0x0000000000000000000000000000000000fffFfF';

// Ecosystem Companies Registry
const ECOSYSTEM_COMPANIES = {
  'reactive': {
    id: 'reactive',
    name: 'Reactive Network',
    type: 'core',
    address: '0x0000000000000000000000000000000000fffFfF',
    description: 'Core Network Protocol',
    category: 'infrastructure',
    status: 'active'
  },
  'qstn': {
    id: 'qstn', 
    name: 'QSTN',
    type: 'platform',
    address: '0x05f5e6b7972275535a5A99A85b7e85821E752CF1',
    description: 'Web3 Survey & Quest Platform',
    category: 'platform',
    status: 'active'
  },
  'world-of-rogues': {
    id: 'world-of-rogues',
    name: 'World of Rogues',
    type: 'gaming',
    address: '0xC5Bd3532198e2D561f817c22524D1F6f10415bc2',
    description: 'On-chain Gaming Platform',
    category: 'gaming',
    status: 'active'
  }
};

// Ecosystem metrics
let ecosystemMetrics = {
  totalTransactions: 0,
  totalVolume: 0,
  dailyVolume: 0,
  avgGasUsed: 0,
  lastUpdated: null,
  companyMetrics: {}
};

// Initialize company metrics
Object.keys(ECOSYSTEM_COMPANIES).forEach(companyId => {
  ecosystemMetrics.companyMetrics[companyId] = {
    transactions: 0,
    volume: '0',
    gasUsed: 0,
    lastActivity: null
  };
});

// Known addresses for labeling
const KNOWN_ADDRESSES = {
  '0xaa24633108fd1d87371c55e6d7f4fa00cdeb26': {
    label: 'RSC Automation Bot',
    type: 'bot',
    description: 'Reactive Smart Contract automation service'
  }
};

// Price tracking
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

// Update token price from CoinGecko
async function updateTokenPrice() {
  try {
    console.log('Fetching REACT price from CoinGecko...');
    
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
      
      console.log(`REACT price updated: $${currentPrice}`);
    }
    
    priceData.lastUpdated = new Date().toISOString();
    
  } catch (error) {
    console.error('Error updating REACT price:', error.message);
  }
}

// Update price on startup and periodically
updateTokenPrice();
setInterval(updateTokenPrice, 60 * 1000);

// Create HTTP server
const server = require('http').createServer(app);

// Initialize WebSocket server
const wss = new WebSocket.Server({ server });

wss.on('connection', (ws) => {
  console.log('New WebSocket client connected');
  
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

// Initialize database tables
function initializeDatabase(callback) {
  db.serialize(() => {
    // Create all necessary tables
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

    db.run(`
      CREATE TABLE IF NOT EXISTS metadata (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
      )
    `);

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

    db.run(`
      CREATE TABLE IF NOT EXISTS known_addresses (
        address TEXT PRIMARY KEY,
        label TEXT NOT NULL,
        category TEXT,
        description TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
      )
    `);

    db.run(`
      CREATE TABLE IF NOT EXISTS ecosystem_activity (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        company_id TEXT NOT NULL,
        address TEXT NOT NULL,
        tx_hash TEXT NOT NULL,
        block_number INTEGER NOT NULL,
        amount TEXT,
        gas_used INTEGER,
        activity_type TEXT,
        from_address TEXT,
        to_address TEXT,
        timestamp INTEGER NOT NULL,
        metadata TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
      )
    `);

    db.run(`
      CREATE TABLE IF NOT EXISTS ecosystem_metrics (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        company_id TEXT NOT NULL,
        date DATE NOT NULL,
        transaction_count INTEGER DEFAULT 0,
        volume REAL DEFAULT 0,
        unique_users INTEGER DEFAULT 0,
        gas_used INTEGER DEFAULT 0,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(company_id, date)
      )
    `);

    // Create indexes
    db.run('CREATE INDEX IF NOT EXISTS idx_timestamp ON burns(timestamp)');
    db.run('CREATE INDEX IF NOT EXISTS idx_block ON burns(block_number)');
    db.run('CREATE INDEX IF NOT EXISTS idx_burn_type ON burns(burn_type)');
    db.run('CREATE INDEX IF NOT EXISTS idx_from_address ON burns(from_address)');
    db.run('CREATE INDEX IF NOT EXISTS idx_balance_numeric ON holders(balance_numeric DESC)');
    db.run('CREATE INDEX IF NOT EXISTS idx_ecosystem_company ON ecosystem_activity(company_id)');
    db.run('CREATE INDEX IF NOT EXISTS idx_ecosystem_timestamp ON ecosystem_activity(timestamp)', callback);
  });
}

// Initialize holder tracker
function initializeHolderTracker() {
  holderTracker = new HolderTracker(db, web3);
  holderTracker.initialize().then(() => {
    console.log('Holder tracker initialized');
  }).catch(error => {
    console.error('Error initializing holder tracker:', error);
  });
}

// Track ecosystem activity
async function trackEcosystemActivity(tx, block, receipt) {
  try {
    const toAddress = tx.to ? tx.to.toLowerCase() : '';
    const fromAddress = tx.from ? tx.from.toLowerCase() : '';
    
    for (const [companyId, company] of Object.entries(ECOSYSTEM_COMPANIES)) {
      const companyAddress = company.address.toLowerCase();
      
      if (toAddress === companyAddress || fromAddress === companyAddress) {
        const gasUsed = receipt ? Number(receipt.gasUsed) : 0;
        const value = tx.value ? web3.utils.fromWei(tx.value, 'ether') : '0';
        
        let activityType = 'interaction';
        if (toAddress === companyAddress && fromAddress !== companyAddress) {
          activityType = 'incoming';
        } else if (fromAddress === companyAddress && toAddress !== companyAddress) {
          activityType = 'outgoing';
        }
        
        db.run(`
          INSERT INTO ecosystem_activity 
          (company_id, address, tx_hash, block_number, amount, gas_used, activity_type, from_address, to_address, timestamp)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        `, [
          companyId,
          companyAddress,
          tx.hash,
          Number(block.number),
          value,
          gasUsed,
          activityType,
          fromAddress,
          toAddress,
          Number(block.timestamp)
        ]);
        
        // Update metrics
        ecosystemMetrics.totalTransactions++;
        ecosystemMetrics.totalVolume += parseFloat(value);
        ecosystemMetrics.companyMetrics[companyId].transactions++;
        ecosystemMetrics.companyMetrics[companyId].volume = (parseFloat(ecosystemMetrics.companyMetrics[companyId].volume) + parseFloat(value)).toString();
        
        broadcast({
          type: 'ecosystem_activity',
          data: {
            companyId: companyId,
            companyName: company.name,
            activityType: activityType,
            txHash: tx.hash,
            amount: value,
            timestamp: Number(block.timestamp)
          }
        });
      }
    }
  } catch (error) {
    console.error('Error tracking ecosystem activity:', error);
  }
}

// Process transaction for burns
async function processRegularTransaction(tx, block) {
  try {
    const receipt = await web3.eth.getTransactionReceipt(tx.hash);
    
    if (receipt) {
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
      const usdValue = burnedReact * currentPrice;
      
      let transactionType = 'gas_fee';
      
      if (gasUsed === 21000n) {
        transactionType = 'gas_fee_transfer';
      } else if (tx.to && tx.to.toLowerCase() === SYSTEM_CONTRACT_ADDRESS.toLowerCase()) {
        transactionType = 'system_contract_payment';
      } else {
        transactionType = 'gas_fee_contract';
      }
      
      db.run(
        `INSERT OR IGNORE INTO burns (tx_hash, block_number, amount, from_address, timestamp, usd_value, burn_type, gas_used) 
         VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
        [tx.hash, Number(block.number), burnedReact.toFixed(18), tx.from, Number(block.timestamp), usdValue, transactionType, Number(receipt.gasUsed)]
      );
      
      return burnedReact;
    }
    return 0;
  } catch (error) {
    console.error('Error processing transaction:', error);
    return 0;
  }
}

// Poll for new blocks
async function pollBlocks() {
  let lastProcessedBlock = null;
  
  db.get('SELECT value FROM metadata WHERE key = ?', ['last_processed_block'], async (err, row) => {
    if (row) {
      lastProcessedBlock = BigInt(row.value);
    }
    
    setInterval(async () => {
      try {
        const currentBlock = await web3.eth.getBlockNumber();
        
        if (lastProcessedBlock === null) {
          lastProcessedBlock = currentBlock - 1n;
          db.run('INSERT OR REPLACE INTO metadata (key, value) VALUES (?, ?)', 
            ['last_processed_block', lastProcessedBlock.toString()]);
        }
        
        while (lastProcessedBlock < currentBlock) {
          lastProcessedBlock++;
          
          try {
            const block = await web3.eth.getBlock(lastProcessedBlock, true);
            
            if (block && block.transactions && block.transactions.length > 0) {
              let totalBurnedInBlock = 0;
              
              for (const tx of block.transactions) {
                const receipt = await web3.eth.getTransactionReceipt(tx.hash);
                const burned = await processRegularTransaction(tx, block);
                totalBurnedInBlock += burned;
                
                await trackEcosystemActivity(tx, block, receipt);
              }
              
              db.run('INSERT OR REPLACE INTO metadata (key, value) VALUES (?, ?)', 
                ['last_processed_block', lastProcessedBlock.toString()]);
              
              db.get(
                `SELECT COUNT(*) as count, SUM(CAST(amount AS REAL)) as total FROM burns`,
                (err, row) => {
                  if (!err && row) {
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
            lastProcessedBlock--;
            break;
          }
        }
      } catch (error) {
        console.error('Error in block polling:', error);
      }
    }, 3000);
    
    console.log('Started block polling');
  });
}

// Load ecosystem metrics
async function loadEcosystemMetrics() {
  try {
    for (const [companyId, company] of Object.entries(ECOSYSTEM_COMPANIES)) {
      const metrics = await new Promise((resolve, reject) => {
        db.get(`
          SELECT 
            COUNT(*) as transaction_count,
            SUM(CAST(amount AS REAL)) as total_volume,
            SUM(gas_used) as total_gas,
            MAX(timestamp) as last_activity
          FROM ecosystem_activity
          WHERE company_id = ?
        `, [companyId], (err, row) => {
          if (err) reject(err);
          else resolve(row);
        });
      });

      if (metrics) {
        ecosystemMetrics.companyMetrics[companyId] = {
          transactions: metrics.transaction_count || 0,
          volume: (metrics.total_volume || 0).toString(),
          gasUsed: metrics.total_gas || 0,
          lastActivity: metrics.last_activity
        };
        
        ecosystemMetrics.totalTransactions += metrics.transaction_count || 0;
        ecosystemMetrics.totalVolume += metrics.total_volume || 0;
      }
    }

    ecosystemMetrics.lastUpdated = new Date().toISOString();
    console.log('Ecosystem metrics loaded');
  } catch (error) {
    console.error('Error loading ecosystem metrics:', error);
  }
}

// ===== API ENDPOINTS =====

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

// Get total stats
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

// Get 24h stats
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

// Get active addresses count
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
      
      res.json({
        activeAddresses: row.active_addresses || 0,
        threshold: threshold
      });
    }
  );
});

// Get recent transactions
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

// Get distribution analysis
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
      
      const distribution = {};
      rows.forEach(row => {
        distribution[row.from_address] = row.tx_count;
      });
      
      const txCounts = rows.map(r => r.tx_count);
      const totalAddresses = txCounts.length;
      const totalTransactions = txCounts.reduce((sum, count) => sum + count, 0);
      
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
      const sortedCounts = [...txCounts].sort((a, b) => b - a);
      
      const top10Index = Math.ceil(totalAddresses * 0.1);
      const top10Sum = sortedCounts.slice(0, top10Index).reduce((sum, count) => sum + count, 0);
      const top10Percent = (top10Sum / totalTransactions) * 100;
      
      const bottom50Index = Math.floor(totalAddresses * 0.5);
      const bottom50Sum = sortedCounts.slice(-bottom50Index).reduce((sum, count) => sum + count, 0);
      const bottom50Percent = (bottom50Sum / totalTransactions) * 100;
      
      const insights = [];
      const percentiles = [1, 5, 10, 20];
      percentiles.forEach(p => {
        const index = Math.ceil(totalAddresses * (p / 100));
        const sum = sortedCounts.slice(0, index).reduce((sum, count) => sum + count, 0);
        const percent = (sum / totalTransactions * 100).toFixed(1);
        insights.push(`Top ${p}% of addresses generate ${percent}% of all transactions`);
      });
      
      if (gini > 0.7) {
        insights.push(`Very high concentration (Gini = ${gini.toFixed(3)}) - activity is dominated by few addresses`);
      } else if (gini > 0.5) {
        insights.push(`Moderate concentration (Gini = ${gini.toFixed(3)}) - typical for many blockchain networks`);
      } else {
        insights.push(`Low concentration (Gini = ${gini.toFixed(3)}) - activity is well distributed across addresses`);
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

// === HOLDER ENDPOINTS ===

// Get holder overview
app.get('/api/holders/overview', async (req, res) => {
  try {
    const stats = await new Promise((resolve, reject) => {
      db.get(`
        SELECT 
          COUNT(*) as total_holders,
          COUNT(CASE WHEN balance_numeric > 0 THEN 1 END) as active_holders,
          SUM(balance_numeric) as total_supply,
          AVG(balance_numeric) as avg_balance,
          COUNT(CASE WHEN is_contract = 1 THEN 1 END) as contract_holders
        FROM holders
        WHERE balance_numeric > 0.000001
      `, (err, row) => {
        if (err) reject(err);
        else resolve(row);
      });
    });

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

    const totalSupply = stats.total_supply || 1;

    res.json({
      overview: {
        totalHolders: stats.total_holders || 0,
        activeHolders: stats.active_holders || 0,
        totalSupply: totalSupply,
        avgBalance: stats.avg_balance || 0,
        contractHolders: stats.contract_holders || 0
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

// Get top holders
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
          ka.label
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

    const totalSupply = await new Promise((resolve, reject) => {
      db.get(
        'SELECT SUM(balance_numeric) as total FROM holders WHERE balance_numeric > 0',
        (err, row) => {
          if (err) reject(err);
          else resolve(row.total || 0);
        }
      );
    });

    const enrichedHolders = holders.map((holder, index) => ({
      rank: offset + index + 1,
      address: holder.address,
      balance: holder.balance,
      balance_numeric: holder.balance_numeric,
      percentage: ((holder.balance_numeric / totalSupply) * 100).toFixed(4),
      is_contract: holder.is_contract === 1,
      tx_count: holder.tx_count,
      label: holder.label
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

// Search holders
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
          ka.label
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

// === POWER USERS ENDPOINTS ===

// Get detailed power users
app.get('/api/power-users/detailed', async (req, res) => {
  const threshold = parseInt(req.query.threshold) || 100;
  const sortBy = req.query.sort || 'tx_count';
  const filter = req.query.filter || 'all';
  
  try {
    const powerUsersQuery = `
      SELECT 
        from_address as address,
        COUNT(*) as tx_count,
        SUM(CAST(amount AS REAL)) as total_burned,
        AVG(CAST(amount AS REAL)) as avg_burn_per_tx,
        MIN(timestamp) as first_seen,
        MAX(timestamp) as last_seen,
        SUM(CASE WHEN burn_type = 'gas_fee_transfer' THEN 1 ELSE 0 END) as transfer_count,
        SUM(CASE WHEN burn_type = 'gas_fee_contract' THEN 1 ELSE 0 END) as contract_count,
        SUM(CASE WHEN burn_type = 'system_contract_payment' THEN 1 ELSE 0 END) as system_contract_count,
        SUM(CASE WHEN burn_type LIKE '%cross_chain%' THEN 1 ELSE 0 END) as cross_chain_count
      FROM burns
      GROUP BY from_address
      HAVING tx_count >= ?
    `;
    
    db.all(powerUsersQuery, [threshold], async (err, users) => {
      if (err) {
        return res.status(500).json({ error: err.message });
      }
      
      const enhancedUsers = await Promise.all(users.map(async (user) => {
        const timeSpan = user.last_seen - user.first_seen;
        const txFrequency = timeSpan > 0 ? timeSpan / user.tx_count : 0;
        
        const typeBreakdown = {};
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
        
        const knownInfo = KNOWN_ADDRESSES[user.address.toLowerCase()];
        const contractRatio = user.contract_count / user.tx_count;
        const isAutomated = txFrequency < 3600 && user.tx_count > 500;
        const isDexTrader = contractRatio > 0.5;
        const isCrossChainUser = user.cross_chain_count > 0;
        const isRscUser = user.system_contract_count > user.tx_count * 0.3;
        
        return {
          ...user,
          tx_frequency: txFrequency,
          type_breakdown: typeBreakdown,
          known_label: knownInfo?.label || null,
          contract_interactions: user.contract_count + user.system_contract_count
        };
      }));
      
      let filteredUsers = enhancedUsers;
      if (filter !== 'all') {
        filteredUsers = enhancedUsers.filter(user => {
          switch(filter) {
            case 'automated':
              return user.tx_frequency < 3600 && user.tx_count > 500;
            case 'dex':
              return user.contract_interactions > user.tx_count * 0.5;
            case 'cross-chain':
              return user.cross_chain_count > 0;
            default:
              return true;
          }
        });
      }
      
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
          default:
            return b.tx_count - a.tx_count;
        }
      });
      
      const totalPowerUsers = filteredUsers.length;
      const totalBurnedByPowerUsers = filteredUsers.reduce((sum, user) => sum + user.total_burned, 0);
      const totalTxByPowerUsers = filteredUsers.reduce((sum, user) => sum + user.tx_count, 0);
      
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
          avgTransactionsPerPowerUser: avgTransactionsPerPowerUser
        }
      });
    });
  } catch (error) {
    console.error('Error in power users analysis:', error);
    res.status(500).json({ error: 'Failed to analyze power users' });
  }
});

// Get user's recent transactions
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

// === ECOSYSTEM ENDPOINTS ===

// Get ecosystem overview
app.get('/api/ecosystem/overview', async (req, res) => {
  try {
    await loadEcosystemMetrics();
    
    const avgGas = ecosystemMetrics.totalTransactions > 0 
      ? (ecosystemMetrics.totalTransactions / ecosystemMetrics.totalTransactions).toFixed(4)
      : '0.003';

    res.json({
      totalCompanies: Object.keys(ECOSYSTEM_COMPANIES).length - 1,
      totalTransactions: ecosystemMetrics.totalTransactions,
      totalVolume: ecosystemMetrics.totalVolume.toFixed(2) + ' REACT',
      dailyVolume: ecosystemMetrics.dailyVolume.toFixed(2) + ' REACT',
      avgGasUsed: avgGas + ' REACT',
      lastUpdated: ecosystemMetrics.lastUpdated
    });
  } catch (error) {
    console.error('Error getting ecosystem overview:', error);
    res.status(500).json({ error: 'Failed to get ecosystem overview' });
  }
});

// Get companies list
app.get('/api/ecosystem/companies', (req, res) => {
  const companies = Object.values(ECOSYSTEM_COMPANIES).map(company => ({
    ...company,
    metrics: ecosystemMetrics.companyMetrics[company.id] || {
      transactions: 0,
      volume: '0',
      gasUsed: 0,
      lastActivity: null
    }
  }));

  res.json(companies);
});

// Get company details
app.get('/api/ecosystem/company/:companyId', async (req, res) => {
  const { companyId } = req.params;
  const company = ECOSYSTEM_COMPANIES[companyId];
  
  if (!company) {
    return res.status(404).json({ error: 'Company not found' });
  }

  try {
    const metrics = await new Promise((resolve, reject) => {
      db.get(`
        SELECT 
          COUNT(*) as transaction_count,
          SUM(CAST(amount AS REAL)) as total_volume,
          SUM(gas_used) as total_gas,
          MAX(timestamp) as last_activity,
          COUNT(DISTINCT CASE WHEN activity_type = 'incoming' THEN from_address END) as unique_users
        FROM ecosystem_activity
        WHERE company_id = ?
      `, [companyId], (err, row) => {
        if (err) reject(err);
        else resolve(row);
      });
    });

    const recentActivity = await new Promise((resolve, reject) => {
      db.all(`
        SELECT 
          tx_hash,
          block_number,
          amount,
          activity_type,
          from_address,
          to_address,
          timestamp
        FROM ecosystem_activity
        WHERE company_id = ?
        ORDER BY timestamp DESC
        LIMIT 10
      `, [companyId], (err, rows) => {
        if (err) reject(err);
        else resolve(rows);
      });
    });

    res.json({
      company: company,
      metrics: {
        transactions: metrics.transaction_count || 0,
        volume: (metrics.total_volume || 0).toFixed(2) + ' REACT',
        gasUsed: metrics.total_gas || 0,
        uniqueUsers: metrics.unique_users || 0,
        lastActivity: metrics.last_activity
      },
      recentActivity: recentActivity
    });
  } catch (error) {
    console.error('Error getting company data:', error);
    res.status(500).json({ error: 'Failed to get company data' });
  }
});

// Get ecosystem activity
app.get('/api/ecosystem/activity', async (req, res) => {
  const limit = parseInt(req.query.limit) || 50;
  
  try {
    const activities = await new Promise((resolve, reject) => {
      db.all(`
        SELECT 
          ea.*,
          CASE 
            WHEN ea.company_id = 'reactive' THEN 'Reactive Network'
            WHEN ea.company_id = 'qstn' THEN 'QSTN'
            WHEN ea.company_id = 'world-of-rogues' THEN 'World of Rogues'
          END as company_name
        FROM ecosystem_activity ea
        ORDER BY ea.timestamp DESC
        LIMIT ?
      `, [limit], (err, rows) => {
        if (err) reject(err);
        else resolve(rows);
      });
    });

    res.json(activities);
  } catch (error) {
    console.error('Error getting ecosystem activity:', error);
    res.status(500).json({ error: 'Failed to get ecosystem activity' });
  }
});

// === ADMIN ENDPOINTS ===

// Historical sync endpoints
let syncInProgress = false;
let syncStartTime = null;
let syncLogs = [];

app.post('/api/admin/sync-historical', (req, res) => {
  const adminToken = req.headers['x-admin-token'];
  if (adminToken !== process.env.ADMIN_TOKEN) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  res.json({ 
    message: 'Please use the separate sync script for historical data',
    command: 'node scripts/sync-historical-direct.js'
  });
});

app.get('/api/admin/sync-status', (req, res) => {
  const adminToken = req.headers['x-admin-token'];
  if (adminToken !== process.env.ADMIN_TOKEN) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  res.json({
    status: 'idle',
    message: 'Historical sync runs as a separate process'
  });
});

app.post('/api/admin/sync-stop', (req, res) => {
  const adminToken = req.headers['x-admin-token'];
  if (adminToken !== process.env.ADMIN_TOKEN) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  res.json({ message: 'No sync process to stop' });
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
  
  initializeDatabase(() => {
    console.log('Database initialized');
    
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
    
    loadEcosystemMetrics();
    initializeHolderTracker();
    pollBlocks();
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
