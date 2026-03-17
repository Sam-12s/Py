// ============================================================
// DISTRIBUTED SCRAPER v2 — 200 PROXIES × N CONTEXTS
// ============================================================

const CONTEXTS_PER_PROXY = 2;
const TOTAL_PER_PREFIX = 39304;
const Database = require("better-sqlite3");
const MAX_RUNTIME_MINUTES = 356;
const START_TIME = Date.now();
const END_TIME = START_TIME + MAX_RUNTIME_MINUTES * 60 * 1000;
let STOP_FLAG = false;
let HARD_SHUTDOWN = false;

let globalStats = {
  ok: 0,
  forbidden: 0,
  other: 0,
  done: 0,
  retries_529: 0
};

const { chromium } = require("playwright");

// ============================================================
// PROXY GENERATION — 200 proxies, ports 10001–10200
// ============================================================
const PROXY_USERNAME = "spfsvdt89u";
const PROXY_PASSWORD = "f25gv0_jbagu1ZPLMb";
const PROXY_HOST = "dc.decodo.com";
const START_PORT = 10001;
const END_PORT = 10200;
const TOTAL_PROXIES = END_PORT - START_PORT + 1; // 200

function getProxy(index) {
  const port = START_PORT + index;
  return {
    server: `http://${PROXY_HOST}:${port}`,
    username: PROXY_USERNAME,
    password: PROXY_PASSWORD
  };
}

// ============================================================
// PREFIX GENERATION & SHUFFLE
// ============================================================
const SUFFIX_CHARS = [
  "0","1","2","3","4","5","6","7","8","9",
  "A","B","C","D","E","F","G","H",
  "J","K","L","M","N",
  "P","Q","R","S","T","U","V","W","X","Y","Z"
];

function generateAllPrefixes() {
  const prefixes = [];
  for (const a of SUFFIX_CHARS) {
    for (const b of SUFFIX_CHARS) {
      prefixes.push(`${a}${b}`);
    }
  }
  return prefixes; // 34×34 = 1156
}

function shuffle(array) {
  for (let i = array.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [array[i], array[j]] = [array[j], array[i]];
  }
}

function generateCodes(prefix) {
  const out = [];
  for (const a of SUFFIX_CHARS) {
    for (const b of SUFFIX_CHARS) {
      for (const c of SUFFIX_CHARS) {
        out.push(`${prefix}${a}${b}${c}`);
      }
    }
  }
  return out; // 34^3 = 39304
}

// ============================================================
// UTILITIES
// ============================================================
function generateXHD() {
  const chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
  let result = "";
  for (let i = 0; i < 256; i++) {
    result += chars[Math.floor(Math.random() * chars.length)];
  }
  return result;
}

function randomContextOptions() {
  const userAgents = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/124.0.0.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) Chrome/125.0.0.0",
    "Mozilla/5.0 (X11; Linux x86_64) Firefox/118.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) Safari/604.1"
  ];
  const timezones = ["America/New_York","Europe/London","Africa/Lagos","Europe/Paris","Asia/Singapore"];
  return {
    userAgent: userAgents[Math.floor(Math.random() * userAgents.length)],
    viewport: {
      width: 1100 + Math.floor(Math.random() * 500),
      height: 700 + Math.floor(Math.random() * 400)
    },
    locale: ["en-US", "en-GB", "fr-FR"][Math.floor(Math.random() * 3)],
    timezoneId: timezones[Math.floor(Math.random() * timezones.length)],
    deviceScaleFactor: [1, 1.25, 1.5][Math.floor(Math.random() * 3)],
    colorScheme: Math.random() > 0.5 ? "dark" : "light"
  };
}
class ContextPool {
  constructor(browser, size) {
    this.browser = browser;
    this.size = size;
    this.pool = [];
    this.queue = [];
    this.generation = 0;
  }

  async init() {
    for (let i = 0; i < this.size; i++) {
      const ctx = await this.browser.newContext(randomContextOptions());
      ctx.__poolGen = this.generation;

      this.pool.push(ctx);
      this.queue.push(ctx);
    }
  }

  async acquire() {
    while (this.queue.length === 0 && !HARD_SHUTDOWN) {
      await new Promise(r => setTimeout(r, 10));
    }

    const ctx = this.queue.pop();
    return ctx;
  }

  release(ctx) {
    if (!ctx) return;

    if (ctx.__poolGen === this.generation) {
      this.queue.push(ctx);
    } else {
      ctx.close().catch(()=>{});
    }
  }

  async close() {
    for (const ctx of this.pool) {
      try { await ctx.close(); } catch {}
    }
  }

  async rebuild(browser) {
    this.generation++;

    await this.close();

    this.browser = browser;
    this.pool = [];
    this.queue = [];

    await this.init();
  }
}
const USER_AGENTS = [
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
  "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0"
];

const SEC_CH_UA = [
  '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
  '"Not_A Brand";v="99", "Google Chrome";v="120", "Chromium";v="120"',
  '"Chromium";v="120", "Not_A Brand";v="8", "Google Chrome";v="120"'
];

// ============================================================
// GOOGLE DRIVE UPLOAD
// ============================================================
const { google } = require("googleapis");
const fs = require("fs");

async function uploadDbToDrive() {
  const dbPath = "OUTPUT.db";
  if (!fs.existsSync(dbPath)) return;

  const oauth2Client = new google.auth.OAuth2(
    process.env.GDRIVE_CLIENT_ID,
    process.env.GDRIVE_CLIENT_SECRET
  );
  oauth2Client.setCredentials({ refresh_token: process.env.GDRIVE_REFRESH_TOKEN });

  const drive = google.drive({ version: "v3", auth: oauth2Client });
  const fileMetadata = { name: "OUTPUT.db", parents: ["1GJ13uUpHRvY0uEAZbXbhL4S1YNTjU7NR"] };
  const media = { mimeType: "application/x-sqlite3", body: fs.createReadStream(dbPath) };

  await drive.files.create({ resource: fileMetadata, media, fields: "id" });
  console.log("✅ Database uploaded to My Drive");
}

// ============================================================
// DATABASE WORKER
// ============================================================
const { Worker, isMainThread, parentPort, workerData } = require("worker_threads");

if (!isMainThread && workerData === "DB_WORKER") {
  const db = new Database("OUTPUT.db");
  db.exec(`
    PRAGMA journal_mode = WAL;
    PRAGMA synchronous = NORMAL;
    PRAGMA temp_store = MEMORY;
    PRAGMA cache_size = -50000;

    CREATE TABLE IF NOT EXISTS codes (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      worker_id TEXT,
      label TEXT,
      code TEXT UNIQUE,
      teams TEXT,
      events TEXT,
      score TEXT,
      times TEXT,
      odds TEXT,
      total_odds TEXT,
      last_change TEXT,
      timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
    );
  `);

  const insert = db.prepare(`
    INSERT OR IGNORE INTO codes
    (worker_id,label,code,teams,events,score,times,odds,total_odds,last_change)
    VALUES (?,?,?,?,?,?,?,?,?,?)
  `);

  let buffer = [];
  function flush() {
    if (buffer.length === 0) return;
    const tx = db.transaction(rows => { for (const r of rows) insert.run(...r); });
    tx(buffer);
    buffer = [];
  }

  parentPort.on("message", msg => {
    if (msg === "flush") { flush(); return; }
    buffer.push(msg);
  });

  setInterval(flush, 200);
  console.log("🗄️  DB Worker ready");
  return;
}

// ============================================================
// MAIN THREAD
// ============================================================
let dbWorker;
let dbReadyPromise;

if (isMainThread) {
  dbWorker = new Worker(__filename, { workerData: "DB_WORKER" });

  dbReadyPromise = new Promise((resolve) => {
    dbWorker.on("message", (msg) => {
      if (msg === "ready") {
        resolve();
      }
    });
  });
}

function logCode(label, code, workerId, teams, events, score, times, odds, totalOdds, lastChange) {

  if (!dbWorker) return;

  dbWorker.postMessage([
    workerId,
    label,
    code,
    teams,
    events,
    score,
    times,
    odds,
    totalOdds,
    lastChange
  ]);

  // force immediate flush for valuable results
  dbWorker.postMessage("flush");
}

async function flushDbWorker() {
  if (!dbWorker) return;
  dbWorker.postMessage("flush");
  await new Promise(r => setTimeout(r, 1000));
}

function logStatus(code, status, proxyIndex) {
  let color;
  if (status === 200) { color = "\x1b[32m"; globalStats.ok++; }
  else if (status === 403) { color = "\x1b[31m"; globalStats.forbidden++; }
  else if (status === 529) { color = "\x1b[35m"; globalStats.retries_529++; }
  else { color = "\x1b[33m"; globalStats.other++; }

  globalStats.done++;
  console.log(
    `${color}[P${proxyIndex}][${code}] → ${status}\x1b[0m ` +
    `| TOTAL=${globalStats.done} OK=${globalStats.ok} 403=${globalStats.forbidden} 529r=${globalStats.retries_529} OTHER=${globalStats.other}`
  );
}

// ============================================================
// PROCESS RESPONSE
// ============================================================
function processResponse(response, local_code, session_id = "JS") {
  if (!response) return false;
  if (response.Success !== true) return "INVALID";

  const pg = response.Value?.Events;
  if (!pg || pg.length === 0) return false;

  const number_of_event = pg.length;
  const events_status = [];
  const datetimestamp = [];
  const match_date = [];
  const odds = [];
  const sports = [];
  const lst_events = [];
  const lst_match_time = [];
  const lst_scores = [];
  const lst_teams = [];
  const lst_change = [];
  const lst_match_oddchanges = [];

  const today_date = new Date().toDateString();

  try {
    for (let i = 0; i < number_of_event; i++) {
      const e = pg[i];
      events_status.push(e.Finish === false);
      datetimestamp.push(Number(e.Start) * 1000);
      odds.push(Number(e.Coef));
      sports.push(String(e.SportNameEng));
      lst_events.push(String(e.GroupName));
      lst_scores.push(String(e.MarketName));
      lst_teams.push(`${e.Opp1} vs ${e.Opp2}`);
      lst_change.push(Number(e.Start));
    }

    for (const ts of datetimestamp) {
      const d = new Date(ts);
      match_date.push(d.toDateString());
      lst_match_time.push(d.toTimeString().split(" ")[0]);
    }

    for (const ch of lst_change) {
      const t = new Date(ch).toTimeString().split(" ")[0];
      lst_match_oddchanges.push(t);
    }

    let result = 1.0;
    for (const o of odds) result *= o;

    const match_times = lst_match_time.join("|");
    const outcomes = lst_scores.join("|");
    const events = lst_events.join("|");
    const var_odd = odds.join("|");
    const total_odd = result.toFixed(2);
    const total_result = Number(result);
    const change_times = lst_match_oddchanges.join("|");
    const teams = lst_teams.join("|");

    if (
      events_status.every(s => s === false) &&
      match_date.every(d => d === today_date) &&
      sports.every(s => s === "Football")
    ) {
      console.log(`-----------------------------------------------------------------------------------------------------1`);
      if (number_of_event === 4 && total_result > 100 && total_result < 200) {
        console.log(`-----------------------------------------------------------------------------------------------------2`);
        logCode("QUADRIPLE", local_code, session_id, teams, events, outcomes, match_times, var_odd, total_odd, change_times);
        console.log(`-----------------------------------------------------------------------------------------------------3`);
        return "VALID";
      } 
    }
  } catch (err) {
    console.log(err, `1----${local_code}`);
  } finally {
    [events_status, datetimestamp, match_date, odds, sports,
     lst_events, lst_match_time, lst_scores, lst_teams,
     lst_change, lst_match_oddchanges].forEach(arr => { arr.length = 0; });
    global.gc?.();
  }
  return false;
}

// ============================================================
// RESOURCE BLOCKING — reduce data traffic ~80%
// ============================================================
async function blockResources(page) {
  await page.route('**/*', (route) => {
    const type = route.request().resourceType();
    if (['image', 'stylesheet', 'font', 'media', 'manifest', 'other'].includes(type)) {
      return route.abort();
    }
    return route.continue();
  });
}

// ============================================================
// BUILD HEADERS
// ============================================================
function buildHeaders() {
  return {
    "accept": "application/json, text/plain, */*",
    "content-type": "application/json",
    "is-srv": "false",
    "Referer": "https://indi-1xbet.com/en",
    "Origin": "https://indi-1xbet.com",
    "sec-ch-ua": SEC_CH_UA[Math.floor(Math.random() * SEC_CH_UA.length)],
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": ['"Windows"', '"macOS"', '"Linux"'][Math.floor(Math.random() * 3)],
    "user-agent": USER_AGENTS[Math.floor(Math.random() * USER_AGENTS.length)],
    "x-app-n": "__BETTING_APP__",
    "x-hd": generateXHD(),
    "x-mobile-project-id": "0",
    "x-requested-with": "XMLHttpRequest",
    "x-svc-source": "__BETTING_APP__"
  };
}

// ============================================================
// FETCH CODE — with 529 immediate retry (per-context)
// ============================================================
async function fetchCode(ctx, code, proxyIndex, recovery) {
  if (HARD_SHUTDOWN || STOP_FLAG) return;

  const payload = JSON.stringify({ "Guid": code, "Lng": "en", "partner": 71 });
  const MAX_529_RETRIES = 5;
  let attempts = 0;

  while (attempts <= MAX_529_RETRIES) {
    try {
      const resp = await ctx.request.post(
        `https://indi-1xbet.com/service-api/LiveBet/Open/GetCoupon`,
        {
          timeout: 30000,
          headers: buildHeaders(),
          data: payload
        }
      );

      const status = resp.status();

      // 529 — immediate retry within this context
      if(status === 529 || status === 503) {
        attempts++;
        logStatus(code, 529, proxyIndex);
        console.log(`  ↻ 529 retry ${attempts}/${MAX_529_RETRIES} for ${code}`);
        await new Promise(r => setTimeout(r, 500 + Math.random() * 1000));
        try { resp.dispose?.(); } catch {}
        continue;
      }

      // 403 — log and skip
      if (status === 403) {
      
        recovery.BLOCKED_COUNT++;
      
        logStatus(code, 403, proxyIndex);
      
        if (recovery.BLOCKED_COUNT >= recovery.BLOCK_THRESHOLD && !recovery.RECOVERING_403) {
      
          recovery.RECOVERING_403 = true;
      
          await recovery.recoverFrom403();
      
        }
      
        return;
      }

      // non-200
      if (status !== 200) {
        logStatus(code, status, proxyIndex);
        try { resp.dispose?.(); } catch {}
        return;
      }

      // 200
      logStatus(code, 200, proxyIndex);

      try {
        const contentType = resp.headers()["content-type"] || "";
        const text = await resp.text();

        if (contentType.includes("application/json")) {
          let jsonObj;
          try { jsonObj = JSON.parse(text); } catch (e) {
            console.log(`[${code}] JSON parse error`);
            return;
          }
          await processResponse(jsonObj, code, `P${proxyIndex}`);
        } else {
          console.log(`[${code}] Unknown format: ${contentType}`);
        }
      } catch (err) {
        console.log(`[${code}] Response error: ${err.message}`);
      } finally {
        try { resp.dispose?.(); } catch {}
      }

      return;

    } catch (err) {
      if (err.message.includes("Timeout")) {
    
        attempts++;
    
        console.log(`[P${proxyIndex}] Timeout retry ${attempts}/${MAX_529_RETRIES} for ${code}`);
    
        if (attempts <= MAX_529_RETRIES) {
          await new Promise(r => setTimeout(r, 500 + Math.random() * 1000));
          continue;
        }
    
      }
    
      globalStats.done++;
      globalStats.other++;
    
      console.log(`\x1b[35m[P${proxyIndex}][${code}] → ERROR (${err.message})\x1b[0m`);
    
      return;
    }
  }

  console.log(`\x1b[31m[P${proxyIndex}][${code}] → 529 MAX RETRIES EXHAUSTED\x1b[0m`);
}

// ============================================================
// PROXY WORKER — one proxy handles one prefix with N contexts
// ============================================================
async function runProxyWorker(prefix, proxyIndex) {
  let RESTARTING = false;
  let REQUEST_COUNTER = 0;
  const RESTART_THRESHOLD = 5000;
  const proxy = getProxy(proxyIndex);
  const codes = generateCodes(prefix);
  let codeIndex = 0;

  let browser;
  try {
    browser = await chromium.launch({
      headless: true,
      proxy: { server: proxy.server, username: proxy.username, password: proxy.password },
      args: [
        "--disable-blink-features=AutomationControlled",
        "--no-sandbox",
        "--disable-web-security",
        "--disable-features=VizDisplayCompositor",
        "--disable-gpu",
        "--disable-dev-shm-usage"
      ]
    });
  } catch (err) {
    console.log(`\x1b[31m[P${proxyIndex}] Browser launch failed: ${err.message}\x1b[0m`);
    return;
  }
  async function restartBrowserAndPool() {

  console.log(`♻ Restarting browser for proxy ${proxyIndex}`);

  try { await pool.close(); } catch {}
  try { await browser.close(); } catch {}

  browser = await chromium.launch({
    headless: true,
    proxy: { server: proxy.server, username: proxy.username, password: proxy.password },
    args: [
      "--disable-blink-features=AutomationControlled",
      "--no-sandbox"
    ]
  });

  await pool.rebuild(browser);

}
  async function recoverFrom403() {

  console.log(`🚨 Proxy ${proxyIndex} blocked — rebuilding browser`);

  try { await pool.close(); } catch {}
  try { await browser.close(); } catch {}

  browser = await chromium.launch({
    headless: true,
    proxy: { server: proxy.server, username: proxy.username, password: proxy.password },
    args: [
      "--disable-blink-features=AutomationControlled",
      "--no-sandbox"
    ]
  });

  await pool.rebuild(browser);

  recovery.BLOCKED_COUNT = 0;
  recovery.RECOVERING_403 = false;

  console.log(`✅ Proxy ${proxyIndex} recovered`);

}
  const recovery = {
  BLOCKED_COUNT: 0,
  BLOCK_THRESHOLD: 20,
  RECOVERING_403: false,
  recoverFrom403
};
  // Create N contexts for this proxy
  const pool = new ContextPool(browser, CONTEXTS_PER_PROXY);
  await pool.init();

  console.log(`🚀 [P${proxyIndex}] Starting prefix "${prefix}" with ${CONTEXTS_PER_PROXY} contexts, ${codes.length} codes`);

  // Run all contexts in parallel
  async function contextWorker(workerId) {
  
    while (!HARD_SHUTDOWN) {
  
      if (recovery.RECOVERING_403) {
        await new Promise(r => setTimeout(r, 100));
        continue;
      }
  
      let ctx;
  
      try {
  
        ctx = await pool.acquire();
  
        const BATCH = 4;

        const tasks = [];
        
        for (let b = 0; b < BATCH; b++) {
        
          const idx = codeIndex++;
        
          if (idx >= codes.length) break;
        
          tasks.push(fetchCode(ctx, codes[idx], proxyIndex, recovery));
        
        }
        
        await Promise.all(tasks);
  
        REQUEST_COUNTER++;
  
        if (REQUEST_COUNTER >= RESTART_THRESHOLD &&!RESTARTING &&!recovery.RECOVERING_403) {
        
          RESTARTING = true;
        
          REQUEST_COUNTER = 0;
        
          await restartBrowserAndPool();
        
          RESTARTING = false;
        }
  
      } catch (err) {
  
        console.log(err.message);
  
      } finally {
  
        pool.release(ctx);
  
      }
  
    }
  
  }
  const workers = [];

  for (let i = 0; i < CONTEXTS_PER_PROXY; i++) {
    workers.push(contextWorker(i));
  }
  
  await Promise.all(workers);

  try { await browser.close(); } catch {}

  console.log(`✅ [P${proxyIndex}] Prefix "${prefix}" complete`);
}

// ============================================================
// RUNTIME WATCHDOG
// ============================================================
function runtimeWatchdog() {
  const interval = setInterval(async () => {
    if (Date.now() >= END_TIME) {
      console.log("⏰ MAX EXECUTION TIME REACHED — FORCE SHUTDOWN");
      HARD_SHUTDOWN = true;
      STOP_FLAG = true;
      clearInterval(interval);
    }
  }, 1000);
}

// ============================================================
// MAIN — SHUFFLED PREFIX LIST, BATCHED BY 200 PROXIES
// ============================================================
(async () => {
  try {
    runtimeWatchdog();
    await dbReadyPromise;
    console.log("✅ DB ready, starting scraper...");
    // Generate all 1156 two-char prefixes and shuffle randomly
    const ALL_PREFIXES = generateAllPrefixes();
    shuffle(ALL_PREFIXES);

    const totalBatches = Math.ceil(ALL_PREFIXES.length / TOTAL_PROXIES);

    console.log(`📋 Total prefixes: ${ALL_PREFIXES.length} (shuffled)`);
    console.log(`🌐 Proxies: ${TOTAL_PROXIES} (ports ${START_PORT}–${END_PORT})`);
    console.log(`🧵 Contexts per proxy: ${CONTEXTS_PER_PROXY}`);
    console.log(`⚡ Total concurrent contexts per batch: ${TOTAL_PROXIES * CONTEXTS_PER_PROXY}`);
    console.log(`📦 Total batches: ${totalBatches}`);
    console.log(`🔀 First 10 prefixes: ${ALL_PREFIXES.slice(0, 10).join(", ")}`);

    // Process prefixes in batches of TOTAL_PROXIES (200)
    // e.g. 1156 prefixes → batch 1: 200, batch 2: 200, ..., batch 6: 156
    for (let batchStart = 0; batchStart < ALL_PREFIXES.length; batchStart += TOTAL_PROXIES) {
      if (HARD_SHUTDOWN) break;

      const batch = ALL_PREFIXES.slice(batchStart, batchStart + TOTAL_PROXIES);
      const batchNum = Math.floor(batchStart / TOTAL_PROXIES) + 1;

      console.log(`\n========================================`);
      console.log(`🔥 BATCH ${batchNum}/${totalBatches} — ${batch.length} prefixes (proxies 0–${batch.length - 1})`);
      console.log(`========================================\n`);

      // Launch all proxy workers simultaneously
      // Each proxy[i] gets batch[i] prefix
      await Promise.all(
        batch.map((prefix, index) => runProxyWorker(prefix, index))
      );

      console.log(`✅ Batch ${batchNum}/${totalBatches} complete | Total processed: ${Math.min(batchStart + TOTAL_PROXIES, ALL_PREFIXES.length)}/${ALL_PREFIXES.length}`);
    }

    console.log("\n🏁 ALL BATCHES COMPLETE");
    console.log(`📊 Final stats: OK=${globalStats.ok} 403=${globalStats.forbidden} 529r=${globalStats.retries_529} OTHER=${globalStats.other} TOTAL=${globalStats.done}`);

  } finally {
    await flushDbWorker();
    console.log("💾 Database flushed");

    try {
      await uploadDbToDrive();
    } catch (err) {
      console.log("Drive upload failed:", err.message);
    }

    console.log("✅ Safe exit");
    process.exit(0);
  }
})();
