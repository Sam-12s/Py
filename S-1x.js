// ============================================================
// DISTRIBUTED SCRAPER v2 — 200 PROXIES × N CONTEXTS
// ============================================================
const CONTEXTS_PER_PROXY = 40;
const TOTAL_PER_PREFIX = 39304;
const Database = require("better-sqlite3");
const MAX_RUNTIME_MINUTES = 350;
const START_TIME = Date.now();
const END_TIME = START_TIME + MAX_RUNTIME_MINUTES * 60 * 1000;
let STOP_FLAG = false;
let HARD_SHUTDOWN = false;
const GLOBAL_RETRY_QUEUE = [];
let globalStats = {
  ok: 0,
  forbidden: 0,
  other: 0,
  done: 0,
  retries_529: 0
};
let GLOBAL_CODE_QUEUE = [];
const { chromium } = require("playwright");
// ============================================================
// GLOBAL REQUEST LIMITER (keeps same pressure)
// ============================================================

const MAX_REQUESTS_IN_FLIGHT = 3200; 
let inFlightRequests = 0;
function getElapsedTime() {
  const ms = Date.now() - START_TIME;

  const totalSeconds = Math.floor(ms / 1000);
  const h = String(Math.floor(totalSeconds / 3600)).padStart(2, "0");
  const m = String(Math.floor((totalSeconds % 3600) / 60)).padStart(2, "0");
  const s = String(totalSeconds % 60).padStart(2, "0");

  return `${h}:${m}:${s}`;
}
async function acquireRequestSlot() {
  while (inFlightRequests >= MAX_REQUESTS_IN_FLIGHT && !HARD_SHUTDOWN) {
    await new Promise(r => setTimeout(r, 2));
  }
  inFlightRequests++;
}

function releaseRequestSlot() {
  inFlightRequests--;
}

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
  const FIRST_CHAR = "2";

  for (const b of SUFFIX_CHARS) {
    prefixes.push(`${FIRST_CHAR}${b}`);
  }

  return prefixes; // 34 prefixes
}

function shuffle(array) {
  for (let i = array.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [array[i], array[j]] = [array[j], array[i]];
  }
}
const proxyTokens = new Array(TOTAL_PROXIES).fill(4);
// ============================================================
// PROXY INTELLIGENCE TRACKING
// ============================================================

const proxyStats = Array.from({ length: TOTAL_PROXIES }, () => ({
  success: 0,
  failure: 0,
  requests: 0,

  score: 100,
  health: 1.0,

  delay: 2000, // start at 2s
  nextAvailable: 0,

  avgLatency: 0,
  lastLatency: 0
}));

const PROXY_WINDOW = 50;
const DELAY_STEP = 25000; // 30 seconds

setInterval(() => {
  for (let i = 0; i < TOTAL_PROXIES; i++) {
    proxyTokens[i] = Math.min(proxyTokens[i] + 1, 4);
  }
}, 250);

// ============================================================
// GENERATE ALL CODES ONCE (~1.33M)
// ============================================================

function generateAllCodes() {

  const prefixes = generateAllPrefixes();
  const codes = [];

  for (const prefix of prefixes) {
    for (const a of SUFFIX_CHARS) {
      for (const b of SUFFIX_CHARS) {
        for (const c of SUFFIX_CHARS) {
          codes.push(`${prefix}${a}${b}${c}`);
        }
      }
    }
  }

  shuffle(codes);
  return codes;
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

      // ✅ WARM-UP SESSION (ADD THIS)
      try {
        await ctx.request.get("https://indi-1xbet.com/en");
      } catch (e) {}

      this.pool.push(ctx);
      this.queue.push(ctx);
    }
  }

  async acquire() {
    while (this.queue.length === 0 && !HARD_SHUTDOWN) {
      await new Promise(r => setTimeout(r, 10));
    }

    const ctx = this.queue.shift();
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
const nodemailer = require("nodemailer");
const fs = require("fs");

async function sendDbViaGmail() {
  const dbPath = "OUTPUT.db";

  if (!fs.existsSync(dbPath)) {
    console.log("📭 OUTPUT.db not found. No email sent.");
    return;
  }

  const transporter = nodemailer.createTransport({
    service: "gmail",
    auth: {
      user: "1btcryptopayment@gmail.com",
      pass: "zjti bewf hoib dteb"
    }
  });

  await transporter.sendMail({
    from: "1btcryptopayment@gmail.com",
    to: "tidianeyonkeu515@gmail.com",
    subject: "Scraper Output DB",
    text: "Attached is the OUTPUT.db generated by the script.",
    attachments: [
      {
        filename: "OUTPUT.db",
        path: dbPath
      }
    ]
  });

  console.log("📧 OUTPUT.db sent successfully via Gmail.");
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
  parentPort.postMessage("ready");
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
  if (status === 200) globalStats.ok++;
  else if (status === 403) globalStats.forbidden++;
  else if (status === 529) globalStats.retries_529++;
  else globalStats.other++;

  globalStats.done++;

  // ✅ ONLY LOG EVERY 100,000 REQUESTS
  if (globalStats.done % 5000 !== 0) return;

  console.log(
  `[${getElapsedTime()}] [PROGRESS] TOTAL=${globalStats.done} | OK=${globalStats.ok} | 403=${globalStats.forbidden} | 529r=${globalStats.retries_529} | OTHER=${globalStats.other}`
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
      events_status.every(s => s === true) &&
      match_date.every(d => d === today_date) &&
      sports.every(s => s === "Football")
    ) {
      if (number_of_event === 4 && total_result > 100 && total_result < 200) {
        logCode("QUADRIPLE", local_code, session_id, teams, events, outcomes, match_times, var_odd, total_odd, change_times);
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

function updateProxyScore(stats) {

  const successRate =
    stats.success / Math.max(stats.requests,1);

  const failureRate =
    stats.failure / Math.max(stats.requests,1);

  const latencyPenalty =
    Math.min(stats.avgLatency / 4000, 1);

  stats.health =
      successRate * 0.55
    + (1 - failureRate) * 0.20
    + (1 - latencyPenalty) * 0.20;

  stats.score = Math.max(5, Math.min(100, stats.health * 100));
}

// ============================================================
// FETCH CODE — with 529 immediate retry (per-context)
// ============================================================
async function fetchCode(ctx, item, proxyIndex, recovery) {
  const stats = proxyStats[proxyIndex];
  await acquireRequestSlot();
  if (HARD_SHUTDOWN || STOP_FLAG) return;
  const { code, tried } = item;
  const payload = JSON.stringify({ "Guid": code, "Lng": "en", "partner": 71 });
  while (proxyTokens[proxyIndex] <= 0) {
  await new Promise(r => setTimeout(r, 5));
  }

  proxyTokens[proxyIndex]--;

    try {
      const startTime = Date.now();
      const resp = await ctx.request.post(
        `https://indi-1xbet.com/service-api/LiveBet/Open/GetCoupon`,
        {
          timeout: 15000,
          headers: buildHeaders(),
          data: payload
        }
      );

      const status = resp.status();
      stats.requests++;
      const latency = Date.now() - startTime;

      stats.lastLatency = latency;

      stats.avgLatency =
        stats.avgLatency === 0
          ? latency
          : stats.avgLatency * 0.85 + latency * 0.15;
      // 529 — immediate retry within this context
      if (status === 529 || status === 503) {
        logStatus(code, 529, proxyIndex);
        stats.failure++;

        console.log(
          `  ↻ RETRY QUEUED [${code}] from P${proxyIndex} (tried=${tried.size})`
        );

        // 🧠 requeue ONLY if not exhausted
        if (!item.queued) {
          item.queued = true;

          setTimeout(() => {
            item.queued = false; // 🔥 reset before requeue
            GLOBAL_RETRY_QUEUE.push(item);
          }, 1000 + Math.random() * 2000);
        }

        return;
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
        stats.failure++;

        // 🔥 LOG UNKNOWN STATUS (but don't spam)
        if (status !== 403 && status !== 529) {
          if (globalStats.other < 50) { // limit logs
            console.log(
              `[${getElapsedTime()}] ❗ OTHER STATUS: ${status} | CODE=${code} | PROXY=P${proxyIndex}`
            );
          }
        }

        if (tried.size < TOTAL_PROXIES) {
          if (!item.queued) {
            item.queued = true;
            GLOBAL_RETRY_QUEUE.push(item);
          }
        }

      return;
}
      // 200
      logStatus(code, 200, proxyIndex);


      stats.success++;

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
      } finally {
        try { resp.dispose?.(); } catch {}
      }

      return;

    } catch (err) {
      stats.failure++;
      stats.requests++;
      // treat timeout like a retryable error
      if (tried.size < TOTAL_PROXIES) {
        if (!item.queued) {
          item.queued = true;

          setTimeout(() => {
            item.queued = false;
            GLOBAL_RETRY_QUEUE.push(item);
          }, 1500 + Math.random()*2000);
        }
      }

    } finally {


            // ============================================================
            // PROXY PERFORMANCE EVALUATION (every 30 requests)
            // ============================================================

            if (stats.requests >= PROXY_WINDOW) {

              updateProxyScore(stats);

              const MIN_DELAY = 1800;
              const MAX_DELAY = 3500;

              const scoreFactor = (100 - stats.score) / 100;

              stats.delay =
                MIN_DELAY +
                scoreFactor * (MAX_DELAY - MIN_DELAY);

              // 🔥 punish weak proxies
              if (stats.score < 30) stats.delay += 1000;
              if (stats.score < 15) stats.delay += 2000;

              stats.nextAvailable = Date.now() + stats.delay;

              stats.requests = 0;
              stats.success = 0;
              stats.failure = 0;
            }
            // 🔥 ALWAYS release slot
            releaseRequestSlot();

    }
}

// ============================================================
// PROXY WORKER — one proxy handles one prefix with N contexts
// ============================================================
async function runProxyWorker(proxyIndex) {
  const proxy = getProxy(proxyIndex);
  let browser;

  // =========================
  // LAUNCH BROWSER
  // =========================
  try {
    browser = await chromium.launch({
      headless: true,
      proxy: {
        server: proxy.server,
        username: proxy.username,
        password: proxy.password
      },
      args: [
        "--disable-blink-features=AutomationControlled",
        "--no-sandbox",
        "--disable-dev-shm-usage"
      ]
    });
  } catch (err) {
    console.log(`\x1b[31m[P${proxyIndex}] Browser launch failed: ${err.message}\x1b[0m`);
    return;
  }

  // =========================
  // CONTEXT POOL
  // =========================
  const pool = new ContextPool(browser, CONTEXTS_PER_PROXY);
  await pool.init();

  // =========================
  // RECOVERY SYSTEM
  // =========================
  const recovery = {
    BLOCKED_COUNT: 0,
    BLOCK_THRESHOLD: 20,
    RECOVERING_403: false,

    async recoverFrom403() {
      console.log(`🚨 [P${proxyIndex}] Proxy blocked → rebuilding`);

      try { await pool.close(); } catch {}
      try { await browser.close(); } catch {}

      browser = await chromium.launch({
        headless: true,
        proxy: {
          server: proxy.server,
          username: proxy.username,
          password: proxy.password
        }
      });

      await pool.rebuild(browser);

      this.BLOCKED_COUNT = 0;
      this.RECOVERING_403 = false;

      console.log(`✅ [P${proxyIndex}] Recovery complete`);
    }
  };

  console.log(`🚀 [P${proxyIndex}] Worker started`);

  // =========================
  // SMART RETRY PICK
  // =========================
  function getNextCode() {
  
    // FIRST: try retry queue
    while (GLOBAL_RETRY_QUEUE.length > 0) {
  
      const item = GLOBAL_RETRY_QUEUE.pop();
  
      if (item.tried.size >= TOTAL_PROXIES) {
        continue;
      }
  
      if (!item.tried.has(proxyIndex)) {
        item.tried.add(proxyIndex);
        return item;
      }
  
      // push back if this proxy already tried
      GLOBAL_RETRY_QUEUE.unshift(item);
      break;
    }
  
    // SECOND: normal queue
    const code = GLOBAL_CODE_QUEUE.pop();
    if (!code) return null;
  
    return {
      code,
      tried: new Set([proxyIndex]),
      queued: false
    };
  }
  // =========================
  // CONTEXT WORKER
  // =========================
  async function contextWorker(workerId) {
  
    let ctx = await pool.acquire();
    let ctxRequests = 0;
  
    const BATCH = 4; // better pipeline
  
    while (!HARD_SHUTDOWN) {
  
      if (recovery.RECOVERING_403) {
        await new Promise(r => setTimeout(r, 100));
        continue;
      }
  
      try {
  
        const tasks = [];
  
        for (let i = 0; i < BATCH; i++) {
  
          // ==========================================
          // PROXY DELAY CONTROL
          // ==========================================
          const stats = proxyStats[proxyIndex];
          const now = Date.now();
  
          if (stats.nextAvailable > now) {
            const wait = stats.nextAvailable - now;
            await new Promise(r => setTimeout(r, wait + Math.random()*200));
          }
  
          // ==========================================
          // GET NEXT CODE
          // ==========================================
          const item = getNextCode();
          if (!item) break;
          await new Promise(r => setTimeout(r, 5 + Math.random()*40));
  
          tasks.push(
            fetchCode(ctx, item, proxyIndex, recovery)
              .catch(() => {
                if (!item.queued) {
                  item.queued = true;
                  GLOBAL_RETRY_QUEUE.push(item);
                }
              })
          );
  
          ctxRequests++;
        }
  
        // ==========================================
        // STOP CONDITION
        // ==========================================
        if (GLOBAL_CODE_QUEUE.length === 0 && GLOBAL_RETRY_QUEUE.length === 0) {
          break;
        }
  
        await Promise.all(tasks);
  
        // ==========================================
        // CONTEXT ROTATION
        // ==========================================
        if (ctxRequests >= 200000) {
  
          try {
            await ctx.close();
          } catch {}
  
          ctx = await pool.browser.newContext(randomContextOptions());
  
          // warm session
          try {
            await ctx.request.get("https://indi-1xbet.com/en");
          } catch {}
  
          ctxRequests = 0;
        }
  
      } catch (err) {
  
        console.log(`[P${proxyIndex}] Worker error: ${err.message}`);
  
      }
    }
  
    // ==========================================
    // CLEANUP
    // ==========================================
    try {
      pool.release(ctx);
    } catch {}
  
  }

  // =========================
  // START WORKERS
  // =========================
  const workers = [];

  for (let i = 0; i < CONTEXTS_PER_PROXY; i++) {
    workers.push(contextWorker(i));
  }

  await Promise.all(workers);

  // =========================
  // CLEANUP
  // =========================
  try { await pool.close(); } catch {}
  try { await browser.close(); } catch {}

  console.log(`✅ [P${proxyIndex}] DONE`);
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

    const ALL_PREFIXES = generateAllPrefixes();
    shuffle(ALL_PREFIXES);

    const totalBatches = Math.ceil(ALL_PREFIXES.length / TOTAL_PROXIES);

    console.log(`📋 Total prefixes: ${ALL_PREFIXES.length} (shuffled)`);
    console.log(`🌐 Proxies: ${TOTAL_PROXIES} (ports ${START_PORT}–${END_PORT})`);
    console.log(`🧵 Contexts per proxy: ${CONTEXTS_PER_PROXY}`);
    console.log(`⚡ Total concurrent contexts per batch: ${TOTAL_PROXIES * CONTEXTS_PER_PROXY}`);
    console.log(`📦 Total batches: ${totalBatches}`);
    console.log(`🔀 First 10 prefixes: ${ALL_PREFIXES.slice(0, 10).join(", ")}`);

    console.log("⚙️ Generating all codes...");
    GLOBAL_CODE_QUEUE = generateAllCodes();

    console.log(`📦 Total codes: ${GLOBAL_CODE_QUEUE.length}`);

    await Promise.all(
      Array.from({ length: TOTAL_PROXIES }, (_, i) => runProxyWorker(i))
    );

    // stop everything immediately
    HARD_SHUTDOWN = true;
    STOP_FLAG = true;
    console.log(`📊 Final stats: OK=${globalStats.ok} 403=${globalStats.forbidden} 529r=${globalStats.retries_529} OTHER=${globalStats.other} TOTAL=${globalStats.done}`);

  } finally {
    await flushDbWorker();
    console.log("💾 Database flushed");

    try {
      await sendDbViaGmail();
    } catch (err) {
      console.log("Drive upload failed:", err.message);
    }

    console.log("✅ Safe exit");
    process.exit(0);
  }
})();
