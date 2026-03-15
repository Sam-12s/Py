const MAX_CONTEXTS = 50;        // real concurrency
const TOTAL_PER_PREFIX = 39304;
const Database = require("better-sqlite3");
const MAX_RUNTIME_MINUTES = 356;
let REQUEST_COUNTER = 0;
const RESTART_THRESHOLD = 2500;
let RESTARTING = false;
let RESTART_REQUESTED = false;
let RECOVERING_403 = false;
let BROWSER_GENERATION = 0;
let POOL_GENERATION = 0;
let IN_FLIGHT = 0;
const BLOCKED_QUEUE = new Set();
const START_TIME = Date.now();
const END_TIME = START_TIME + MAX_RUNTIME_MINUTES * 60 * 1000;
const xml2js = require("xml2js");
let STOP_FLAG = false;
let BLOCKED_CODE = null;
let DEBUG_COUNTER = 0;
let HARD_SHUTDOWN = false;
let PROXY_ROTATING = false;
let stats = {
  ok: 0,
  forbidden: 0,
  other: 0,
  done: 0
};
const { chromium } = require("playwright");
let GLOBAL_PAUSE = false;

const PROXIES = [
  'http://spfsvdt89u:f25gv0_jbagu1ZPLMb@dc.decodo.com:10001',
  'http://spfsvdt89u:f25gv0_jbagu1ZPLMb@dc.decodo.com:10002',
  'http://spfsvdt89u:f25gv0_jbagu1ZPLMb@dc.decodo.com:10003'
];

let CURRENT_PROXY_INDEX = 0;
let CURRENT_PROXY = PROXIES[CURRENT_PROXY_INDEX];
const PROXY_ROTATION_THRESHOLD = 6000;
let PROXY_REQUEST_COUNTER = 0;

async function waitIfPaused() {
  if (!GLOBAL_PAUSE) return;
  while (GLOBAL_PAUSE) {
    await new Promise(r => setTimeout(r, 50));
  }
}
async function rotateProxy(state) {

  if (HARD_SHUTDOWN) return;

  console.log("🌐 Proxy rotation starting...");

  // Pause workers
  GLOBAL_PAUSE = true;
  STOP_FLAG = true;

  // Wait for all in-flight requests to finish
  while (IN_FLIGHT > 0) {
    await new Promise(r => setTimeout(r, 20));
  }

  console.log("🧊 All active requests completed");

  // Move to next proxy
  CURRENT_PROXY_INDEX++;

  if (CURRENT_PROXY_INDEX >= PROXIES.length) {
    CURRENT_PROXY_INDEX = 0;
  }

  CURRENT_PROXY = PROXIES[CURRENT_PROXY_INDEX];

  console.log(`🔁 Switching to proxy: ${CURRENT_PROXY}`);
  POOL_GENERATION++;

  // Destroy current pool and browser
  try { await state.pool.close(); } catch {}
  try { await state.browser.close(); } catch {}

  // Launch new browser with new proxy
  state.browser = await chromium.launch({
    headless: true,
    proxy: {
      server: CURRENT_PROXY
    },
    args: [
      "--disable-blink-features=AutomationControlled",
      "--no-sandbox"
    ]
  });

  // Recreate context pool
  state.pool = new ContextPool(state.browser, MAX_CONTEXTS);
  await state.pool.init();

  // Reset request counter
  PROXY_REQUEST_COUNTER = 0;

  console.log("✅ Proxy rotation complete");

  // Resume workers
  STOP_FLAG = false;
  GLOBAL_PAUSE = false;
}

const { google } = require("googleapis");
const fs = require("fs");

async function uploadDbToDrive() {
  const dbPath = "OUTPUT.db";
  if (!fs.existsSync(dbPath)) return;

  const oauth2Client = new google.auth.OAuth2(
    process.env.GDRIVE_CLIENT_ID,
    process.env.GDRIVE_CLIENT_SECRET
  );

  oauth2Client.setCredentials({
    refresh_token: process.env.GDRIVE_REFRESH_TOKEN
  });

  const drive = google.drive({ version: "v3", auth: oauth2Client });

  const fileMetadata = {
    name: "OUTPUT.db",
    parents: ["1GJ13uUpHRvY0uEAZbXbhL4S1YNTjU7NR"]  // Optional: put in a folder
  };

  const media = {
    mimeType: "application/x-sqlite3",
    body: fs.createReadStream(dbPath)
  };

  await drive.files.create({
    resource: fileMetadata,
    media: media,
    fields: "id"
  });

  console.log("✅ Database uploaded to My Drive");
}
const { Worker, isMainThread, parentPort, workerData } = require("worker_threads");

if (!isMainThread && workerData === "DB_WORKER") {
  // ==========================
  // DATABASE WORKER
  // ==========================
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
    const tx = db.transaction(rows => {
      for (const r of rows) insert.run(...r);
    });
    tx(buffer);
    buffer = [];
  }

  parentPort.on("message", msg => {
    if (msg === "flush") {
      flush();
      return;
    }
    buffer.push(msg);
  });

  setInterval(flush, 200);

  console.log("🗄️  DB Worker ready");
  return;
}

// ==========================
// MAIN THREAD
// ==========================
let dbWorker;
if (isMainThread) {
  dbWorker = new Worker(__filename, { workerData: "DB_WORKER" });
}

// Function to log codes to DB worker
function logCode(label, code, workerId, teams, events, score, times, odds, totalOdds, lastChange) {
  if (dbWorker) {
    dbWorker.postMessage([workerId, label, code, teams, events, score, times, odds, totalOdds, lastChange]);
  }
}
// Function to flush DB worker (call on shutdown)
async function flushDbWorker() {
  if (!dbWorker) return;
  dbWorker.postMessage("flush");
  await new Promise(r => setTimeout(r, 1000)); // give time to flush
}

function runtimeWatchdog(state) {
  const interval = setInterval(async () => {
    if (Date.now() >= END_TIME) {
      console.log("⏰ MAX EXECUTION TIME REACHED — FORCE SHUTDOWN");
      HARD_SHUTDOWN = true;
      STOP_FLAG = true;
      GLOBAL_PAUSE = true;
      clearInterval(interval);
      
      // ✅ FIXED - proper shutdown sequence
      await stopAllWorkers(state);
      await gracefulShutdown(state);
    }
  }, 1000);
}

async function gracefulShutdown(state) {
  console.log("🛑 Initiating graceful shutdown...");

  // Stop recovery loop
  RECOVERING_403 = false;
  BLOCKED_QUEUE.clear();

  // Wait for in-flight requests
  while (IN_FLIGHT > 0) {
    await new Promise(r => setTimeout(r, 50));
  }

  console.log("🧊 All requests finished");

  // Flush DB worker
  await flushDbWorker();

  console.log("💾 Database flushed");

  // Close browser safely
  try { await state.pool.close(); } catch {}
  try { await state.browser.close(); } catch {}

  console.log("🌐 Browser closed");

  // Upload DB
  try {
    await uploadDbToDrive();
  } catch (err) {
    console.log("Drive upload failed:", err.message);
  }

  console.log("☁️ Upload finished");
  console.log("✅ Safe exit");

  process.exit(0);
}

function logStatus(code, status) {
  let color;
  let label = status;

  if (status === 200) {
    color = "\x1b[32m"; // green
    stats.ok++;
  } else if (status === 403) {
    color = "\x1b[31m"; // red
    stats.forbidden++;
  } else {
    color = "\x1b[33m"; // yellow
    stats.other++;
  }

  stats.done++;

  console.log(
    `${color}[${code}] → ${label}\x1b[0m ` +
    `| DONE ${stats.done}/${TOTAL_PER_PREFIX} | OK=${stats.ok} 403=${stats.forbidden} OTHER=${stats.other}`
  );
}

const SUFFIX_CHARS = ["0","1","2","3","4","5","6","7","8","9","A","B","C","D","E","F","G","H","J","K","L","M","N","P","Q","R","S","T","U","V","W","X","Y","Z"];

function generateCodes(prefix) {
  const out = [];

  for (const a of SUFFIX_CHARS) {
    for (const b of SUFFIX_CHARS) {
      for (const c of SUFFIX_CHARS) {
        out.push(`${prefix}${a}${b}${c}`);
      }
    }
  }

  return out;
}

function randomContextOptions() {
  const userAgents = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/124.0.0.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) Chrome/125.0.0.0",
    "Mozilla/5.0 (X11; Linux x86_64) Firefox/118.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) Safari/604.1"
  ];

  const timezones = [
    "America/New_York",
    "Europe/London",
    "Africa/Lagos",
    "Europe/Paris",
    "Asia/Singapore"
  ];

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
  }

  async init() {
    for (let i = 0; i < this.size; i++) {
      const ctx = await this.browser.newContext(randomContextOptions());
      this.pool.push(ctx);
      this.queue.push(ctx);
    }
  }

  async acquire() {
  while (this.queue.length === 0 && !STOP_FLAG) {
    await new Promise(r => setTimeout(r, 5));
  }

  if (STOP_FLAG) throw new Error("Pool stopped");

  const ctx = this.queue.pop();
  ctx.__poolGen = POOL_GENERATION;   // 🔑
  return ctx;
}

  release(ctx) {
    this.queue.push(ctx);
  }

  async close() {
    for (const ctx of this.pool) {
      await ctx.close();
    }
  }
}

async function restartBrowserAndPool(state) {

  if (HARD_SHUTDOWN) return;

  console.log("♻ Restarting browser + context pool");

  // 🔴 PAUSE ALL WORKERS
  GLOBAL_PAUSE = true;
  STOP_FLAG = true;

  // Wait for running requests to finish
  while (IN_FLIGHT > 0) {
    await new Promise(r => setTimeout(r, 20));
  }
  POOL_GENERATION++;

  // Close old pool and browser
  try { await state.pool.close(); } catch {}
  try { await state.browser.close(); } catch {}

  // Launch new browser
  state.browser = await chromium.launch({
      headless: true,
      proxy: {server: CURRENT_PROXY},
      args: [
        "--disable-blink-features=AutomationControlled", 
        "--no-sandbox",
        "--disable-web-security",
        "--disable-features=VizDisplayCompositor"
      ]
    });

  // Recreate pool
  state.pool = new ContextPool(state.browser, MAX_CONTEXTS);
  await state.pool.init();

  console.log("✅ Browser and pool restarted");

  // 🟢 RESUME WORKERS
  STOP_FLAG = false;
  GLOBAL_PAUSE = false;
}

// 🔄 1XBET Header Rotation System (Add BEFORE fetchCode)
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

async function fetchCode(ctx, code, state) {
  // ⛔ hard pause barrier (KEEP ALL ORIGINAL LOGIC)
  await waitIfPaused();

  // 🚫 reject stale contexts (KEEP ALL ORIGINAL LOGIC)
  if (ctx.__poolGen !== POOL_GENERATION) {
    return "STALE";
  }

  IN_FLIGHT++; // 🔒 mark request active (KEEP)

  try {
    await waitIfPaused(); // double guard (KEEP)

    // 🔥 1XBET-SPECIFIC PAYLOAD - ONLY Guid changes per request
    const payload = {
      "Guid": code,      // ← DYNAMIC: NHBXF, ABC12, etc.
      "Lng": "en",
      "partner": 71      // ← FIXED as per your info
    };

    // 🔄 ROTATING HEADERS - Different machine fingerprints
    const headers = {
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
      "x-hd": "X1cEBzjgXQIpJfHKN7hn4KKUPFfFP9pkoArENkWOgSyEMlhsyK1OdNmyIaoPJvSFISOBoWDaWRrzh9sGvjXhiJBc7O7d3wwK6UIIIonqgxyyGaCiOj+wannOImrkLEBnP1N8fih9oZMCu8jr6qvAnsG2J3FcekoRh4RokFUdRYVdN+bz018HINbj2aqS2Vw7JiXzx9aPzPmbzaOlRmMeYHgzoMxwV8qgyEvULtJbI8gwlbiCckChXEr5NbIxzykFWxWfwUG6yl3qUZoGU6W+sX1L+kdA2yOzn2TvV5x6Rv6dE8GL5gGrnCZsO6x4WXxMEhhfHR/7JafxUBY9j0LwRx0q28E9NDYBTEnywdfFCFDTHnKg4NrzSGIG2TNPkLLeRgSi0c2PM3XQ9W2zp8VbzyJ/vo+9M91DWuj1Qep1825OaK75KXDcWYYgzL27N3PCXd/tOC0Ta5LAkumhhif1YLLx140TT39K+V9e56bzeHtPF44TdUKrUOjsEhQ1okGNBZZ/r++e8pn0Dx1wWp43T73sh1IvjtDCpJ6QveoGmSrEuYrIM5PSyKV1tgqE9rnn/lKQ2aUTUywiwhyvGehBrMj92gfDNXY8", // Static for now
      "x-mobile-project-id": "0",
      "x-requested-with": "XMLHttpRequest",
      "x-svc-source": "__BETTING_APP__"
    };

    // 🔥 1XBET ENDPOINT + POST
    const resp = await ctx.request.post(
      `https://indi-1xbet.com/service-api/LiveBet/Open/GetCoupon`,
      {
        timeout: 30000,
        headers: headers,
        data: JSON.stringify(payload)  // JSON payload
      }
    );

    const status = resp.status();

    // 🚨 403 HANDLING (KEEP ALL ORIGINAL LOGIC)
    if (status === 403) {
      BLOCKED_QUEUE.add(code);
      logStatus(code, 403);
      console.log("🚨 403 detected — queued for recovery");
      try { await ctx.close(); } catch {}
      recoverFrom403(state);
      return "403";
    }

    // ❌ non-200 (KEEP)
    if (status !== 200) {
      logStatus(code, status);
      return;
    }
    if (status === 200) {
    logStatus(code, 200);
    }
    // ✅ 1XBET JSON PROCESSING (KEEP JSON HANDLING)
    try {
      const contentType = resp.headers()["content-type"] || "";
      const text = await resp.text();

      if (contentType.includes("application/json")) {
        let jsonObj;
        try {
          jsonObj = JSON.parse(text);
        } catch (e) {
          console.log(`[${code}] JSON parse error`);
          return;
        }

        // 🔥 PASS TO NEW processResponse
        await processResponse(jsonObj, code, "JS");
        return;
      }

      console.log(`[${code}] Unknown response format: ${contentType}`);
    } catch (err) {
      console.log(`[${code}] Response handling error: ${err.message}`);
    } finally {
      try { resp.dispose?.(); } catch {}
    }

  } catch (err) {
    stats.done++;
    stats.other++;
    console.log(
      `\x1b[35m[${code}] → ERROR (${err.message})\x1b[0m | DONE ${stats.done}/${TOTAL_PER_PREFIX}`
    );
  } finally {
    IN_FLIGHT--; // 🔓 request finished (KEEP)
  }

  REQUEST_COUNTER++; // KEEP

  PROXY_REQUEST_COUNTER++;

  if (PROXY_REQUEST_COUNTER >= PROXY_ROTATION_THRESHOLD && !PROXY_ROTATING) {
    PROXY_ROTATING = true;
    PROXY_REQUEST_COUNTER = 0;
    await rotateProxy(state);
    PROXY_ROTATING = false;
  }
}



function processResponse(response, local_code, session_id = "JS") {
  if (!response) return false;

  // 🔥 1XBET: Check Success
  if (response.Success !== true) {
    return "INVALID";
  }

  // 🔥 1XBET: response.Value.Events
  const pg = response.Value?.Events;
  if (!pg || pg.length === 0) return false;

  const number_of_event = pg.length;

  // --- Memory-safe arrays (EXACT SAME) ---
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

      // 🔥 1XBET FIELDS ONLY - NO EXTRA lst_type
      events_status.push(e.Finish === false);                    // false = not finished
      datetimestamp.push(Number(e.Start) * 1000);                // Unix → ms
      odds.push(Number(e.Coef));                                 // Odds
      sports.push(String(e.SportNameEng));                       // "Football"
      lst_events.push(String(e.GroupName));                      // "1x2"
      lst_scores.push(String(e.MarketName));                     // "W1", "W2"
      lst_teams.push(`${e.Opp1} vs ${e.Opp2}`);
      lst_change.push(Number(e.Start));                          // Use Start as change proxy
    }

    // Timestamps (EXACT SAME LOGIC)
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

    // 🔥 YOUR EXACT ORIGINAL STRUCTURE - 1XBET ADAPTED
    if (
      events_status.every(s => s === false) &&                    // Finish: false
      match_date.every(d => d === today_date) &&                  // Today
      sports.every(s => s === "Football")                         // Football only
    ) {
      if (number_of_event === 4 && total_result > 100 && total_result < 200) {
        logCode("QUADRIPLE", local_code, session_id, teams, events, outcomes, match_times, var_odd, total_odd, change_times);
        return "VALID";
      } else if (number_of_event === 3 && total_result > 18 && total_result < 200) {
        logCode("TRIPLE", local_code, session_id, teams, events, outcomes, match_times, var_odd, total_odd, change_times);
        return "VALID";
      } else if (number_of_event === 2 && total_result > 25 && total_result < 180 &&
                 lst_events.every(e => e === "Correct Score")) {        // 1xbet equiv?
        logCode("DOUBLE", local_code, session_id, teams, events, outcomes, match_times, var_odd, total_odd, change_times);
        return "VALID";
      } else if (number_of_event === 1 && total_result > 5 && total_result < 40 &&
                 lst_events.every(e => e === "Correct Score")) {        // 1xbet equiv?
        logCode("SINGLE", local_code, session_id, teams, events, outcomes, match_times, var_odd, total_odd, change_times);
        return "VALID";
      }
    } 

  } catch (err) {
    console.log(err, `1----${local_code}`);
  } finally {
    // --- Memory cleanup (EXACT SAME) ---
[events_status, datetimestamp, match_date, odds, sports,
 lst_events, lst_match_time, lst_scores, lst_teams,
 lst_change, lst_match_oddchanges].forEach(arr => { 
  arr.length = 0;
});

    global.gc?.();
  }

  return false;
}


async function runPrefix(state, prefix) {
  const codes = generateCodes(prefix);
  let index = 0;

  async function worker(workerId) {
    while (true) {

      // ⏸ pause during recovery
      while (STOP_FLAG || RECOVERING_403) {
        await new Promise(r => setTimeout(r, 50));
      }

      const i = index++;
      if (i >= codes.length) break;

      let ctx;
      try {
        ctx = await state.pool.acquire();
      } catch {
        continue;
      }

      try {
        const result = await fetchCode(ctx, codes[i], state);

        // 🔁 stale → retry with fresh context
        if (result === "STALE") {
          continue;
        }

      } catch (err) {
        console.log(`[${codes[i]}] Worker error: ${err.message}`);
      } finally {

        // ✅ only return valid contexts
        if (ctx && ctx.__poolGen === POOL_GENERATION) {
          try {
            state.pool.release(ctx);
          } catch {}
        } else {
          try {
            await ctx?.close();
          } catch {}
        }
      }
    }
  }

  // 🚀 spawn workers
  const workers = [];
  for (let i = 0; i < MAX_CONTEXTS; i++) {
    workers.push(worker(i));
  }

  await Promise.all(workers);
}


// --------------------
// STOP ALL WORKERS UTILITY
// --------------------
async function stopAllWorkers(state) {
  console.log("⏸ Pausing workers...");

  STOP_FLAG = true;

  const start = Date.now();
  const MAX_WAIT = 5000; // give in-flight requests up to 15s to finish

  while (Date.now() - start < MAX_WAIT) {
    const returned = state.pool.queue.length;
    const total = state.pool.size;

    console.log(`Waiting contexts: ${returned}/${total}`);

    if (returned >= total) break;

    await new Promise(r => setTimeout(r, 250));
  }

  console.log(
    `All contexts returned: ${state.pool.queue.length}/${state.pool.size}`
  );

  // Extra grace delay for late network responses
  console.log("⏳ Final grace wait for late responses (2s)...");
  await new Promise(r => setTimeout(r, 2000));
}

// --------------------
// 403 RECOVERY FUNCTION
// --------------------
async function recoverFrom403(state) {
  if (HARD_SHUTDOWN) return;
  if (RECOVERING_403) return;
  RECOVERING_403 = true;

  console.log("🚨 Starting 403 recovery");

  // ⛔ freeze the world
  GLOBAL_PAUSE = true;
  STOP_FLAG = true;

  // ⏳ wait for ALL in-flight requests
  while (IN_FLIGHT > 0) {
    await new Promise(r => setTimeout(r, 20));
}

  while (state.pool.queue.length < state.pool.size) {
    await new Promise(r => setTimeout(r, 20));
  }

  console.log("🧊 All in-flight requests completed");

  // 🔥 invalidate all old contexts
  POOL_GENERATION++;

  // 💣 destroy pool + browser
  try { await state.pool.close(); } catch {}
  try { await state.browser.close(); } catch {}

  console.log("♻️ Browser & pool destroyed");

  // 🔄 rebuild
  state.browser = await chromium.launch({
    headless: true,
    proxy: {server: CURRENT_PROXY},
    args: [
      "--disable-blink-features=AutomationControlled", 
      "--no-sandbox",
      "--disable-web-security",
      "--disable-features=VizDisplayCompositor"
    ]
  });

  state.pool = new ContextPool(state.browser, MAX_CONTEXTS);
  await state.pool.init();

  console.log("✅ New browser & pool ready");

  // 🔁 retry ALL blocked codes safely
// Replace the entire retry loop with:
while (BLOCKED_QUEUE.size > 0) {
  const codes = [...BLOCKED_QUEUE];
  
  for (const code of codes) {
    let retryBrowser, retryCtx;
    try {
      retryBrowser = await chromium.launch({headless: true,proxy: { server: CURRENT_PROXY }});
      retryCtx = await retryBrowser.newContext(randomContextOptions());

      const payload = { "Guid": code, "Lng": "en", "partner": 71 };
      const headers = {
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
      "x-hd": "X1cEBzjgXQIpJfHKN7hn4KKUPFfFP9pkoArENkWOgSyEMlhsyK1OdNmyIaoPJvSFISOBoWDaWRrzh9sGvjXhiJBc7O7d3wwK6UIIIonqgxyyGaCiOj+wannOImrkLEBnP1N8fih9oZMCu8jr6qvAnsG2J3FcekoRh4RokFUdRYVdN+bz018HINbj2aqS2Vw7JiXzx9aPzPmbzaOlRmMeYHgzoMxwV8qgyEvULtJbI8gwlbiCckChXEr5NbIxzykFWxWfwUG6yl3qUZoGU6W+sX1L+kdA2yOzn2TvV5x6Rv6dE8GL5gGrnCZsO6x4WXxMEhhfHR/7JafxUBY9j0LwRx0q28E9NDYBTEnywdfFCFDTHnKg4NrzSGIG2TNPkLLeRgSi0c2PM3XQ9W2zp8VbzyJ/vo+9M91DWuj1Qep1825OaK75KXDcWYYgzL27N3PCXd/tOC0Ta5LAkumhhif1YLLx140TT39K+V9e56bzeHtPF44TdUKrUOjsEhQ1okGNBZZ/r++e8pn0Dx1wWp43T73sh1IvjtDCpJ6QveoGmSrEuYrIM5PSyKV1tgqE9rnn/lKQ2aUTUywiwhyvGehBrMj92gfDNXY8", // Static for now
      "x-mobile-project-id": "0",
      "x-requested-with": "XMLHttpRequest",
      "x-svc-source": "__BETTING_APP__"
    };

      const resp = await retryCtx.request.post(
        `https://indi-1xbet.com/service-api/LiveBet/Open/GetCoupon`,
        { timeout: 30000, headers, data: JSON.stringify(payload) }
      );

      const status = resp.status();
      if (status === 200) {
        console.log(`✅ 403 cleared for ${code}`);
        BLOCKED_QUEUE.delete(code);
      }
    } catch (err) {
      console.log(`Retry error for ${code}: ${err.message}`);
    } finally {
      try { await retryCtx?.close(); } catch {}
      try { await retryBrowser?.close(); } catch {}
    }
    await new Promise(r => setTimeout(r, 3000));
  }
}

  // ▶ resume everything
  STOP_FLAG = false;
  GLOBAL_PAUSE = false;
  RECOVERING_403 = false;

  console.log("▶ Workers resumed — full concurrency restored");
}


(async () => {

  

  try {
    const PREFIXES = [
    "RH",
];

    const state = {};
    runtimeWatchdog(state);
    state.browser = await chromium.launch({
      headless: true,
      proxy: {server: CURRENT_PROXY},
      args: [
        "--disable-blink-features=AutomationControlled", 
        "--no-sandbox",
        "--disable-web-security",
        "--disable-features=VizDisplayCompositor"
      ]
    });

    state.pool = new ContextPool(state.browser, MAX_CONTEXTS);
    await state.pool.init();


    for (const prefix of PREFIXES) {
      console.log(`🚀 1XBET: Processing ${TOTAL_PER_PREFIX} codes for ${prefix}`);

      stats = { ok: 0, forbidden: 0, other: 0, done: 0 };

      await runPrefix(state, prefix);
    }

    await state.pool.close();
    await state.browser.close();


  } finally {
  dbWorker.postMessage("flush");
  await new Promise(r => setTimeout(r, 1000));

  await uploadDbToDrive();
  process.exit(0);
}
})();


