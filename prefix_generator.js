// ================================
// BLOCK 1 — IMPORTS & CONFIG
// ================================
const { request, Agent } = require("undici");
const fs = require("fs");
const path = require("path");

// In CommonJS, __dirname already exists
const agent = new Agent({
    connections: 100,        // connection pool
    pipelining: 1,
    keepAliveTimeout: 10_000,
    keepAliveMaxTimeout: 60_000
});
// 🔥 CONTROL HOW MANY PREFIXES YOU WANT
const TOTAL_PREFIXES_TARGET = 26;

// Prefix storage
let PREFIXES = [];

// Worker file path (example target file)
const WORKER_FILES = ["scrapper1.js","scrapper2.js","scrapper3.js","scrapper4.js","scrapper5.js","scrapper6.js","scrapper7.js","scrapper8.js","scrapper9.js","scrapper10.js","scrapper11.js","scrapper12.js","scrapper13.js","scrapper14.js","scrapper15.js","scrapper16.js","scrapper17.js","scrapper18.js"];
const TOTAL_PREFIXES_NEEDED =TOTAL_PREFIXES_TARGET * WORKER_FILES.length;

// ================================
// BLOCK 2 — PREFIX PROCESSOR
// ================================

function extractPrefix(code) {
    // same logic as Python: remove last 3 chars
    return code.slice(0, -3);
}

function collectUniquePrefixes(codes) {
    const prefixSet = new Set();

    for (const code of codes) {
        if (prefixSet.size >= TOTAL_PREFIXES_TARGET) break;

        const prefix = extractPrefix(code);
        prefixSet.add(prefix);
    }

    PREFIXES = Array.from(prefixSet);
    return PREFIXES;
}
// ================================
// BLOCK 3 — UPDATE PREFIX FILE
// ================================

function updateAllWorkerFiles(prefixes) {
    const groups = splitPrefixes(prefixes, TOTAL_PREFIXES_TARGET);

    WORKER_FILES.forEach((file, i) => {
        const filePath = path.join(__dirname, file);

        if (!fs.existsSync(filePath)) {
            console.log(`⚠️ ${file} not found`);
            return;
        }

        let content = fs.readFileSync(filePath, "utf-8");

        const newPrefixBlock =
            `const PREFIXES = ${JSON.stringify(groups[i], null, 4)};`;

        content = content.replace(
            /const\s+PREFIXES\s*=\s*\[[\s\S]*?\];/,
            newPrefixBlock
        );

        fs.writeFileSync(filePath, content, "utf-8");

        console.log(`✅ ${file} updated with ${groups[i].length} prefixes`);
    });
}

// ================================
// BLOCK 4 — FETCH EVENT IDS
// ================================

const SPORTY_EVENTS_URL =
    "https://www.sportybet.com/api/ng/factsCenter/pcEvents";

async function fetchEventIds(tournamentId) {
    const payload = [
        {
            sportId: "sr:sport:1",
            marketId: "1,18,10,29,11,26,36,14",
            tournamentId: [[tournamentId]]
        }
    ];

    const { statusCode, body } = await request(SPORTY_EVENTS_URL, {
        method: "POST",
        dispatcher: agent,
        headers: {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
            "Accept": "application/json, text/plain, */*",
            "Content-Type": "application/json",
            "Referer": "https://www.sportybet.com/ng/m/code-hub/codes",
            "Origin": "https://www.sportybet.com"
        },
        body: JSON.stringify(payload)
    });

    if (statusCode !== 200) {
        console.log("Event Fetch Status:", statusCode);
        throw new Error("Failed fetching events");
    }

    const json = await body.json();
    const events = json?.data?.[0]?.events ?? [];

    const validEvents = [];

    for (const ev of events) {
        if (ev.matchStatus !== "Not start") continue;

        const markets = {};

        for (const m of ev.markets ?? []) {
            if (!m.id || !m.outcomes?.length) continue;

            markets[m.id] = {
                specifier: m.specifier ?? "",
                outcomes: m.outcomes.map(o => ({
                    id: o.id,
                    desc: o.desc ?? ""
                }))
            };
        }

        if (Object.keys(markets).length > 0) {
            validEvents.push({
                eventId: ev.eventId,
                markets
            });
        }
    }

    return validEvents;
}

// ================================
// BLOCK 5 — GENERATE SELECTIONS
// ================================

function randomItem(arr) {
    return arr[Math.floor(Math.random() * arr.length)];
}

function generateRandomSelections(eventList) {
    const numberOfSelections = Math.floor(Math.random() * 4) + 5;
    const selections = [];

    for (let i = 0; i < numberOfSelections; i++) {
        const ev = randomItem(eventList);

        const marketId = randomItem(Object.keys(ev.markets));
        const market = ev.markets[marketId];

        const outcome = randomItem(market.outcomes);

        selections.push({
            eventId: ev.eventId,
            marketId,
            specifier: market.specifier ?? "",
            outcomeId: outcome.id
        });
    }

    return selections;
}
// ================================
// BLOCK 6 — SHARE REQUEST
// ================================

const SHARE_URL ="https://www.sportybet.com/api/ng/orders/share";

async function makePostRequest(eventList) {
    const payload = {
        selections: generateRandomSelections(eventList)
    };

    const { statusCode, body } = await request(SHARE_URL, {
        method: "POST",
        dispatcher: agent,
        headers: {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
            "Accept": "application/json, text/plain, */*",
            "Content-Type": "application/json",
            "Referer": "https://www.sportybet.com/ng/m/code-hub/codes",
            "Origin": "https://www.sportybet.com"
        },
        body: JSON.stringify(payload)
    });

    if (statusCode !== 200) {
        console.log("Share Status:", statusCode);
        const text = await body.text();
        console.log("Share Body:", text);
        throw new Error("Share request failed");
    }

    const json = await body.json();
    const shareCode = json?.data?.shareCode;

    if (!shareCode) {
        console.log("Share response:", json);
        throw new Error("shareCode missing");
    }

    return shareCode;
}

// ================================
// BLOCK 7 — PARALLEL SHARE ENGINE
// ================================

const CONCURRENCY_LIMIT = 35; // 🔥 Increase for more speed (be careful with rate limits)

async function generateShareCodes(eventList) {
    const collectedCodes = [];
    const prefixSet = new Set();

    let STOP = false;

    async function worker(id) {
        while (!STOP) {
            try {
                const code = await makePostRequest(eventList);

                if (STOP) return;

                const prefix = extractPrefix(code);

                if (!prefixSet.has(prefix)) {
                    prefixSet.add(prefix);
                    collectedCodes.push(code);

                    console.log(
                        `[Worker ${id}] ✅ ${prefixSet.size}/${TOTAL_PREFIXES_NEEDED} → ${prefix}`
                    );

                    if (prefixSet.size >= TOTAL_PREFIXES_NEEDED) {
                        STOP = true;
                        return;
                    }
                }

            } catch (err) {
                if (!STOP) {
                    console.log(`[Worker ${id}] ⚠️ ${err.message}`);
                }
            }
        }
    }




    // Launch workers in parallel
    const workers = Array.from({ length: CONCURRENCY_LIMIT }, (_, i) => worker(i+1));
    await Promise.all(workers);


    return collectedCodes;
}
function splitPrefixes(prefixes, perFile) {
    const result = [];
    let index = 0;

    for (let i = 0; i < WORKER_FILES.length; i++) {
        result.push(prefixes.slice(index, index + perFile));
        index += perFile;
    }

    return result;
}

// ================================
// BLOCK 8 — PREFIX ANALYZER
// ================================

function analyzePrefixes(codes) {
    const codeMap = new Map();
    const prefixMap = new Map();

    for (const code of codes) {
        // Full code frequency
        codeMap.set(code, (codeMap.get(code) || 0) + 1);

        // Prefix frequency
        const prefix = extractPrefix(code);
        prefixMap.set(prefix, (prefixMap.get(prefix) || 0) + 1);
    }

    console.log("\n======== SUMMARY ========");
    console.log("Total Codes:", codes.length);
    console.log("Unique Codes:", codeMap.size);
    console.log("Unique Prefixes:", prefixMap.size);
    console.log("Duplicate Codes:", codes.length - codeMap.size);
    console.log("Duplicate Prefix Hits:", codes.length - prefixMap.size);

    return {
        uniquePrefixes: Array.from(prefixMap.keys()),
        prefixMap
    };
}
// ================================
// BLOCK 9 — MASTER CONTROLLER
// ================================

async function startScraper(eventList) {
    console.log("🚀 Starting share generation...\n");

    const codes = await generateShareCodes(eventList);

    console.log("\n🔎 Analyzing results...");
    const { uniquePrefixes } = analyzePrefixes(codes);

    // Trim to requested size (extra safety)
    const trimmed = uniquePrefixes.slice(0, TOTAL_PREFIXES_NEEDED);
    PREFIXES = trimmed;
    updateAllWorkerFiles(trimmed);

    console.log("\n🎯 DONE — Prefix target achieved.");
}
// ================================
// BLOCK 10 — EVENT COLLECTOR
// ================================

const TOURNAMENT_IDS = [
    "sr:tournament:176", "sr:tournament:1083",
    "sr:tournament:19724", "sr:tournament:720",
    "sr:tournament:202", "sr:tournament:215",
    "sr:tournament:1117", "sr:tournament:27092",
    "sr:tournament:40817", "sr:tournament:17",
    "sr:tournament:37", "sr:tournament:52"
    // You can paste the full list here like in Python
];

const EVENT_FETCH_RUNS = 30; // same as RUNS in Python

async function collectEvents() {
    console.log("📡 Collecting events...\n");

    const tasks = [];

    for (let i = 0; i < EVENT_FETCH_RUNS; i++) {
        const randomTournament =
            TOURNAMENT_IDS[Math.floor(Math.random() * TOURNAMENT_IDS.length)];

        tasks.push(fetchEventIds(randomTournament));
    }

    const results = await Promise.all(tasks);

    const EVENT_LIST = [];

    for (const eventArray of results) {
        for (const ev of eventArray) {
            EVENT_LIST.push(ev);
        }
    }

    console.log("✅ Total Events Collected:", EVENT_LIST.length);

    if (EVENT_LIST.length === 0) {
        throw new Error("No events collected.");
    }

    return EVENT_LIST;
}
// ================================
// BLOCK 11 — EXECUTION PIPELINE
// ================================

async function run() {
    try {
        const eventList = await collectEvents();

        await startScraper(eventList);

    } catch (err) {
        console.error("❌ Fatal Error:", err.message);
    }
}
// ================================
// BLOCK 12 — ENTRY POINT
// ================================

run();
