// ================================
// BLOCK 1 — IMPORTS
// ================================
const fs = require("fs");
const path = require("path");

// ================================
// BLOCK 2 — CONFIG
// ================================

const TOTAL_SCRAPPERS = 63;

const WORKER_FILES = Array.from(
  { length: TOTAL_SCRAPPERS },
  (_, i) => `scrapper${i + 1}.js`
);

// 34 allowed characters
const suffix_char = [
"1","2","3","4","5","6","7","8","9","0",
"A","B","C","D","E","F","G","H",
"J","K","L","M","N",
"P","Q","R","S","T","U","V","W","X","Y","Z"
];

// ================================
// BLOCK 3 — GENERATE COMBINATIONS
// ================================

function generateAllCombinations() {
    const combos = [];

    for (const a of suffix_char) {
        for (const b of suffix_char) {
            combos.push(a + b);
        }
    }

    return combos; // 1156 combos
}

// ================================
// BLOCK 4 — RANDOM SHUFFLE
// ================================

function shuffle(array) {
    for (let i = array.length - 1; i > 0; i--) {
        const j = Math.floor(Math.random() * (i + 1));
        [array[i], array[j]] = [array[j], array[i]];
    }
}

// ================================
// BLOCK 5 — DISTRIBUTE TO SCRAPERS
// ================================

function distributePrefixes(combos) {

    shuffle(combos);

    const groups = Array.from({ length: TOTAL_SCRAPPERS }, () => []);

    combos.forEach((combo, index) => {
        const workerIndex = index % TOTAL_SCRAPPERS;
        groups[workerIndex].push(combo);
    });

    return groups;
}

// ================================
// BLOCK 6 — UPDATE WORKER FILES
// ================================

function updateAllWorkerFiles(groups) {

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
// BLOCK 7 — MAIN
// ================================

function run() {

    console.log("⚙️ Generating combinations...");

    const combos = generateAllCombinations();

    console.log("Total combinations:", combos.length); // 1156

    const groups = distributePrefixes(combos);

    console.log("Updating worker files...\n");

    updateAllWorkerFiles(groups);

    console.log("\n🎯 DONE — All combinations distributed.");
}

run();
