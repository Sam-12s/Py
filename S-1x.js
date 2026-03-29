const { chromium } = require("playwright");

const PROXY_USERNAME = "spfsvdt89u";
const PROXY_PASSWORD = "f25gv0_jbagu1ZPLMb";
const PROXY_HOST = "dc.decodo.com";

const START_PORT = 10001;
const END_PORT = 10200;

const TEST_DURATION = 5; // seconds per step

function buildHeaders() {
  return {
    "accept": "application/json, text/plain, */*",
    "content-type": "application/json",
    "origin": "https://indi-1xbet.com",
    "referer": "https://indi-1xbet.com/en",
    "user-agent":
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0",
    "x-requested-with": "XMLHttpRequest"
  };
}

async function testProxy(port) {
  console.log(`\n🔎 Testing proxy port ${port}`);

  const browser = await chromium.launch({
    headless: true,
    proxy: {
      server: `http://${PROXY_HOST}:${port}`,
      username: PROXY_USERNAME,
      password: PROXY_PASSWORD
    }
  });

  const ctx = await browser.newContext();
  const request = ctx.request;

  let rps = 1;
  let safeRps = 1;

  while (true) {
    console.log(`   ⚡ Testing ${rps} req/sec`);

    let errors529 = 0;
    let total = 0;

    const start = Date.now();

    while ((Date.now() - start) / 1000 < TEST_DURATION) {

      const promises = [];

      for (let i = 0; i < rps; i++) {
        promises.push(
          request.post(
            "https://indi-1xbet.com/service-api/LiveBet/Open/GetCoupon",
            {
              headers: buildHeaders(),
              data: JSON.stringify({
                Guid: "2ABCD",
                Lng: "en",
                partner: 71
              }),
              timeout: 10000
            }
          )
          .then(res => {
            if (res.status() === 529) errors529++;
          })
          .catch(() => {})
        );
      }

      await Promise.all(promises);

      total += rps;

      await new Promise(r => setTimeout(r, 1000));
    }

    console.log(
      `      total=${total} 529=${errors529}`
    );

    if (errors529 > 0) {
      console.log(`❌ Limit reached at ${rps} RPS`);
      break;
    }

    safeRps = rps;
    rps++;
  }

  await browser.close();

  console.log(`✅ SAFE RPS = ${safeRps}`);

  return safeRps;
}

(async () => {

  const results = {};

  for (let port = START_PORT; port <= END_PORT; port++) {

    const safe = await testProxy(port);

    results[port] = safe;

  }

  console.log("\n========= FINAL RESULTS =========");

  console.log(results);

})();
