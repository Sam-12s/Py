import sys
import asyncio
import os
import random
import re
import aiohttp
from datetime import datetime, timedelta, timezone
import time
sys.stdout.reconfigure(line_buffering=True)

WORKER_FILES = [
    "AV1.py",
    "AV2.py",
    "AV3.py",
    "AV4.py",
    "AV5.py",
    "AV6.py",
    "AV7.py",
    "AV8.py",
    "AV9.py",
    "AV10.py",
    "AV11.py",
    "AV12.py",
    "AV13.py",
    "AV14.py",
    "AV15.py",
    "AV16.py",
    "AV17.py",
    "AV18.py",
]

DEFAULT_PAYLOAD = {
    "defaultList": True,
    "featureCodeMarket": "NON_BB",
    "foldsFilter": [1, 50],
    "i": 1,
    "oddsFilter": [1, 2147483647],
    "sortBy": {"index": 1, "field": "popularity", "order": "asc"},
    "timeFilter": []  # will be filled dynamically
}

def get_current_week_timestamps():
    """Return list of UTC timestamps (ms) for Monday → Sunday of current week."""
    today = datetime.now(timezone.utc)
    monday = today - timedelta(days=today.weekday())  # start of week
    timestamps = []
    for i in range(7):
        day = monday + timedelta(days=i)
        ts_ms = int(day.replace(hour=0, minute=0, second=0, microsecond=0).timestamp() * 1000)
        timestamps.append(ts_ms)
    return timestamps

# List of event IDs to randomly choose from for each selection
EVENT_ID_LIST = []

tournaments_id = ['sr:tournament:176', 'sr:tournament:1083', 'sr:tournament:19724', 'sr:tournament:720', 'sr:tournament:202', 'sr:tournament:215', 'sr:tournament:1117', 'sr:tournament:27092', 'sr:tournament:40817', 'sr:tournament:17', 'sr:tournament:37', 'sr:tournament:52', 'sr:tournament:210', 'sr:tournament:1644', 'sr:tournament:238', 'sr:tournament:27414', 'sr:tournament:1032', 'sr:tournament:709', 'sr:tournament:30913', 'sr:tournament:254', 'sr:tournament:1139', 'sr:tournament:27464', 'sr:tournament:97', 'sr:tournament:33980', 'sr:tournament:155', 'sr:tournament:19238', 'sr:tournament:634', 'sr:tournament:718', 'sr:tournament:1892', 'sr:tournament:7', 'sr:tournament:34228', 'sr:tournament:39', 'sr:tournament:544', 'sr:tournament:136', 'sr:tournament:27821', 'sr:tournament:218', 'sr:tournament:187', 'sr:tournament:14864', 'sr:tournament:727', 'sr:tournament:347', 'sr:tournament:270', 'sr:simple_tournament:11843', 'sr:tournament:172', 'sr:tournament:19278', 'sr:tournament:1191', 'sr:tournament:152', 'sr:tournament:171', 'sr:tournament:742', 'sr:tournament:629', 'sr:tournament:406', 'sr:tournament:170', 'sr:tournament:48933', 'sr:tournament:186', 'sr:tournament:27088', 'sr:tournament:20162', 'sr:tournament:200', 'sr:tournament:222', 'sr:tournament:27396', 'sr:tournament:402', 'sr:tournament:2386', 'sr:tournament:217', 'sr:tournament:365', 'sr:tournament:32217', 'sr:tournament:1015', 'sr:tournament:240', 'sr:tournament:19492', 'sr:tournament:594', 'sr:tournament:45', 'sr:tournament:212', 'sr:tournament:1588', 'sr:tournament:23551', 'sr:tournament:27072', 'sr:tournament:955', 'sr:tournament:203', 'sr:tournament:211', 'sr:tournament:23']

# How many times the script should run
RUNS = 30

# REPLACE this function in your script
async def fetch_event_ids(session, t_id):
    url = "https://www.sportybet.com/api/ng/factsCenter/pcEvents"

    payload = [
        {
            "sportId": "sr:sport:1",
            "marketId": "1,18,10,29,11,26,36,14",
            "tournamentId": [[t_id]]
        }
    ]

    async with session.post(url, json=payload) as resp:
        resp.raise_for_status()
        data = await resp.json()

        try:
            events = data["data"][0]["events"]
        except Exception:
            return []

        valid = []
        for ev in events:
            if ev.get("matchStatus") != "Not start":
                continue

            markets = {}
            for m in ev.get("markets", []):

                market_id = m.get("id")

                # MARKET-LEVEL SPECIFIER (the important part)
                market_specifier = m.get("specifier", "") or ""

                outcomes = []
                for o in m.get("outcomes", []):
                    outcomes.append({
                        "id": o.get("id"),
                        "desc": o.get("desc", "")
                    })

                if market_id and outcomes:
                    markets[market_id] = {
                        "specifier": market_specifier,
                        "outcomes": outcomes
                    }

            if markets:
                valid.append({
                    "eventId": ev["eventId"],
                    "markets": markets
                })

        return valid

# REPLACE this function in your script
def generate_random_selections():
    number_of_selections = random.randint(1, 4)
    selections = []

    for _ in range(number_of_selections):

        # pick a real event
        ev = random.choice(EVENT_ID_LIST)
        event_id = ev["eventId"]
        markets = ev["markets"]

        # pick real market
        market_id = random.choice(list(markets.keys()))
        market = markets[market_id]

        # pick real outcome
        outcome_obj = random.choice(market["outcomes"])
        outcome_id = outcome_obj["id"]

        # specifier belongs to the MARKET, not the outcome
        specifier = market.get("specifier", "") or ""

        selections.append({
            "eventId": event_id,
            "marketId": market_id,
            "specifier": specifier,
            "outcomeId": outcome_id
        })

    return selections
async def make_post_request(session):
    url = "https://www.sportybet.com/api/ng/orders/share"
    payload = {"selections": generate_random_selections()}

    async with session.post(url, json=payload) as resp:
        resp.raise_for_status()
        j = await resp.json(content_type=None)

        if not isinstance(j, dict):
            raise ValueError("Invalid response, not JSON dict")

        data = j.get("data")
        if not data or not isinstance(data, dict):
            raise KeyError("'data' missing")

        code = data.get("shareCode")
        if not code:
            raise KeyError("'shareCode' missing")

        return code

async def run_until_success(session, attempt_index):
    tries = 0
    while True:
        tries += 1
        try:
            code = await make_post_request(session)
            print(f"[OK] Attempt {attempt_index} => {code}")
            return code
        except Exception as e:
            print(f"[ERROR] Attempt {attempt_index} (retry {tries}): {e}")

async def run_multiple_times(times):
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json",
        "Referer": "https://www.sportybet.com/ng/m/code-hub/codes"
    }

    results = []
    start = time.time()

    async with aiohttp.ClientSession(headers=headers) as session:
        for i in range(1, times + 1):
            code = await run_until_success(session, i)
            results.append(code)


    print(f"\nCollected {len(results)} codes in {time.time() - start:.1f}s")
    return results

def split_codes_among_files(nletter, n=1):
    random.shuffle(nletter)
    base_size = len(nletter) // n
    remainder = len(nletter) % n
    result = []
    start = 0
    for i in range(n):
        extra = 1 if i < remainder else 0
        end = start + base_size + extra
        result.append(nletter[start:end])
        start = end
    return result

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

WORKER_FILES = [os.path.join(BASE_DIR, f"AV{i}.py") for i in range(1, 19)]

def update_bxx_file(fname, new_codes):
    if not os.path.exists(fname):
        print(f"⚠️ {os.path.basename(fname)} not found, skipping.")
        return

    with open(fname, 'r', encoding='utf-8') as f:
        content = f.read()

    new_list_str = f"PREFIXES = {new_codes!r}"
    content, count = re.subn(r'PREFIXES\s*=\s*\[.*?]', new_list_str, content, flags=re.DOTALL)


    if count == 0:
        print("Fails")
        return

    # Remove pop_random_code_from_file and loading loop
    content = re.sub(
        r'def\s+pop_random_code_from_file\s*\(.*?\)\s*:\s*(?:\n\s+.*)+?(?=\n\S)',
        '',
        content,
        flags=re.DOTALL
    )
    content = re.sub(
        r'print\("⏳ Loading real prefixes.*?print\(f"✅ Loaded.*?\n',
        '',
        content,
        flags=re.DOTALL
    )

    with open(fname, 'w', encoding='utf-8') as f:
        f.write(content)

    print(f"✅ {fname}: updated list with {len(new_codes)} codes and removed unused logic.")

def update_all_bxx(prefixes):
    if not prefixes:
        print("❌ No prefixes to distribute")
        return

    n = len(WORKER_FILES)
    groups = split_codes_among_files(prefixes, n=n)

    for fname, group in zip(WORKER_FILES, groups):
        update_bxx_file(fname, group)

    print("\n✅ PREFIX DISTRIBUTION COMPLETE")
    for fname, group in zip(WORKER_FILES, groups):
        print(f"{fname}: {len(group)} prefixes")

from collections import Counter

async def fetcher():
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json",
        "Referer": "https://www.sportybet.com/ng/m/code-hub/codes"
    }

    EVENT_ID_LIST.clear()

    # 🔹 STEP 1: Collect event IDs
    async with aiohttp.ClientSession(headers=headers) as session:
        tasks = []
        for _ in range(RUNS):
            t_id = random.choice(tournaments_id)
            tasks.append(fetch_event_ids(session, t_id))

        results = await asyncio.gather(*tasks)

        for event_list in results:
            for ev in event_list:
                EVENT_ID_LIST.append(ev)

    print("\n======== EVENT COLLECTION ========")
    print("TOTAL EVENTS COLLECTED:", len(EVENT_ID_LIST))

    if not EVENT_ID_LIST:
        print("❌ No events collected. Aborting fetcher.")
        return []

    # 🔹 STEP 2: Generate share codes
    times = 510 # number of attempts
    codes = await run_multiple_times(times)

    # 🔹 STEP 3: Analyze duplicates (FULL CODES)
    code_counter = Counter(codes)

    # 🔹 STEP 4: Analyze prefix frequency
    prefix_counter = Counter(code[:-3] for code in codes)

    # 🔹 STEP 5: REPORTS
    print("\n======== DUPLICATE CODE REPORT ========")
    duplicate_found = False
    for code, count in code_counter.items():
        if count > 1:
            duplicate_found = True
            print(f"{code} → appeared {count} times")

    if not duplicate_found:
        print("✅ No duplicate full codes found.")

    print("\n======== PREFIX FREQUENCY REPORT ========")
    for prefix, count in prefix_counter.most_common():
        print(f"{prefix} → {count} occurrence(s)")

    # 🔹 STEP 6: SUMMARY
    print("\n======== SUMMARY ========")
    print("Total trials:", times)
    print("Unique codes:", len(code_counter))
    print("Duplicate codes:", sum(1 for c in code_counter.values() if c > 1))
    print("Unique prefixes:", len(prefix_counter))
    print("Duplicate prefix appearances:", times - len(prefix_counter))

    # 🔹 STEP 7: UNIQUE PREFIX LIST (used elsewhere in your script)
    unique_prefixes = list(prefix_counter.keys())

    print("\n======== UNIQUE PREFIX LIST ========")
    print(unique_prefixes)

    return unique_prefixes


def main():
    trimmed_codes = asyncio.run(fetcher())
    update_all_bxx(trimmed_codes)


if __name__ == "__main__":
    main()




