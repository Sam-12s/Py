import sys
import httpx
import asyncio
import sqlite3
from datetime import datetime, timedelta
import smtplib
from email.message import EmailMessage
from pathlib import Path
import random
sys.stdout.reconfigure(line_buffering=True)


MAX_RUNTIME_MINUTES = 355  # ⏱️ CHANGE THIS
START_TIME = datetime.now()
END_TIME = START_TIME + timedelta(minutes=MAX_RUNTIME_MINUTES)

STOP_EVENT = asyncio.Event()
MAX_INITIAL_INVALID = 40


def init_db(db_name="OUTPUT.db"):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    cursor.execute("PRAGMA journal_mode=WAL;")
    cursor.execute("PRAGMA synchronous=NORMAL;")
    cursor.execute("PRAGMA cache_size=-50000;")  # ~50MB memory
    cursor.execute("PRAGMA temp_store=MEMORY;")  # faster
    cursor.execute("PRAGMA locking_mode=EXCLUSIVE;")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS codes (
            ID INTEGER PRIMARY KEY AUTOINCREMENT,
            Worker_Id TEXT,
            Label TEXT,
            Code TEXT UNIQUE,
            Teams TEXT,   -- store teams as string, e.g. "Man Utd vs Chelsea"
            Events TEXT,
            Score TEXT,
            Time TEXT,
            Odds TEXT,
            Total_odds TEXT,
            Last_change TEXT,
            Timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    conn.close()

PREFIXES = ['SMU', 'KRF', 'G4F', 'UY5', 'K59', 'QH0', 'WZX', 'NUQ', 'JX7', 'MEZ', 'JNN', 'S3G', 'XR1', 'MA3', 'MV2', 'XY9', 'K41', 'N6P', 'GQ8', 'UUH', 'KBD', 'GPE', 'ZZH', 'N3M', 'J6U']

USER_AGENTS = [
    # Desktop browsers
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Firefox/118.0",
    "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:102.0) Gecko/20100101 Firefox/102.0",
    # Mobile browsers
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Linux; Android 11; Pixel 4 XL) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Mobile Safari/537.36",
]

SUFFIX_CHARS = [
    '0','1','2','3','4','5','6','7','8','9',
    'A','B','C','D','E','F','G','H','J','K','L',
    'M','N','P','Q','R','S','T','U','V','W',
    'X','Y','Z'
]

MAX_PARALLEL_PREFIXES = int(len(PREFIXES))  # start with 2–4

PREFIX_SEMAPHORE = asyncio.Semaphore(MAX_PARALLEL_PREFIXES)

FAILED_CODES_FILE = "failed_403_codes.log"

def save_failed_code(worker_id, code, reason):
    line = f"{datetime.now().isoformat()} | {worker_id} | {code} | {reason}\n"
    with open(FAILED_CODES_FILE, "a", buffering=1) as f:
        f.write(line)

async def fetch_code(local_code, client, session_id):


    url = f"https://www.sportybet.com/api/ng/orders/share/{local_code}"

    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Referer": "https://www.sportybet.com/",
        "Origin": "https://www.sportybet.com",
    }


    resp = await client.get(url, headers=headers)


    content_type = resp.headers.get("Content-Type", "")
    text = resp.text.strip()

    if resp.status_code == 403:
        return "ERROR_403"

    if resp.status_code !=200:
        return "ERROR_RETRY"

    if content_type.startswith("application/json"):
        try:
            response = resp.json()
        except Exception as e:
            print(f"[{session_id}] JSON decode error: {e} | Raw: {text[:200]}")
            
            return True

        if not response:
            print("Empty response")
            
            return False

        if response["message"] == 'The code is invalid.':
            print(response["message"], local_code, "-----B02", str(session_id), flush=True)
            return "INVALID"

        elif response["message"] == 'Success':
            pg = response["data"]["outcomes"]
            number_of_event = len(pg)


            if number_of_event > 3:
                print("Retry", "-----", str(session_id), flush=True)
                return "VALID"

            elif number_of_event == 3:
                try:
                    types = str(pg[0]["sport"]["category"]["name"])
                    types1 = str(pg[1]["sport"]["category"]["name"])
                    types2 = str(pg[2]["sport"]["category"]["name"])
                    event = str(pg[0]["markets"][0]["desc"])
                    event1 = str(pg[1]["markets"][0]["desc"])
                    event2 = str(pg[2]["markets"][0]["desc"])
                    date = int(pg[0]['estimateStartTime'])
                    date1 = int(pg[1]['estimateStartTime'])
                    date2 = int(pg[2]['estimateStartTime'])
                    timestamp = date / 1000
                    timestamp1 = date1 / 1000
                    timestamp2 = date2 / 1000
                    match_date = datetime.fromtimestamp(timestamp).date()
                    match_date1 = datetime.fromtimestamp(timestamp1).date()
                    match_date2 = datetime.fromtimestamp(timestamp2).date()
                    match_time = datetime.fromtimestamp(timestamp).time()
                    match_time1 = datetime.fromtimestamp(timestamp1).time()
                    match_time2 = datetime.fromtimestamp(timestamp2).time()
                    match_times = f"{match_time}||{match_time1}||{match_time2}"
                    change = int(pg[0]["markets"][0]["lastOddsChangeTime"])
                    change1 = int(pg[1]["markets"][0]["lastOddsChangeTime"])
                    change2 = int(pg[2]["markets"][0]["lastOddsChangeTime"])
                    match_odd_changes = datetime.fromtimestamp(change/1000).strftime("%H:%M:%S")
                    match_odd_changes1 = datetime.fromtimestamp(change1/1000).strftime("%H:%M:%S")
                    match_odd_changes2 = datetime.fromtimestamp(change2 / 1000).strftime("%H:%M:%S")
                    score =  pg[0]["markets"][0]["outcomes"][0]["desc"]
                    score1 = pg[1]["markets"][0]["outcomes"][0]["desc"]
                    score2 = pg[2]["markets"][0]["outcomes"][0]["desc"]
                    outcomes = f"{score}|{score1}|{score2}"
                    today_date = datetime.now().date()
                    valid = pg[0]['matchStatus']
                    odd = str(pg[0]["markets"][0]["outcomes"][0]["odds"])
                    odd1 = str(pg[1]["markets"][0]["outcomes"][0]["odds"])
                    odd2 = str(pg[2]["markets"][0]["outcomes"][0]["odds"])
                    calculate_odds = (float(odd) * float(odd1) * float(odd2))
                    odds = f"{odd}|{odd1}|{odd2}"
                    total_odd = str(calculate_odds)
                    change_times = f"{match_odd_changes}||{match_odd_changes1}||{match_odd_changes2}"
                    events = f"{event}|{event1}|{event2}"
                except KeyError as ff:
                    print(ff, '1')
                    return False

                if (valid in ["Not start", "H1"]) and match_date == today_date and match_date1 == today_date and match_date2 == today_date and calculate_odds > 15.00 and types != "Simulated Reality League" and types1 != "Simulated Reality League" and types2 != "Simulated Reality League":
                    teams1 = f"{pg[0]['homeTeamName']} vs {pg[0]['awayTeamName']}"
                    teams2 = f"{pg[1]['homeTeamName']} vs {pg[1]['awayTeamName']}"
                    teams3 = f"{pg[2]['homeTeamName']} vs {pg[2]['awayTeamName']}"
                    teams = f"{teams1}|{teams2}|{teams3}"
                    init_db()
                    log_code("TRIPLE", local_code, session_id, teams,events, outcomes,match_times,odds,total_odd,change_times)
                    print("TRIPLE ODD")
                    return False
                else:
                    print(f"FORGET--------{local_code}")
                    return False

            elif number_of_event == 2:
                try:
                    types = str(pg[0]["sport"]["category"]["name"])
                    types1 = str(pg[1]["sport"]["category"]["name"])
                    date = int(pg[0]['estimateStartTime'])
                    date1 = int(pg[1]['estimateStartTime'])
                    timestamp = date / 1000
                    timestamp1 = date1 / 1000
                    match_date = datetime.fromtimestamp(timestamp).date()
                    match_date1 = datetime.fromtimestamp(timestamp1).date()
                    match_time = datetime.fromtimestamp(timestamp).time()
                    match_time1 = datetime.fromtimestamp(timestamp1).time()
                    match_times = f"{match_time}||{match_time1}"
                    change = int(pg[0]["markets"][0]["lastOddsChangeTime"])
                    change1 = int(pg[1]["markets"][0]["lastOddsChangeTime"])
                    match_odd_changes = datetime.fromtimestamp(change/1000).strftime("%H:%M:%S")
                    match_odd_changes1 = datetime.fromtimestamp(change1/1000).strftime("%H:%M:%S")
                    score =  pg[0]["markets"][0]["outcomes"][0]["desc"]
                    score1 = pg[1]["markets"][0]["outcomes"][0]["desc"]
                    scores = f"{score}|{score1}"
                    today_date = datetime.now().date()
                    valid = pg[0]['matchStatus']
                    odd = str(pg[0]["markets"][0]["outcomes"][0]["odds"])
                    odd1 = str(pg[1]["markets"][0]["outcomes"][0]["odds"])
                    calculate_odds = (float(odd) * float(odd1))
                    odds = f"{odd}|{odd1}"
                    total_odd = str(calculate_odds)
                    change_times = f"{match_odd_changes}||{match_odd_changes1}"
                    event = str(pg[0]["markets"][0]["desc"])
                    event1 = str(pg[1]["markets"][0]["desc"])
                    events = f"{event}|{event1}"
                except KeyError as ff:
                    print(ff, '1')
                    return False

                if  (valid in ["Not start", "H1"]) and event == "Correct Score" and event1 == "Correct Score"and match_date == today_date and match_date1 == today_date and types != "Simulated Reality League" and types1 != "Simulated Reality League":
                    teams1 = f"{pg[0]['homeTeamName']} vs {pg[0]['awayTeamName']}"
                    teams2 = f"{pg[1]['homeTeamName']} vs {pg[1]['awayTeamName']}"
                    teams = f"{teams1}|{teams2}"
                    init_db()
                    log_code("DOUBLE", local_code, session_id, teams,events,scores,match_times,odds,total_odd,change_times)
                    print("DOUBLE ODD")
                    return False
                elif (valid in ["Not start", "H1"]) and match_date == today_date and match_date1 == today_date and calculate_odds > 15.00 and types != "Simulated Reality League" and types1 != "Simulated Reality League":
                    teams1 = f"{pg[0]['homeTeamName']} vs {pg[0]['awayTeamName']}"
                    teams2 = f"{pg[1]['homeTeamName']} vs {pg[1]['awayTeamName']}"
                    teams = f"{teams1}|{teams2}"
                    init_db()
                    log_code("X/1X2", local_code, session_id, teams,events,scores,match_times,odds,total_odd,change_times)
                    print("X/1X2")
                    return False
                else:
                    print("FORGET")
                    return False
            elif number_of_event == 1:
                try:
                    types = str(pg[0]["sport"]["category"]["name"])
                    date23 = int(pg[0]['estimateStartTime'])
                    timestamp23 = date23 / 1000
                    match_date23 = datetime.fromtimestamp(timestamp23).date()
                    today_date = datetime.now().date()
                    valid23 = pg[0]['matchStatus']
                    match_time23 = datetime.fromtimestamp(timestamp23).time()
                    score2 = pg[0]["markets"][0]["outcomes"][0]["desc"]
                    score23 = f"{score2}"
                    odd23 = str(pg[0]["markets"][0]["outcomes"][0]["odds"])
                    change23 = int(pg[0]["markets"][0]["lastOddsChangeTime"])
                    match_odd_changes23 = datetime.fromtimestamp(change23/1000).strftime("%H:%M:%S")
                    change_times23 = f"{match_odd_changes23}"
                    odds23 = f"{odd23}"
                    event = str(pg[0]["markets"][0]["desc"])
                except KeyError as ff3:
                    print(ff3, '3')
                    return False

                if valid23 in ["Not start", "H1"] and event == "Correct Score" and match_date23 == today_date and types != "Simulated Reality League":
                    teams = f"{pg[0]['homeTeamName']} vs {pg[0]['awayTeamName']}"
                    init_db()
                    log_code("SINGLE", local_code, session_id, teams,event,score23,match_time23,odds23,odds23,change_times23)
                    print("SINGLE ODD")

                    return False
                else:
                    print("Ha")
                    return False

            else:
                print("wow-1")
            return "VALID"
        else:
            print("wow")

            return False

    # ✅ XML fallback with EXACT JSON-like structure
    elif text.startswith("<BaseRsp"):
        import xml.etree.ElementTree as ET
        try:
            root = ET.fromstring(text)
            message = root.findtext("message", "")
            if message != "Success":
                print(f"{message} {local_code}-----B02", str(session_id), flush=True)
                return "INVALID"

            outcomes = root.findall(".//data/outcomes/outcomes")
            if not outcomes:
                print("No usable XML outcomes")

                return "VALID"

            number_of_event = len(outcomes)

            def find(elem, path):
                node = elem.find(path)
                return node.text if node is not None else None

            if number_of_event > 3:
                print("Retry", "-----", str(session_id), flush=True)
                return "VALID"


            if number_of_event == 3:

                try:
                    types = (find(outcomes[0], "sport/category/name"))
                    types1 = (find(outcomes[1], "sport/category/name"))
                    types2 = (find(outcomes[2], "sport/category/name"))
                    date = int(find(outcomes[0], "estimateStartTime"))
                    date1 = int(find(outcomes[1], "estimateStartTime"))
                    date2 = int(find(outcomes[2], "estimateStartTime"))
                    match_date = datetime.fromtimestamp(date / 1000).date()
                    match_date1 = datetime.fromtimestamp(date1 / 1000).date()
                    match_date2 = datetime.fromtimestamp(date2 / 1000).date()
                    match_time = datetime.fromtimestamp(date/1000).time()
                    match_time1 = datetime.fromtimestamp(date1 / 1000).time()
                    match_time2 = datetime.fromtimestamp(date2 / 1000).time()
                    today_date = datetime.now().date()
                    match_times = f"{match_time}||{match_time1}||{match_time2}"
                    change = int(find(outcomes[0], "markets/markets/lastOddsChangeTime"))
                    match_odd_changes = datetime.fromtimestamp(change/1000).strftime("%H:%M:%S")
                    change1 = int(find(outcomes[1], "markets/markets/lastOddsChangeTime"))
                    match_odd_changes1 = datetime.fromtimestamp(change1/1000).strftime("%H:%M:%S")
                    change2 = int(find(outcomes[2], "markets/markets/lastOddsChangeTime"))
                    match_odd_changes2 = datetime.fromtimestamp(change2/1000).strftime("%H:%M:%S")
                    score = find(outcomes[0],"markets/markets/outcomes/outcomes/desc")
                    score1 = find(outcomes[1],"markets/markets/outcomes/outcomes/desc")
                    score2 = find(outcomes[2], "markets/markets/outcomes/outcomes/desc")
                    scores = f"{score}|{score1}|{score2}"
                    valid = find(outcomes[0], "matchStatus")
                    event = find(outcomes[0], "markets/markets/desc")
                    event1 = find(outcomes[1], "markets/markets/desc")
                    event2 = find(outcomes[2], "markets/markets/desc")
                    events = f"{event}|{event1}|{event2}"
                    odd = find(outcomes[0], "markets/markets/outcomes/outcomes/odds")
                    odd1 = find(outcomes[1], "markets/markets/outcomes/outcomes/odds")
                    odd2 = find(outcomes[2], "markets/markets/outcomes/outcomes/odds")
                    calculate_odds = (float(odd)*float(odd1)*float(odd2))
                    odds = f"{odd}|{odd1}|{odd2}"
                    total_odd = str(calculate_odds)
                    last_change = f"{match_odd_changes}||{match_odd_changes1}||{match_odd_changes2}"
                except Exception as ff:
                    print(ff, '1(XML)')
                    return False

                if (
                        valid in ["Not start", "H1"]
                        and match_date == today_date
                        and match_date1 == today_date
                        and match_date2 == today_date
                        and types != "Simulated Reality League"
                        and types1 != "Simulated Reality League"
                        and types2 != "Simulated Reality League"
                ):
                    home1, away1 = find(outcomes[0], "homeTeamName"), find(outcomes[0], "awayTeamName")
                    home2, away2 = find(outcomes[1], "homeTeamName"), find(outcomes[1], "awayTeamName")
                    home3, away3 = find(outcomes[2], "homeTeamName"), find(outcomes[2], "awayTeamName")
                    teams = f"{home1} vs {away1} | {home2} vs {away2}| {home3} vs {away3}"
                    init_db()
                    log_code("XML-TRIPPLE", local_code, session_id, teams, events, scores,match_times,odds,total_odd,last_change)
                    print("TRIPLE ODD")
                    return "VALID"

                else:
                    print("HA (XML)")
                    return "VALID"
            elif number_of_event == 2:

                try:
                    types = (find(outcomes[0], "sport/category/name"))
                    types1 = (find(outcomes[1], "sport/category/name"))
                    date = int(find(outcomes[0], "estimateStartTime"))
                    date1 = int(find(outcomes[1], "estimateStartTime"))
                    match_date = datetime.fromtimestamp(date / 1000).date()
                    match_date1 = datetime.fromtimestamp(date1 / 1000).date()
                    match_time = datetime.fromtimestamp(date/1000).time()
                    match_time1 = datetime.fromtimestamp(date1 / 1000).time()
                    today_date = datetime.now().date()
                    match_times = f"{match_time}||{match_time1}"
                    change = int(find(outcomes[0], "markets/markets/lastOddsChangeTime"))
                    match_odd_changes = datetime.fromtimestamp(change/1000).strftime("%H:%M:%S")
                    change1 = int(find(outcomes[1], "markets/markets/lastOddsChangeTime"))
                    match_odd_changes1 = datetime.fromtimestamp(change1/1000).strftime("%H:%M:%S")
                    score = find(outcomes[0],"markets/markets/outcomes/outcomes/desc")
                    score1 = find(outcomes[1],"markets/markets/outcomes/outcomes/desc")
                    scores = f"{score}|{score1}"
                    valid = find(outcomes[0], "matchStatus")
                    odd = find(outcomes[0], "markets/markets/outcomes/outcomes/odds")
                    odd1 = find(outcomes[1], "markets/markets/outcomes/outcomes/odds")
                    calculate_odds = (float(odd)*float(odd1))
                    odds = f"{odd}|{odd1}"
                    total_odd = str(calculate_odds)
                    last_change = f"{match_odd_changes}||{match_odd_changes1}"
                    event = find(outcomes[0], "markets/markets/desc")
                    event1 = find(outcomes[1], "markets/markets/desc")
                    events = f"{event}|{event1}"
                except Exception as ff:
                    print(ff, '1(XML)')
                    return False

                if (
                        valid in ["Not start", "H1"]
                        and event == "Correct Score"
                        and event1 == "Correct Score"
                        and match_date == today_date
                        and match_date1 == today_date
                        and types != "Simulated Reality League"
                        and types1 != "Simulated Reality League"
                ):
                    home1, away1 = find(outcomes[0], "homeTeamName"), find(outcomes[0], "awayTeamName")
                    home2, away2 = find(outcomes[1], "homeTeamName"), find(outcomes[1], "awayTeamName")
                    teams = f"{home1} vs {away1} | {home2} vs {away2}"
                    init_db()
                    log_code("XML-DOUBLE", local_code, session_id, teams,events,scores,match_times,odds,total_odd,last_change)
                    print("DOUBLE ODD")
                    return False

                elif match_date == today_date and match_date1 == today_date and (valid in ["Not start", "H1"]) and calculate_odds > 15.00 and types != "Simulated Reality League"and types1 != "Simulated Reality League":
                    home1, away1 = find(outcomes[0], "homeTeamName"), find(outcomes[0], "awayTeamName")
                    home2, away2 = find(outcomes[1], "homeTeamName"), find(outcomes[1], "awayTeamName")
                    teams = f"{home1} vs {away1} | {home2} vs {away2}"
                    init_db()
                    log_code("XML-1X2/H", local_code, session_id, teams,events, scores, match_times, odds, total_odd,last_change)
                    print("1X2/H")
                    return False
                else:
                    print("HA (XML)")


                return "VALID"

            elif number_of_event == 1:

                try:
                    types = (find(outcomes[0], "sport/category/name"))
                    date23 = int(find(outcomes[0], "estimateStartTime"))
                    match_date23 = datetime.fromtimestamp(date23 / 1000).date()
                    today_date = datetime.now().date()
                    match_time23 = datetime.fromtimestamp(date23 / 1000).time()
                    score23 = find(outcomes[0], "markets/markets/outcomes/outcomes/desc")
                    valid23 = find(outcomes[0], "matchStatus")
                    event = find(outcomes[0], "markets/markets/desc")
                    odd2 = find(outcomes[0], "markets/markets/outcomes/outcomes/odds")
                    odd23 = f"{odd2}"
                    change23 = int(find(outcomes[0], "markets/markets/lastOddsChangeTime"))
                    match_odd_changes2 = datetime.fromtimestamp(change23/1000).strftime("%H:%M:%S")

                except Exception as ff3:
                    print(ff3, '3(XML)')
                    return False

                if (
                        valid23 in ["Not start", "H1"]
                        and event == "Correct Score"
                        and match_date23 == today_date
                        and types != "Simulated Reality League"

                ):
                    home = find(outcomes[0], "homeTeamName")
                    away = find(outcomes[0], "awayTeamName")
                    teams = f"{home} vs {away}"
                    init_db()
                    log_code("XML-SINGLE", local_code, session_id, teams,event, score23,match_time23,odd23,odd23,match_odd_changes2)
                    print("SINGLE ODD")

                else:
                    print("Ha (XML)")


                return "VALID"
            else:
                
                print("wow-1 (XML)")
                return False

        except Exception as e:
            print(f"[{session_id}] XML parse error: {e}")
            return True

    else:
        print(f"[{session_id}] Unrecognized response format")
        print(resp)
        print(resp.text)
        return "ERROR_RETRY"

async def fourth_worker(prefix, fourth_char, client, worker_id, start_index, step):
    print(f"[{prefix}] 🚀 Worker {worker_id} started")

    invalid_count = 0
    counting_enabled = True

    # 🔹 split 5th char space
    for i in range(start_index, len(SUFFIX_CHARS), step):
        a = SUFFIX_CHARS[i]

        if STOP_EVENT.is_set():
            break

        for b in SUFFIX_CHARS:
            if STOP_EVENT.is_set():
                break

            code = f"{prefix}{fourth_char}{a}{b}"

            while True:
                result = await fetch_code(code, client, worker_id)

                if result == "ERROR_403":
                    save_failed_code(worker_id, code, "403")
                    print(f"[{worker_id}] 🔄 403 on {code}")
                    return "NEED_CLIENT_RESET"

                if result == "ERROR_RETRY":
                    await asyncio.sleep(random.uniform(1, 3))
                    continue

                break

            await asyncio.sleep(random.uniform(26, 45))

            if counting_enabled:
                if result == "INVALID":
                    invalid_count += 1
                    if invalid_count >= MAX_INITIAL_INVALID:
                        print(f"[{worker_id}] 🛑 stopped after INVALID limit")
                        return

                elif result == "VALID":
                    counting_enabled = False
                    invalid_count = 0
                    print(f"[{worker_id}] 🔓 unlocked")

    print(f"[{prefix}] ✅ Worker {fourth_char} finished normally")

async def process_prefix(prefix):
    async with PREFIX_SEMAPHORE:
        print(f"\n🔐 STARTING PREFIX {prefix}")

        while not STOP_EVENT.is_set():

            # 🔑 NEW CLIENT = NEW TLS HANDSHAKE
            async with httpx.AsyncClient(
                http2=False,
                timeout=httpx.Timeout(200.0, connect=50.0),
                limits=httpx.Limits(
                    max_connections=68,
                    max_keepalive_connections=68
                ),
                headers={
                    "User-Agent": random.choice(USER_AGENTS),
                    "Accept": "application/json, text/plain, */*",
                    "Accept-Language": "en-US,en;q=0.9",
                    "Referer": "https://www.sportybet.com/",
                    "Origin": "https://www.sportybet.com",
                    "Connection": "keep-alive",
                }
            ) as client:

                # 🚀 START ALL WORKERS FOR THIS PREFIX
                tasks = []

                for fourth in SUFFIX_CHARS:
                    # Worker 0 → even 5th chars
                    tasks.append(
                        asyncio.create_task(
                            fourth_worker(
                                prefix,
                                fourth,
                                client,
                                f"{prefix}-{fourth}-W0",
                                start_index=0,
                                step=2
                            )
                        )
                    )

                    # Worker 1 → odd 5th chars
                    tasks.append(
                        asyncio.create_task(
                            fourth_worker(
                                prefix,
                                fourth,
                                client,
                                f"{prefix}-{fourth}-W1",
                                start_index=1,
                                step=2
                            )
                        )
                    )

                # 🧠 WAIT FOR ALL WORKERS TO FINISH
                results = await asyncio.gather(*tasks, return_exceptions=True)

                # 🔴 CHECK IF ANY WORKER REQUESTED TLS RESET
                if "NEED_CLIENT_RESET" in results:
                    print(f"[{prefix}] 🔄 403 DETECTED — closing client & sleeping 30s")

                    # client is automatically CLOSED here by context manager
                    await asyncio.sleep(30)

                    # 🔁 RESTART PREFIX WITH NEW TLS
                    continue

                # ✅ NORMAL COMPLETION (NO 403)
                break

        print(f"🏁 PREFIX {prefix} COMPLETED\n")

def log_code(label, code, worker_id, teams, events, score, time,odds,total_odds,last_change,db_name="OUTPUT.db"):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO codes (
            worker_id,
            label,
            code,
            teams,
            events,
            score,
            time,
            odds,
            total_odds,
            last_change
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            str(worker_id),
            str(label),
            str(code),
            str(teams),
            str(events),
            str(score),
            str(time),
            str(odds),
            str(total_odds),
            str(last_change),
        )
    )

    conn.commit()
    conn.close()
    print(f"[Worker {worker_id}] => {label}: {code} ({teams})")

def send_db_via_gmail(sender_email,app_password,recipient_email,db_path="OUTPUT.db"):
    db_file = Path(db_path)

    if not db_file.exists():
        print("📭 OUTPUT.db not found. No email sent.")
        return

    msg = EmailMessage()
    msg["Subject"] = "SportyBet Script Output DB"
    msg["From"] = sender_email
    msg["To"] = recipient_email
    msg.set_content("Attached is the OUTPUT.db generated by the script.")

    with open(db_file, "rb") as f:
        msg.add_attachment(
            f.read(),
            maintype="application",
            subtype="octet-stream",
            filename=db_file.name
        )

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
        smtp.login(sender_email, app_password)
        smtp.send_message(msg)

    print("📧 OUTPUT.db sent successfully via Gmail.")

async def runtime_watchdog():
    while not STOP_EVENT.is_set():
        if datetime.now() >= END_TIME:
            print("⏰ MAX EXECUTION TIME REACHED — STOPPING SCRIPT")
            STOP_EVENT.set()
            return
        await asyncio.sleep(1)

async def main_async():
    print("STARTING PREFIX ENGINE")

    watchdog_task = asyncio.create_task(runtime_watchdog())

    prefix_tasks = [
        asyncio.create_task(process_prefix(prefix))
        for prefix in PREFIXES
    ]

    done, pending = await asyncio.wait(
        prefix_tasks + [watchdog_task],
        return_when=asyncio.FIRST_COMPLETED
    )

    if STOP_EVENT.is_set():
        print("🛑 Cancelling remaining tasks...")
        for task in pending:
            task.cancel()

        await asyncio.gather(*pending, return_exceptions=True)

def main():
    try:
        asyncio.run(main_async())
    finally:
        # 🔽 CHANGE THESE VALUES
        gmail_sender = "1btcryptopayment@gmail.com"
        gmail_app_password = "zjti bewf hoib dteb"
        gmail_receiver = "tidianeyonkeu515@gmail.com"

        send_db_via_gmail(
            gmail_sender,
            gmail_app_password,
            gmail_receiver,
            "OUTPUT.db"
        )

if __name__ == "__main__":
    main()