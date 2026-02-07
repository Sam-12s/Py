import sys
import httpx
import asyncio
import sqlite3
from datetime import datetime, timedelta
import smtplib
from email.message import EmailMessage
from pathlib import Path
import random
import time
sys.stdout.reconfigure(line_buffering=True)

MAX_RUNTIME_MINUTES = 355  # ⏱️ CHANGE THIS
START_TIME = datetime.now()
END_TIME = START_TIME + timedelta(minutes=MAX_RUNTIME_MINUTES)

STOP_EVENT = asyncio.Event()



def init_db(db_name="OUTPUT.db"):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    cursor.execute("PRAGMA journal_mode=WAL;")
    cursor.execute("PRAGMA synchronous=NORMAL;")
    cursor.execute("PRAGMA cache_size=-50000;")  # ~50MB memory
    cursor.execute("PRAGMA temp_store=MEMORY;")  # faster
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS codes (
            ID INTEGER PRIMARY KEY AUTOINCREMENT,
            Worker_Id TEXT,
            Label TEXT,
            Code TEXT UNIQUE,
            Teams TEXT,   -- store teams as string, e.g. "Man Utd vs Chelsea"
            Events TEXT,
            Score TEXT,
            Times TEXT,
            Odds TEXT,
            Total_odds TEXT,
            Last_change TEXT,
            Timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    conn.close()


PREFIXES = ['G8C', 'N8R', 'W7V', 'ZBQ', 'YXN', 'TYR', 'GV5', 'RQV', 'S3Q', 'LAN', 'Q8P', 'L08', 'GKZ', 'X9B', 'YA7', 'WTM', 'LQR', 'WDZ', 'RNE', 'K3V', 'XPE']

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
DB_QUEUE = asyncio.Queue()

SUFFIX_CHARS = [
    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
    'A', 'B', 'C', 'D', 'E', 'F', 'G', "H", 'J', 'K', 'L',
    'M', 'N', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W',
    'X', 'Y', 'Z'
]

MAX_PARALLEL_PREFIXES = int(len(PREFIXES))  # start with 2–4

PREFIX_SEMAPHORE = asyncio.Semaphore(MAX_PARALLEL_PREFIXES)

FAILED_CODES_FILE = "failed_403_codes.log"


def current_timestamp_ms():
    """
    Returns the current timestamp in milliseconds.
    """
    return int(time.time() * 1000)


def save_failed_code(worker_id, code, reason):
    line = f"{datetime.now().isoformat()} | {worker_id} | {code} | {reason}\n"
    with open(FAILED_CODES_FILE, "a", buffering=1) as f:
        f.write(line)


async def fetch_code(local_code, client, session_id):
    url = f"https://www.sportybet.com/api/ng/orders/share/{local_code}?_t={current_timestamp_ms()}"

    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Referer": "https://www.sportybet.com/en",
        "Origin": "https://www.sportybet.com",
        "X - Forwarded - For": "186.136.25.000",
        
    }

    resp = await client.get(url, headers=headers)

    content_type = resp.headers.get("Content-Type", "")
    text = resp.text.strip()

    if resp.status_code == 403:
        print(f"{resp.status_code}")
        return "ERROR_403"

    if resp.status_code != 200:
        print(f"{resp.status_code}")
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

            if number_of_event == 0:
                print("NO-OUTCOME", "-----", str(session_id), flush=True)
                return "VALID"

            else:

                try:
                    i = 0
                    events_status = []
                    datetimestamp = []
                    match_date = []
                    odds = []
                    sports = []
                    lst_events = []
                    lst_match_time = []
                    lst_scores = []
                    lst_teams = []
                    lst_change = []
                    lst_match_oddchanges = []
                    lst_type = []
                    today_date = datetime.now().date()
                    for _ in range(number_of_event):
                        valid = pg[i]['matchStatus']
                        timestamp = int(pg[i]['estimateStartTime'])
                        odd = (pg[i]["markets"][0]["outcomes"][0]["odds"])
                        sport = str(pg[i]["sport"]["name"])
                        types = str(pg[i]["sport"]["category"]["name"])
                        event = str(pg[i]["markets"][0]["desc"])
                        score = pg[i]["markets"][0]["outcomes"][0]["desc"]
                        team = f"{pg[i]['homeTeamName']} vs {pg[i]['awayTeamName']}"
                        change = int(pg[i]["markets"][0]["lastOddsChangeTime"])
                        lst_change.append(change)
                        datetimestamp.append(timestamp)
                        events_status.append(valid)
                        odds.append(odd)
                        sports.append(sport)
                        lst_events.append(event)
                        lst_scores.append(score)
                        lst_teams.append(team)
                        lst_type.append(types)
                        i += 1
                    for timestamps in datetimestamp:
                        s = timestamps/1000
                        start = datetime.fromtimestamp(s).date()
                        start2 = datetime.fromtimestamp(s).time()
                        match_date.append(start)
                        lst_match_time.append(start2)
                    for changes in lst_change:
                        r = datetime.fromtimestamp(changes / 1000).strftime("%H:%M:%S")
                        lst_match_oddchanges.append(r)
                    result = 1.0
                    for odd in odds:
                        result *= float(odd)
                    match_times = "|".join(f"{tmes}" for tmes in lst_match_time)
                    outcomes = "|".join(f"{sc}" for sc in lst_scores)
                    events = "|".join(f"{ev}" for ev in lst_events)
                    var_odd = "|".join(f"{od}" for od in odds)
                    total_odd = str(result)
                    total_result = float(result)
                    change_times = "|".join(f"{chng}" for chng in lst_match_oddchanges)
                    teams = "|".join(f"{tm}" for tm in lst_teams)
                except Exception as ff:
                    print(ff, f'1----{local_code}')
                    return False

                if all(status in ["Not start","H1"] for status in events_status) and all(match_start == today_date for match_start in match_date) and all(stat != "Simulated Reality League" for stat in lst_type) and all(sportn == "Football" for sportn in sports):
                    init_db()
                    if  number_of_event == 4 and 100.0 < total_result < 200.0:
                        log_code("QUADRIPLE", local_code, session_id, teams, events, outcomes, match_times, var_odd, total_odd,change_times)
                        print("QUADRIPLE ODD")
                        return "VALID"
                    elif number_of_event == 3 and 18.00 < total_result < 200.0:
                        log_code("TRIPLE", local_code, session_id, teams, events, outcomes, match_times, var_odd, total_odd,change_times)
                        print("TRIPLE ODD")
                        return "VALID"
                    elif number_of_event == 2 and 25.00 < total_result < 180.0 and all(outcom == 'Correct Score' for outcom in lst_events):
                        log_code("DOUBLE", local_code, session_id, teams, events, outcomes, match_times, var_odd, total_odd,change_times)
                        print("DOUBLE ODD")
                        return "VALID"
                    elif number_of_event == 1 and 5.00 < total_result < 30.0 and all(outcom == 'Correct Score' for outcom in lst_events):
                        log_code("SINGLE", local_code, session_id, teams, events, outcomes, match_times, var_odd, total_odd,change_times)
                        print("SINGLE ODD")
                        return "VALID"
                    else:
                        print(f"UNSATISFIED--------{local_code}")
                else:
                    print(f"FORGET--------{local_code}")
                    return False
        else:
            print(f"NAHH--------{local_code}")

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

            if number_of_event != 0:
                try:
                    i = 0
                    events_status = []
                    datetimestamp = []
                    match_date = []
                    odds = []
                    sports = []
                    lst_events = []
                    lst_match_time = []
                    lst_scores = []
                    lst_teams = []
                    lst_change = []
                    lst_match_oddchanges = []
                    lst_type = []
                    today_date = datetime.now().date()
                    for _ in range(number_of_event):
                        valid = find(outcomes[i], "matchStatus")
                        timestamp = int(find(outcomes[i], "estimateStartTime"))
                        odd = find(outcomes[i], "markets/markets/outcomes/outcomes/odds")
                        sport = (find(outcomes[i], "sport/name"))
                        types = (find(outcomes[i], "sport/category/name"))
                        event = find(outcomes[i], "markets/markets/desc")
                        score = find(outcomes[i], "markets/markets/outcomes/outcomes/desc")
                        team = team = f"{find(outcomes[i], 'homeTeamName')} vs {find(outcomes[i], 'awayTeamName')}"
                        change = int(find(outcomes[i], "markets/markets/lastOddsChangeTime"))
                        lst_change.append(change)
                        datetimestamp.append(timestamp)
                        events_status.append(valid)
                        odds.append(odd)
                        sports.append(sport)
                        lst_events.append(event)
                        lst_scores.append(score)
                        lst_teams.append(team)
                        lst_type.append(types)
                        i += 1
                    for timestamps in datetimestamp:
                        s = timestamps / 1000
                        start = datetime.fromtimestamp(s).date()
                        start2 = datetime.fromtimestamp(s).time()
                        match_date.append(start)
                        lst_match_time.append(start2)
                    result = 1.0
                    for odd in odds:
                        result *= float(odd)
                    for changes in lst_change:
                        r = datetime.fromtimestamp(changes / 1000).strftime("%H:%M:%S")
                        lst_match_oddchanges.append(r)
                    match_times = "|".join(f"{tmes}" for tmes in lst_match_time)
                    outcomes = "|".join(f"{sc}" for sc in lst_scores)
                    events = "|".join(f"{ev}" for ev in lst_events)
                    var_odd = "|".join(f"{od}" for od in odds)
                    total_odd = str(result)
                    total_result = float(result)
                    change_times = "|".join(f"{chng}" for chng in lst_match_oddchanges)
                    teams = "|".join(f"{tm}" for tm in lst_teams)


                except Exception as ff:
                    print(ff, '1(XML)')
                    return False

                if all(status in ["Not start", "H1"] for status in events_status) and all(
                        match_start == today_date for match_start in match_date) and all(
                        stat != "Simulated Reality League" for stat in lst_type) and all(
                        sportn == "Football" for sportn in sports):
                    init_db()
                    if number_of_event == 4 and 100 < total_result < 200:
                        log_code("XML-QUADRIPLE", local_code, session_id, teams, events, outcomes, match_times, var_odd,total_odd, change_times)
                        print("XML-QUADRIPLE ODD")
                        return "VALID"
                    elif number_of_event == 3 and 18.00 < total_result < 200:
                        log_code("XML-TRIPLE", local_code, session_id, teams, events, outcomes, match_times, var_odd,total_odd, change_times)
                        print("XML-TRIPLE ODD")
                        return "VALID"
                    elif number_of_event == 2 and 25.00 < total_result < 180 and all(outcom == "Correct Score" for outcom in lst_events):
                        log_code("XML-DOUBLE", local_code, session_id, teams, events, outcomes, match_times, var_odd,total_odd, change_times)
                        print("XML-DOUBLE ODD")
                        return "VALID"
                    elif number_of_event == 1 and 5.00 < total_result < 30 and all(outcom == "Correct Score" for outcom in lst_events):
                        log_code("XML-SINGLE", local_code, session_id, teams, events, outcomes, match_times, var_odd,total_odd, change_times)
                        print("XML-SINGLE ODD")
                        return "VALID"
                    else:
                        print("UNSATISFIED")
                else:
                    print(f"FORGET--------{local_code}")
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
                    continue
                break

    print(f"[{prefix}] ✅ Worker {worker_id} finished normally")



async def process_prefix(prefix):
    async with PREFIX_SEMAPHORE:
        print(f"\n🔐 STARTING PREFIX {prefix}")

        while not STOP_EVENT.is_set():

            # 🔑 NEW CLIENT = NEW TLS HANDSHAKE AND NO KEEP-ALIVE
            async with httpx.AsyncClient(
                    http2=True,
                    timeout=httpx.Timeout(200.0, connect=50.0),
                    headers={
                        "User-Agent": random.choice(USER_AGENTS),
                        "Accept": "application/json, text/plain, */*",
                        "Accept-Language": "en-US,en;q=0.9",
                        "Referer": "https://www.sportybet.com/en",
                        "Origin": "https://www.sportybet.com",
                    },
                    verify=False
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

async def db_writer(db_name="OUTPUT.db"):
    conn = sqlite3.connect(db_name, timeout=30)
    cursor = conn.cursor()

    cursor.execute("PRAGMA journal_mode=WAL;")
    cursor.execute("PRAGMA synchronous=NORMAL;")

    print("🗄️ DB writer started")

    while True:
        data = await DB_QUEUE.get()

        if data is None:  # shutdown signal
            break

        try:
            cursor.execute("""
                INSERT OR IGNORE INTO codes (
                    worker_id,
                    label,
                    code,
                    teams,
                    events,
                    score,
                    times,
                    odds,
                    total_odds,
                    last_change
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, data)

            conn.commit()
            print(f"💾 DB SAVED: {data[2]}")

        except sqlite3.Error as e:
            print(f"[DB ERROR] {e}")

        DB_QUEUE.task_done()

    conn.close()
    print("🗄️ DB writer stopped")

def log_code(label, code, worker_id, teams, events, score, times, odds, total_odds, last_change):
    DB_QUEUE.put_nowait((
        worker_id,
        label,
        code,
        teams,
        events,
        score,
        times,
        odds,
        total_odds,
        last_change
    ))


def send_db_via_gmail(sender_email, app_password, recipient_email, db_path="OUTPUT.db"):
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
    db_task = asyncio.create_task(db_writer())
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

        await DB_QUEUE.put(None)
        await db_task

        for task in pending:
            task.cancel()

        await asyncio.gather(*pending, return_exceptions=True)


def main():
    try:
        asyncio.run(main_async())
    finally:
        '''
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
        '''

if __name__ == "__main__":
    main()
