import sys
from httpx import ConnectTimeout, ReadTimeout, RequestError
import httpx
import asyncio
import sqlite3
from datetime import datetime, timedelta
import smtplib
from email.message import EmailMessage
from pathlib import Path
import random

# ===== RPS METRICS =====
REQUEST_COUNTER = 0
REQUEST_COUNTER_LOCK = asyncio.Lock()
START_TS = time.perf_counter()

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


PREFIXES = ['3J', '4J', 'VJ', 'L4', 'N4', '7J', 'TK', 'RH', 'U4', '14', '5J', 'RK', '44', 'FK', 'MK', '1K', 'JK', 'ZH', 'R4', 'VH', 'P4', 'ZJ', 'S4', '2J', '7K']

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
    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
    'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'J', 'K', 'L',
    'M', 'N', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W',
    'X', 'Y', 'Z'
]

MAX_PARALLEL_PREFIXES = int(len(PREFIXES))  # start with 2–4

PREFIX_SEMAPHORE = asyncio.Semaphore(MAX_PARALLEL_PREFIXES)

FAILED_CODES_FILE = "failed_403_codes.log"

async def rps_reporter(interval=10):
    last_count = 0
    last_time = time.perf_counter()

    while not STOP_EVENT.is_set():
        await asyncio.sleep(interval)

        async with REQUEST_COUNTER_LOCK:
            current_count = REQUEST_COUNTER

        now = time.perf_counter()

        delta_requests = current_count - last_count
        delta_time = now - last_time

        rps = delta_requests / delta_time if delta_time > 0 else 0

        print(
            f"[RPS] {rps:.2f} req/s | "
            f"Total requests: {current_count}"
        )

        last_count = current_count
        last_time = now


def save_failed_code(worker_id, code, reason):
    line = f"{datetime.now().isoformat()} | {worker_id} | {code} | {reason}\n"
    with open(FAILED_CODES_FILE, "a", buffering=1) as f:
        f.write(line)


async def fetch_code(local_code, client, session_id):
    payload = {
        "Guid": local_code,
        "Lng": "en",
        "partner": "1"
    }

    url = "https://ca.1xbet.com/service-api/LiveBet/Open/GetCoupon"

    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-CA,en;q=0.9",
        "Connection": "keep-alive",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Referer": "https://ca.1xbet.com/en",
        "Origin": "https://ca.1xbet.com",
    }


    try:
        resp = await client.post(url, headers=headers, json=payload)
        async with REQUEST_COUNTER_LOCK:
            global REQUEST_COUNTER
            REQUEST_COUNTER += 1

    except ConnectTimeout:
        print(f"[{session_id}] ⏱️ ConnectTimeout on {local_code}")
        return "ERROR_TIMEOUT"

    except ReadTimeout:
        print(f"[{session_id}] 📥 ReadTimeout on {local_code}")
        return "ERROR_RETRY"

    except RequestError as e:
        print(f"[{session_id}] 🌐 Network error: {e}")
        return "ERROR_RETRY"

    content_type = resp.headers.get("Content-Type", "")
    text = resp.text.strip()

    if resp.status_code == 403:
        return "ERROR_403"

    if resp.status_code != 200:
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

        if not response.get("Success"):
            print("The code is invalid", local_code, "-----B02", str(session_id), flush=True)
            return "INVALID"

        elif response.get("Success"):
            pg = response.get("Value")

            number_of_event = len(pg.get("Events", []))
            all_events = pg.get("Events", [])

            if number_of_event != 4:
                print("Retry", "-----", str(session_id), local_code)
                return "VALID"



            elif number_of_event == 4:

                try:

                    types = str(all_events[0].get("SportNameEng"))

                    types1 = str(all_events[1].get("SportNameEng"))

                    types2 = str(all_events[2].get("SportNameEng"))

                    types3 = str(all_events[3].get("SportNameEng"))

                    event = str(all_events[0].get("GroupName"))

                    event1 = str(all_events[1].get("GroupName"))

                    event2 = str(all_events[2].get("GroupName"))

                    event3 = str(all_events[3].get("GroupName"))

                    timestamp = int(all_events[0].get("Start"))

                    timestamp1 = int(all_events[1].get("Start"))

                    timestamp2 = int(all_events[2].get("Start"))

                    timestamp3 = int(all_events[3].get("Start"))

                    match_date = datetime.fromtimestamp(timestamp).date()

                    match_date1 = datetime.fromtimestamp(timestamp1).date()

                    match_date2 = datetime.fromtimestamp(timestamp2).date()

                    match_date3 = datetime.fromtimestamp(timestamp3).date()

                    match_time = datetime.fromtimestamp(timestamp).time()

                    match_time1 = datetime.fromtimestamp(timestamp1).time()

                    match_time2 = datetime.fromtimestamp(timestamp2).time()

                    match_time3 = datetime.fromtimestamp(timestamp3).time()

                    match_times = f"{match_time}||{match_time1}||{match_time2}||{match_time3}"

                    score = all_events[0].get("MarketName")

                    score1 = all_events[1].get("MarketName")

                    score2 = all_events[2].get("MarketName")

                    score3 = all_events[3].get("MarketName")

                    outcomes = f"{score}|{score1}|{score2}|{score3}"

                    today_date = datetime.now().date()

                    valid = all_events[0].get("Finish")

                    valid1 = all_events[1].get("Finish")

                    valid2 = all_events[2].get("Finish")

                    valid3 = all_events[3].get("Finish")

                    odd = str(all_events[0].get("Coef"))

                    odd1 = str(all_events[1].get("Coef"))

                    odd2 = str(all_events[2].get("Coef"))

                    odd3 = str(all_events[3].get("Coef"))

                    calculate_odds = (float(odd) * float(odd1) * float(odd2) * float(odd3))

                    odds = f"{odd}|{odd1}|{odd2}|{odd3}"

                    total_odd = str(calculate_odds)

                    events = f"{event}|{event1}|{event2}|{event3}"

                except KeyError as ff:

                    print(ff, '1')

                    return False

                if (valid, valid1, valid2,
                    valid3 == False) and match_date == today_date and match_date1 == today_date and match_date2 == today_date and match_date3 == today_date and calculate_odds > 50.00 and types == "Football" and types1 == "Football" and types2 == "Football" and types3 == "Football":

                    teams1 = f"{all_events[0].get('Opp1')} vs {all_events[0]}"

                    teams2 = f"{pg[1]['homeTeamName']} vs {pg[1]['awayTeamName']}"

                    teams3 = f"{pg[2]['homeTeamName']} vs {pg[2]['awayTeamName']}"

                    teams = f"{teams1}|{teams2}|{teams3}"

                    init_db()

                    log_code("4x", local_code, session_id, teams, events, outcomes, match_times, odds, total_odd, "NA")

                    print("FOUR ODD")

                    return "VALID"

                else:

                    print(f"FORGET--------{local_code}")

                    return "VALID"

        else:
            print("wow")
            return False
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
                    # 🔁 RESTART PREFIX WITH NEW TLS
                    continue

                # ✅ NORMAL COMPLETION (NO 403)
                break

        print(f"🏁 PREFIX {prefix} COMPLETED\n")

def log_code(label, code, worker_id, teams, events, score, time, odds, total_odds, last_change, db_name="OUTPUT.db"):
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

    watchdog_task = asyncio.create_task(runtime_watchdog())
    rps_task = asyncio.create_task(rps_reporter(interval=15))

    prefix_tasks = [
        asyncio.create_task(process_prefix(prefix))
        for prefix in PREFIXES
    ]

    done, pending = await asyncio.wait(
        prefix_tasks + [watchdog_task, rps_task],
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
