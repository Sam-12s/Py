from httpx import ConnectTimeout, ReadTimeout, RequestError
import httpx
import asyncio
import sqlite3
from datetime import datetime, timedelta
import smtplib
from email.message import EmailMessage
from pathlib import Path
import random



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


PREFIXES = ['X1']

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
    'A', 'B', 'C', 'D', 'E', 'F', 'G', "H", 'J', 'K', 'L',
    'M', 'N', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W',
    'X', 'Y', 'Z'
]

MAX_PARALLEL_PREFIXES = int(len(PREFIXES))  # start with 2–4

PREFIX_SEMAPHORE = asyncio.Semaphore(MAX_PARALLEL_PREFIXES)

FAILED_CODES_FILE = "failed_403_codes.log"


def save_failed_code(worker_id, code, reason):
    line = f"{datetime.now().isoformat()} | {worker_id} | {code} | {reason}\n"
    with open(FAILED_CODES_FILE, "a", buffering=1) as f:
        f.write(line)


async def fetch_code(local_code, client, session_id):
    payload = {
        "Guid": local_code,
        "Lng": "en",
        "partner": "159"
    }

    url = "https://1xbet.ng/service-api/LiveBet/Open/GetCoupon"

    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-CA,en;q=0.9",
        "Connection": "keep-alive",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Referer": "https://1xbet.ng/en",
        "Origin": "https://1xbet.ng",
    }
    try:
        resp = await client.post(url, headers=headers, json=payload)
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
        if not response.get("Success"):
            return "INVALID"
        elif response.get("Success"):
            pg = response.get("Value")
            number_of_event = len(pg.get("Events", []))
            all_events = pg.get("Events", [])
            if number_of_event > 15:
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
                    today_date = datetime.now().date()
                    for _ in range(number_of_event):
                        valid = all_events[i].get("Finish")
                        timestamp = int(all_events[i].get("Start"))
                        odd = (all_events[i].get("Coef"))
                        sport = str(all_events[i].get("SportNameEng"))
                        event = str(all_events[i].get("GroupName"))
                        score = all_events[i].get("MarketName")
                        team = f"{all_events[i].get('Opp1')} vs {all_events[i].get('Opp2')}"
                        datetimestamp.append(timestamp)
                        events_status.append(valid)
                        odds.append(odd)
                        sports.append(sport)
                        lst_events.append(event)
                        lst_scores.append(score)
                        lst_teams.append(team)
                        i +=1
                    for timestamps in datetimestamp:
                        start = datetime.fromtimestamp(timestamps).date()
                        start2 = datetime.fromtimestamp(timestamps).time()
                        match_date.append(start)
                        lst_match_time.append(start2)
                    result = odds[0]
                    for i in range(1, len(odds)):
                        result *= float(odds[i])

                    match_times = "|".join(f"{tmes}"for tmes in lst_match_time)
                    outcomes = "|".join(f"{sc}" for sc in lst_scores)
                    events = "|".join(f"{ev}" for ev in lst_events)
                    var_odd = "|".join(f"{od}"for od in odds)
                    total_odd = str(result)
                    total_result = float(result)
                except KeyError as ff:
                    print(ff, '1')
                    return False

                if all(status == False for status in events_status) and all(match_start == today_date for match_start in match_date) and 80.00 < total_result < 500 and all(sportnames == "Football" for sportnames in sports):
                    teams = "|".join(f"{tm}"for tm in lst_teams)
                    init_db()
                    log_code(number_of_event , local_code, session_id, teams, events, outcomes, match_times, var_odd, total_odd, "NA")

                    print("BAGGED")

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


async def fourth_worker(prefix, fourth_char, client, worker_id):
    print(f"[{prefix}] 🚀 Worker {fourth_char} started")



    for a in SUFFIX_CHARS:
        if STOP_EVENT.is_set():
            break

        for b in SUFFIX_CHARS:
            if STOP_EVENT.is_set():
                break

            code = f"{prefix}{fourth_char}{a}{b}"

            while True:
                result = await fetch_code(code, client, worker_id)

                # 🔴 CASE 1: HTTP 403 → SAVE + RESET TLS
                if result == "ERROR_403":
                    save_failed_code(worker_id, code, "403")
                    print(f"[{worker_id}] 🔄 403 on {code}, requesting TLS reset")
                    continue

                # 🟡 CASE 2: Retryable error (timeout, parse, etc.)
                elif result == "ERROR_RETRY" or result == "ERROR_TIMEOUT":
                    continue  # retry SAME code
                else:
                # 🟢 CASE 3: Normal response (VALID / INVALID / others)
                    break


            # If counting is disabled → INVALIDs are ignored forever

    print(f"[{prefix}] ✅ Worker {fourth_char} finished normally")

async def process_prefix(prefix):
    async with PREFIX_SEMAPHORE:
        print(f"\n🔐 STARTING PREFIX {prefix}")

        while not STOP_EVENT.is_set():

            # 🔑 NEW CLIENT = NEW TLS HANDSHAKE
            async with httpx.AsyncClient(
                    timeout=httpx.Timeout(50.0, connect=50.0),
                    limits=httpx.Limits(
                        max_connections=34,
                        max_keepalive_connections=34
                    ),
                    headers={
                        "User-Agent": random.choice(USER_AGENTS),
                        "Accept": "application/json, text/plain, */*",
                        "Accept-Language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
                        "Referer": "https://1xbet.ng/en",
                        "Origin": "https://1xbet.ng",
                        "Connection": "keep-alive",
                    }
            ) as client:

                # 🚀 START ALL WORKERS FOR THIS PREFIX
                tasks = [
                    asyncio.create_task(
                        fourth_worker(prefix, fourth, client, f"{prefix}-{fourth}")
                    )
                    for fourth in SUFFIX_CHARS
                ]

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
        pass


if __name__ == "__main__":
    main()from httpx import ConnectTimeout, ReadTimeout, RequestError
import httpx
import asyncio
import sqlite3
from datetime import datetime, timedelta
import smtplib
from email.message import EmailMessage
from pathlib import Path
import random



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


PREFIXES = ['X1']

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
    'A', 'B', 'C', 'D', 'E', 'F', 'G', "H", 'J', 'K', 'L',
    'M', 'N', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W',
    'X', 'Y', 'Z'
]

MAX_PARALLEL_PREFIXES = int(len(PREFIXES))  # start with 2–4

PREFIX_SEMAPHORE = asyncio.Semaphore(MAX_PARALLEL_PREFIXES)

FAILED_CODES_FILE = "failed_403_codes.log"


def save_failed_code(worker_id, code, reason):
    line = f"{datetime.now().isoformat()} | {worker_id} | {code} | {reason}\n"
    with open(FAILED_CODES_FILE, "a", buffering=1) as f:
        f.write(line)


async def fetch_code(local_code, client, session_id):
    payload = {
        "Guid": local_code,
        "Lng": "en",
        "partner": "159"
    }

    url = "https://1xbet.ng/service-api/LiveBet/Open/GetCoupon"

    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-CA,en;q=0.9",
        "Connection": "keep-alive",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Referer": "https://1xbet.ng/en",
        "Origin": "https://1xbet.ng",
    }
    try:
        resp = await client.post(url, headers=headers, json=payload)
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
        if not response.get("Success"):
            return "INVALID"
        elif response.get("Success"):
            pg = response.get("Value")
            number_of_event = len(pg.get("Events", []))
            all_events = pg.get("Events", [])
            if number_of_event > 15:
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
                    today_date = datetime.now().date()
                    for _ in range(number_of_event):
                        valid = all_events[i].get("Finish")
                        timestamp = int(all_events[i].get("Start"))
                        odd = (all_events[i].get("Coef"))
                        sport = str(all_events[i].get("SportNameEng"))
                        event = str(all_events[i].get("GroupName"))
                        score = all_events[i].get("MarketName")
                        team = f"{all_events[i].get('Opp1')} vs {all_events[i].get('Opp2')}"
                        datetimestamp.append(timestamp)
                        events_status.append(valid)
                        odds.append(odd)
                        sports.append(sport)
                        lst_events.append(event)
                        lst_scores.append(score)
                        lst_teams.append(team)
                        i +=1
                    for timestamps in datetimestamp:
                        start = datetime.fromtimestamp(timestamps).date()
                        start2 = datetime.fromtimestamp(timestamps).time()
                        match_date.append(start)
                        lst_match_time.append(start2)
                    result = odds[0]
                    for i in range(1, len(odds)):
                        result *= float(odds[i])

                    match_times = "|".join(f"{tmes}"for tmes in lst_match_time)
                    outcomes = "|".join(f"{sc}" for sc in lst_scores)
                    events = "|".join(f"{ev}" for ev in lst_events)
                    var_odd = "|".join(f"{od}"for od in odds)
                    total_odd = str(result)
                    total_result = float(result)
                except KeyError as ff:
                    print(ff, '1')
                    return False

                if all(status == False for status in events_status) and all(match_start == today_date for match_start in match_date) and 80.00 < total_result < 500 and all(sportnames == "Football" for sportnames in sports):
                    teams = "|".join(f"{tm}"for tm in lst_teams)
                    init_db()
                    log_code(number_of_event , local_code, session_id, teams, events, outcomes, match_times, var_odd, total_odd, "NA")

                    print("BAGGED")

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


async def fourth_worker(prefix, fourth_char, client, worker_id):
    print(f"[{prefix}] 🚀 Worker {fourth_char} started")



    for a in SUFFIX_CHARS:
        if STOP_EVENT.is_set():
            break

        for b in SUFFIX_CHARS:
            if STOP_EVENT.is_set():
                break

            code = f"{prefix}{fourth_char}{a}{b}"

            while True:
                result = await fetch_code(code, client, worker_id)

                # 🔴 CASE 1: HTTP 403 → SAVE + RESET TLS
                if result == "ERROR_403":
                    save_failed_code(worker_id, code, "403")
                    print(f"[{worker_id}] 🔄 403 on {code}, requesting TLS reset")
                    continue

                # 🟡 CASE 2: Retryable error (timeout, parse, etc.)
                elif result == "ERROR_RETRY" or result == "ERROR_TIMEOUT":
                    continue  # retry SAME code
                else:
                # 🟢 CASE 3: Normal response (VALID / INVALID / others)
                    break


            # If counting is disabled → INVALIDs are ignored forever

    print(f"[{prefix}] ✅ Worker {fourth_char} finished normally")

async def process_prefix(prefix):
    async with PREFIX_SEMAPHORE:
        print(f"\n🔐 STARTING PREFIX {prefix}")

        while not STOP_EVENT.is_set():

            # 🔑 NEW CLIENT = NEW TLS HANDSHAKE
            async with httpx.AsyncClient(
                    timeout=httpx.Timeout(50.0, connect=50.0),
                    limits=httpx.Limits(
                        max_connections=34,
                        max_keepalive_connections=34
                    ),
                    headers={
                        "User-Agent": random.choice(USER_AGENTS),
                        "Accept": "application/json, text/plain, */*",
                        "Accept-Language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
                        "Referer": "https://1xbet.ng/en",
                        "Origin": "https://1xbet.ng",
                        "Connection": "keep-alive",
                    }
            ) as client:

                # 🚀 START ALL WORKERS FOR THIS PREFIX
                tasks = [
                    asyncio.create_task(
                        fourth_worker(prefix, fourth, client, f"{prefix}-{fourth}")
                    )
                    for fourth in SUFFIX_CHARS
                ]

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
        pass


if __name__ == "__main__":
    main()
