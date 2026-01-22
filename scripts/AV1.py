asyncio.run(run_test())
import asyncio
import aiohttp
import time

def current_timestamp_ms():
    """
    Returns the current timestamp in milliseconds.
    """
    return int(time.time() * 1000)

URL = f"https://www.sportybet.com/api/ng/orders/share/XYZ23S?_t={current_timestamp_ms()}" # change this
DURATION = 10                   # test runtime
CONCURRENCY = 200
MAX_RESPONSE_TIME = 15.0        # seconds

async def worker(session, counter):
    start = time.perf_counter()
    try:
        async with session.get(URL) as resp:
            await resp.read()
            elapsed = time.perf_counter() - start

            if elapsed <= MAX_RESPONSE_TIME:
                counter["success"] += 1
            else:
                counter["slow"] += 1

    except:
        counter["fail"] += 1

async def run_test():
    counter = {
        "success": 0,
        "fail": 0,
        "slow": 0,
    }

    timeout = aiohttp.ClientTimeout(
        total=MAX_RESPONSE_TIME + 1
    )

    connector = aiohttp.TCPConnector(
        limit=CONCURRENCY,
        ssl=False
    )

    async with aiohttp.ClientSession(
        timeout=timeout,
        connector=connector
    ) as session:

        start = time.time()

        while time.time() - start < DURATION:
            tasks = [
                asyncio.create_task(worker(session, counter))
                for _ in range(CONCURRENCY)
            ]
            await asyncio.gather(*tasks)

    elapsed = time.time() - start
    rps = counter["success"] / elapsed

    print("====== RESULT ======")
    print(f"Duration: {elapsed:.2f}s")
    print(f"Success (<=15s): {counter['success']}")
    print(f"Slow   (>15s):  {counter['slow']}")
    print(f"Failed:         {counter['fail']}")
    print(f"RPS (valid):    {rps:.2f}")

asyncio.run(run_test())
