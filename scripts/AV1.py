import asyncio
import aiohttp
import time



def current_timestamp_ms():
    """
    Returns the current timestamp in milliseconds.
    """
    return int(time.time() * 1000)

URL = f"https://www.sportybet.com/api/ng/orders/share/XYZ23S?_t={current_timestamp_ms()}" # change this
DURATION = 10                   # seconds to run test
CONCURRENCY = 200               # number of parallel requests

async def worker(session, counter):
    try:
        async with session.get(URL, timeout=10) as resp:
            await resp.read()
            counter["success"] += 1
    except:
        counter["fail"] += 1

async def run_test():
    counter = {"success": 0, "fail": 0}
    timeout = aiohttp.ClientTimeout(total=15)
    connector = aiohttp.TCPConnector(limit=CONCURRENCY, ssl=False)

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
    print(f"Success: {counter['success']}")
    print(f"Failed:  {counter['fail']}")
    print(f"RPS:     {rps:.2f}")

asyncio.run(run_test())
