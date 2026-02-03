import asyncio
import aiohttp
import json

TEST_CODE = "M2BH11"   # 👈 change this to any code you want to test

URL = f"https://www.sportybet.com/api/ng/orders/share/{TEST_CODE}"

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.sportybet.com/",
    "Origin": "https://www.sportybet.com",
}

async def verify_code():
    async with aiohttp.ClientSession(headers=HEADERS) as session:
        async with session.get(URL) as resp:
            print("HTTP STATUS:", resp.status)

            text = await resp.text()
            try:
                data = json.loads(text)
            except Exception:
                print("❌ Response is not JSON")
                print(text[:500])
                return

            if "data" not in data:
                print("❌ No 'data' field")
                print(data)
                return

            outcomes = data["data"].get("outcomes", [])
            print(f"TOTAL OUTCOMES: {len(outcomes)}\n")

            for idx, item in enumerate(outcomes):
                print(f"Outcome #{idx + 1}")
                print("eventId:", item.get("eventId"))
                print("matchStatus present:", "matchStatus" in item)
                print("markets present:", "markets" in item)
                print("-" * 40)

asyncio.run(verify_code())
