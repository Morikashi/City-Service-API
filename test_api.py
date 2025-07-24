# scripts/test_api.py
import asyncio, httpx, rich
from rich.console import Console

API = "http://localhost:8000"
CITIES = [
    {"name": "Tokyo",          "codes": ("KZ","LU","TK","VN","HM","GH","BY","IM","TV","AO","TG","GE","FR","ZW","TH","WF","OM")},
    {"name": "London",         "codes": ("BA","SI","FM","HU","FR","CC","CN","GB","NA","SH","LT","SD","TC","BT","UY","TT","TZ")},
    {"name": "New York City",  "codes": ("GA","SK","CU","AL","PH","NO","BF","IL","NC","SC","FI","SV")},
    {"name": "Paris",          "codes": ("AU","BD","CK","JE","FK","MP","MA","GF","VG","SI","GQ")},
    {"name": "Berlin",         "codes": ("EH","AX","TG","NU","EG")},
]

console = Console(highlight=False)
ok = err = 0

def passed(msg):
    global ok; ok += 1; console.print(f"[green]✓ {msg}")

def failed(msg):
    global err; err += 1; console.print(f"[red]✗ {msg}")

async def main():
    await asyncio.sleep(2)          # give API a moment

    # ---------- health ----------
    r = httpx.get(f"{API}/health")
    if r.status_code==200 and r.json().get("status")=="healthy": passed("health")
    else: failed(f"health {r.status_code}")

    # ---------- create / update ----------
    for item in CITIES:
        for code in item["codes"]:
            r = httpx.post(f"{API}/api/v1/cities/", json={"name": item["name"],"country_code":code})
            if r.status_code in (200,201): passed(f"POST {item['name']}:{code}")
            else: failed(f"POST {item['name']}:{code} -> {r.status_code}")

    # ---------- get single code (first) ----------
    for item in CITIES:
        city = item["name"]
        r = httpx.get(f"{API}/api/v1/cities/{city}/country-code")
        if r.status_code==200 and r.json()["country_code"] in item["codes"]:
            passed(f"GET first code {city}")
        else:
            failed(f"GET first code {city} -> {r.status_code}")

    # ---------- get all codes ----------
    for item in CITIES:
        city = item["name"]
        r = httpx.get(f"{API}/api/v1/cities/{city}/country-codes")
        if r.status_code==200 and set(r.json()["country_codes"])>=set(item["codes"]):
            passed(f"GET all codes {city}")
        else:
            failed(f"GET all codes {city} -> {r.status_code}")

    # ---------- list ----------
    r = httpx.get(f"{API}/api/v1/cities/")
    if r.status_code==200 and r.json().get("total")>=len(CITIES): passed("list")
    else: failed(f"list {r.status_code}")

    # ---------- metrics ----------
    r = httpx.get(f"{API}/api/v1/cities/metrics")
    if r.status_code==200: passed("metrics")
    else: failed(f"metrics {r.status_code}")

    # ---------- delete one pair ----------
    city = CITIES[0]["name"]; code = CITIES[0]["codes"][0]
    r = httpx.delete(f"{API}/api/v1/cities/{city}?country_code={code}")
    if r.status_code==204: passed(f"delete {city}:{code}")
    else: failed(f"delete {city}:{code} -> {r.status_code}")

    # ---------- summary ----------
    total=ok+err
    console.print(f"\n[bold]RESULT:[/] {ok}/{total} passed, {err} failed")
    exit(1 if err else 0)

if __name__=="__main__":
    asyncio.run(main())
