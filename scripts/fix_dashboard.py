import requests, json

BASE="http://localhost:8088"
USER="admin"
PASS="Admin@123"

s=requests.Session()

# LOGIN
r=s.post(
    f"{BASE}/api/v1/security/login",
    json={
        "username":USER,
        "password":PASS,
        "provider":"db",
        "refresh":True
    }
)

token=r.json()["access_token"]

s.headers.update({
    "Authorization":f"Bearer {token}"
})

csrf=s.get(f"{BASE}/api/v1/security/csrf_token/").json()["result"]

s.headers.update({
    "X-CSRFToken":csrf,
    "Referer":BASE,
    "Content-Type":"application/json"
})

# GET CHARTS
charts=s.get(f"{BASE}/api/v1/chart/").json()["result"]

print("\n==== CHARTS ====\n")

for c in charts:
    print(f"{c['id']} - {c['slice_name']}")

print("\n================\n")

# SIMPLE DASHBOARD CREATE
r=s.post(
    f"{BASE}/api/v1/dashboard/",
    json={
        "dashboard_title":"🎯 Customer360 Analytics"
    }
)

print(r.status_code)
print(r.text)

if r.status_code not in [200,201]:
    print("FAILED")
    exit()

dash_id=r.json()["id"]

print(f"\nDASHBOARD ID = {dash_id}")

# ADD CHARTS TO DASHBOARD
for c in charts:

    cid=c["id"]

    payload={
        "slice_id":cid
    }

    rr=s.post(
        f"{BASE}/api/v1/dashboard/{dash_id}/charts",
        json=payload
    )

    print(f"ADD CHART {cid} => {rr.status_code}")

print("\nDONE")
print(f"\nhttp://3.88.43.105:8088/superset/dashboard/{dash_id}/")
