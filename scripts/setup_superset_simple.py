import requests, json, time, urllib.request

BASE   = "http://localhost:8088"
USER   = "admin"
PASSWD = "Admin@123"
TRINO  = "trino://admin@172.20.0.1:8080/hive"

def login():
    s = requests.Session()

    r = s.post(
        f"{BASE}/api/v1/security/login",
        json={
            "username": USER,
            "password": PASSWD,
            "provider": "db",
            "refresh": True
        }
    )

    token = r.json()["access_token"]

    csrf = s.get(
        f"{BASE}/api/v1/security/csrf_token/",
        headers={"Authorization": f"Bearer {token}"}
    ).json()["result"]

    headers = {
        "Authorization": f"Bearer {token}",
        "X-CSRFToken": csrf,
        "Content-Type": "application/json",
        "Referer": BASE
    }

    print("✅ Login + CSRF OK")
    return s, headers

def post(session, h, path, data):
    r = session.post(f"{BASE}{path}", headers=h, json=data)

    if r.status_code not in [200, 201]:
        print(f"\n❌ ERROR {path}")
        print(r.status_code)
        print(r.text)
        return None

    return r.json()

def mk(col, agg="COUNT"):
    return {
        "aggregate": agg,
        "column": {"column_name": col},
        "expressionType": "SIMPLE",
        "label": f"{agg}({col})"
    }

def mksql(sql, label):
    return {
        "expressionType":"SQL",
        "sqlExpression":sql,
        "label":label
    }

s, h = login()

# =========================
# DATABASE
# =========================

db_resp = requests.get(
    f"{BASE}/api/v1/database/",
    headers=h
).json()

db_id = None

for db in db_resp.get("result", []):
    if db["database_name"] == "Trino_C360":
        db_id = db["id"]

if not db_id:
    r = post(s, h, "/api/v1/database/", {
        "database_name": "Trino_C360",
        "sqlalchemy_uri": TRINO,
        "expose_in_sqllab": True,
    })

    db_id = r["id"]

print(f"✅ DB id={db_id}")

# =========================
# DATASET
# =========================

ds_resp = requests.get(
    f"{BASE}/api/v1/dataset/",
    headers=h
).json()

ds_id = None

for ds in ds_resp.get("result", []):
    if ds.get("table_name") == "customer_rfm":
        ds_id = ds["id"]

if not ds_id:
    r = post(s, h, "/api/v1/dataset/", {
        "database": db_id,
        "schema": "gold",
        "table_name": "customer_rfm",
    })

    ds_id = r["id"]

print(f"✅ Dataset id={ds_id}")

# =========================
# CREATE CHART
# =========================

def chart(name, viz, params):

    r = post(s, h, "/api/v1/chart/", {
        "slice_name": name,
        "viz_type": viz,
        "datasource_id": ds_id,
        "datasource_type": "table",
        "params": json.dumps(params),
    })

    if not r:
        return None

    cid = r.get("id")

    print(f"  ✅ [{cid}] {name}")

    return cid

print("\n📊 Tạo charts...\n")

ids = []

# KPI
ids.append(chart(
    "👥 Tổng Khách Hàng",
    "big_number_total",
    {
        "metric": mk("contract","COUNT_DISTINCT"),
        "subheader":"unique users"
    }
))

# PIE
ids.append(chart(
    "🎯 RFM Segment Distribution",
    "pie",
    {
        "groupby":["rfm_segment"],
        "metric":mk("contract","COUNT"),
        "donut":True
    }
))

# BAR FIXED
ids.append(chart(
    "🗺 Tổng Giờ Xem Theo Region",
    "echarts_bar",
    {
        "x_axis":"region",
        "metrics":[
            mksql(
                "SUM(CAST(total_duration_sec AS BIGINT))",
                "Total Duration"
            )
        ],
        "x_axis_label":"Region",
        "y_axis_label":"Tổng giây"
    }
))

# TABLE CHUYÊN SÂU
ids.append(chart(
    "📋 Customer Detail Analytics",
    "table",
    {
        "all_columns":[
            "contract",
            "region",
            "rfm_segment",
            "frequency",
            "avg_duration_min",
            "device_count",
            "active_days",
            "recency_days",
            "last_seen"
        ],
        "order_by_cols":[["frequency",False]],
        "page_length":50,
        "show_cell_bars":True
    }
))

print(f"\n✅ Total charts created: {len([x for x in ids if x])}")

# =========================
# DASHBOARD
# =========================

r = post(s, h, "/api/v1/dashboard/", {
    "dashboard_title": "🎯 Customer 360 — KPLUS Analytics",
    "published": True,
    "slug": "customer360"
})

if not r:
    print("\n❌ Dashboard create failed")
    exit()

dash_id = r["id"]

print(f"\n✅ Dashboard id={dash_id}")

try:
    IP = urllib.request.urlopen(
        "https://ifconfig.me",
        timeout=3
    ).read().decode()
except:
    IP = "localhost"

print("\n" + "="*60)
print("🎉 DASHBOARD READY")
print(f"http://{IP}:8088/superset/dashboard/{dash_id}/")
print("Login: admin / Admin@123")
print("="*60)

