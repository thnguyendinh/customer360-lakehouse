import json, random, time, string, boto3, argparse, logging, os
from datetime import datetime, timedelta
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

BUCKET = os.environ.get("S3_BUCKET", "customer360-lakehouse")
REGION_AWS = os.environ.get("AWS_DEFAULT_REGION", "ap-southeast-1")
APP_NAME = "KPLUS"

CONTRACT_PREFIXES = ["HNH","HND","HNJ","HUFD","DNFD","DTFD","LDFD","NTFD","TBAAA","NBAAA"]
REGIONS = {"HN":0.30,"HCM":0.35,"DN":0.12,"CT":0.08,"HP":0.07,"BD":0.04,"BG":0.04}
PEAK_HOURS = {6:0.3,7:0.5,12:0.7,13:0.6,18:1.0,19:1.0,20:0.9,21:0.8,22:0.5}

def random_mac(): return "".join(random.choices("0123456789ABCDEF",k=12))
def random_contract(): return random.choice(CONTRACT_PREFIXES)+str(random.randint(100000,999999))
def random_id(): return "AX_"+"".join(random.choices(string.ascii_letters+string.digits+"_-",k=14))
def weighted_region(): return random.choices(list(REGIONS.keys()),weights=list(REGIONS.values()))[0]
def realistic_duration(hour): return max(60, int(random.randint(60,7200)*(0.5+PEAK_HOURS.get(hour,0.2)*0.8)))
def num_records(hour): return random.randint(10+int(PEAK_HOURS.get(hour,0.2)*100), 50+int(PEAK_HOURS.get(hour,0.2)*450))

def generate_records(n, event_time):
    h=event_time.hour
    return [{ "_index":"history","_type":"kplus","_id":random_id(),"_score":0,
               "_source":{ "Contract":random_contract(),"Mac":random_mac(),
                           "TotalDuration":realistic_duration(h),"AppName":APP_NAME,
                           "EventTime":event_time.isoformat(),"Region":weighted_region() } }
             for _ in range(n)]

def upload_to_s3(s3_client, records, event_time, dry_run=False):
    key=f"bronze/{event_time.strftime('%Y%m%d')}/{event_time.strftime('%H%M%S')}.json"
    body="\n".join(json.dumps(r) for r in records)
    if dry_run:
        log.info(f"[DRY RUN] {len(records)} records → s3://{BUCKET}/{key}")
        return key
    s3_client.put_object(Bucket=BUCKET, Key=key, Body=body.encode("utf-8"), ContentType="application/x-ndjson")
    log.info(f"✅ {len(records):>4} records → s3://{BUCKET}/{key}")
    return key

def backfill(s3_client, days=14):
    log.info(f"🔄 Backfill {days} ngày lịch sử...")
    for offset in range(days,0,-1):
        day=datetime.now()-timedelta(days=offset)
        for hour in range(24):
            for batch in range(3):
                t=day.replace(hour=hour, minute=batch*20, second=0)
                r=generate_records(num_records(hour), t)
                upload_to_s3(s3_client, r, t)
    log.info("🎉 Backfill xong")

def run(interval_sec=300, dry_run=False, do_backfill=False):
    s3=boto3.client("s3", region_name=REGION_AWS)
    if do_backfill: backfill(s3)
    if interval_sec==0:
        now=datetime.now()
        upload_to_s3(s3, generate_records(num_records(now.hour), now), now, dry_run)
        return
    log.info(f"🚀 Loop generator interval={interval_sec}s")
    while True:
        now=datetime.now()
        upload_to_s3(s3, generate_records(num_records(now.hour), now), now, dry_run)
        time.sleep(interval_sec)

if __name__=="__main__":
    p=argparse.ArgumentParser()
    p.add_argument("--interval", type=int, default=300)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--backfill", action="store_true")
    a=p.parse_args()
    run(a.interval, a.dry_run, a.backfill)
