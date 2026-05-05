import os
import sys
import json
import time
from datetime import datetime

import psycopg2
import requests as rq
from dotenv import load_dotenv
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.user import User
from facebook_business.api import FacebookAdsApi
from facebook_business.exceptions import FacebookRequestError

from utils import get_allowed_account_ids


load_dotenv("/opt/fb_audit/.env")

access_token = os.environ["FB_ACCESS_TOKEN"]
host = os.environ["DB_HOST"]
port = os.environ["DB_PORT"]
database = os.environ["DB_NAME"]
user = os.environ["DB_USER"]
password = os.environ["DB_PASSWORD"]

graph_api_version = os.environ.get("FB_GRAPH_API_VERSION", "23.0")


def connection_to_database(host, port, database, user, password):
    connection = psycopg2.connect(
        host=host, port=port, database=database, user=user, password=password
    )
    cur = connection.cursor()
    return cur, connection


def dict2json(dictionary):
    return json.dumps(dictionary, ensure_ascii=False, sort_keys=True)


def modify_object(object_data):
    for key in object_data.keys():
        if isinstance(object_data[key], (list, dict)):
            object_data[key] = dict2json(object_data[key])
    return object_data


def store_data(cur, data, table):
    for row in data:
        fields = ",".join(row.keys())
        values = ",".join(map(lambda x: f"%({x})s", row.keys()))
        query = f"INSERT INTO {table} ({fields}) values ({values})"
        cur.execute(query, row)


def check_limit(account_id, version=None):
    version = version or graph_api_version
    check = rq.get(
        "https://graph.facebook.com/v"
        + version
        + "/act_"
        + account_id
        + "/insights?access_token="
        + access_token
    )
    usage = check.headers["x-fb-ads-insights-throttle"]
    return json.loads(usage)


def ensure_intraday_table(cur, connection, table):
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table}(
            account_id VARCHAR(50),
            campaign_id VARCHAR(50),
            adset_id VARCHAR(50),
            ad_id VARCHAR(50),
            date_start DATE,
            date_stop DATE,
            impressions VARCHAR(50),
            reach VARCHAR(50),
            clicks VARCHAR(50),
            spend VARCHAR(50),
            unique_inline_link_clicks VARCHAR(50),
            inline_link_clicks VARCHAR(50),
            inline_post_engagement VARCHAR(50),
            estimated_ad_recallers VARCHAR(50),
            estimated_ad_recall_rate VARCHAR(50),
            objective VARCHAR(50),
            unique_clicks VARCHAR(50),
            actions JSONB,
            action_values JSONB,
            outbound_clicks JSONB,
            unique_actions JSONB,
            unique_outbound_clicks JSONB,
            video_p25_watched_actions JSONB,
            video_p50_watched_actions JSONB,
            video_p75_watched_actions JSONB,
            video_p95_watched_actions JSONB,
            results JSONB,
            cost_per_result JSONB,
            collected_at timestamp without time zone DEFAULT now()
        )
        """
    )
    connection.commit()


print(f"[START] intraday_insights\t{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}")

try:
    FacebookAdsApi.init(access_token=access_token)
except FacebookRequestError as e:
    code = e._body["error"]["code"]
    print(f"[ERROR] FB API init failed (code={code}): {e}")
    sys.exit(1)

cur, connection = connection_to_database(host, port, database, user, password)
table = "intraday_insights"
ensure_intraday_table(cur, connection, table)

me = User(fbid="me")
id_list = list(me.get_ad_accounts())
id_list = list(map(lambda x: x.export_all_data(), id_list))
id_list = list(map(lambda x: x["account_id"], id_list))
id_list = get_allowed_account_ids(id_list)

fields = [
    "account_id",
    "campaign_id",
    "adset_id",
    "ad_id",
    "date_start",
    "date_stop",
    "impressions",
    "reach",
    "clicks",
    "spend",
    "unique_inline_link_clicks",
    "inline_link_clicks",
    "inline_post_engagement",
    "estimated_ad_recallers",
    "estimated_ad_recall_rate",
    "objective",
    "unique_clicks",
    "actions",
    "action_values",
    "outbound_clicks",
    "unique_actions",
    "unique_outbound_clicks",
    "video_p25_watched_actions",
    "video_p50_watched_actions",
    "video_p75_watched_actions",
    "video_p95_watched_actions",
    "results",
    "cost_per_result",
]
action_attribution_windows = ["1d_view", "1d_click", "7d_click", "28d_click"]

MAX_RETRIES = 3
pending_accounts = list(id_list)
retry_counts = {}

while pending_accounts:
    account_id = pending_accounts.pop(0)
    try:
        limit = check_limit(account_id)
        if limit["app_id_util_pct"] >= 60 or limit["acc_id_util_pct"] >= 70:
            retries = retry_counts.get(account_id, 0)
            if retries < MAX_RETRIES:
                retry_counts[account_id] = retries + 1
                print(
                    f"[WARN] Rate limit reached for {account_id}, sleep 30s "
                    f"(attempt {retries + 1}/{MAX_RETRIES})"
                )
                time.sleep(30)
                pending_accounts.append(account_id)
            else:
                print(
                    f"[WARN] Rate limit still high for {account_id} after "
                    f"{MAX_RETRIES} retries, skipping"
                )
            continue

        cur.execute(f"DELETE FROM {table} WHERE account_id = %s", (account_id,))

        params = {
            "level": "ad",
            "date_preset": "today",
            "action_attribution_windows": action_attribution_windows,
            "fields": fields,
        }
        response = AdAccount("act_" + account_id).get_insights(params=params, is_async=False)
        data = list(response)

        if data:
            data = list(map(lambda x: x.export_all_data(), data))
            for row in data:
                modify_object(row)
            store_data(cur, data, table)

        connection.commit()
        print(f"[INFO] intraday account={account_id} rows={len(data)}")

    except FacebookRequestError as error:
        code = error._body["error"]["code"]
        subcode = error._body["error"].get("error_subcode", None)
        if code == 190:
            print("[CRITICAL] Token expired during processing. Halting.")
            sys.exit(2)
        if code == 4 and subcode == 1504022:
            print(f"[WARN] Rate limit error for {account_id}, sleeping 20s")
            time.sleep(20)
            pending_accounts.append(account_id)
            continue
        print(f"[WARN] FacebookRequestError for {account_id}: code={code}")
    except Exception as error:
        print(f"[WARN] Unexpected error for {account_id}: {error}")

print(f"[END] intraday_insights\t{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}")
