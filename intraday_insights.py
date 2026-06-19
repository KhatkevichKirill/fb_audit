"""
intraday_insights — today's ad performance snapshot, refreshed on a short cadence.

Full DELETE + INSERT per account on every run (not incremental) so the table always
reflects the latest intraday state. Only commits after a complete fetch, so a
partial/failed fetch can never overwrite a good snapshot.

Rate-limit handling, the throttle check and the DB connection helper are shared
from utils.py (previously duplicated locally).

Config: .env file (FB_ACCESS_TOKEN, DB_*, FB_GRAPH_API_VERSION); path overridable via FB_AUDIT_ENV.
"""

import os
import sys
import json
import time
from datetime import datetime

import psycopg2
from dotenv import load_dotenv
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.user import User
from facebook_business.api import FacebookAdsApi
from facebook_business.exceptions import FacebookRequestError

from utils import (
    connection_to_database,
    get_allowed_account_ids,
    check_limit,
    rate_limit_exceeded,
    is_rate_limit_error,
    is_token_error,
    MAX_RETRIES,
)


load_dotenv(os.environ.get("FB_AUDIT_ENV", ".env"))

access_token = os.environ["FB_ACCESS_TOKEN"]
host = os.environ["DB_HOST"]
port = os.environ["DB_PORT"]
database = os.environ["DB_NAME"]
user = os.environ["DB_USER"]
password = os.environ["DB_PASSWORD"]

graph_api_version = os.environ.get("FB_GRAPH_API_VERSION", "23.0")

table = "intraday_insights"
action_attribution_windows = ["1d_view", "1d_click", "7d_click", "28d_click"]

fields = [
    "account_id", "campaign_id", "adset_id", "ad_id", "date_start", "date_stop",
    "impressions", "reach", "clicks", "spend", "unique_inline_link_clicks",
    "inline_link_clicks", "inline_post_engagement", "estimated_ad_recallers",
    "estimated_ad_recall_rate", "objective", "unique_clicks", "actions",
    "action_values", "outbound_clicks", "unique_actions", "unique_outbound_clicks",
    "video_p25_watched_actions", "video_p50_watched_actions",
    "video_p75_watched_actions", "video_p95_watched_actions",
    "results", "cost_per_result",
]


def dict2json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def modify_object(object_data):
    for key in object_data.keys():
        if isinstance(object_data[key], (list, dict)):
            object_data[key] = dict2json(object_data[key])
    return object_data


def store_data(cur, data, table_name):
    for row in data:
        cols = ",".join(row.keys())
        vals = ",".join(map(lambda x: f"%({x})s", row.keys()))
        cur.execute(f"INSERT INTO {table_name} ({cols}) VALUES ({vals})", row)


def ensure_intraday_table(cur, connection, table_name):
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name}(
            account_id VARCHAR(50), campaign_id VARCHAR(50), adset_id VARCHAR(50),
            ad_id VARCHAR(50), date_start DATE, date_stop DATE,
            impressions VARCHAR(50), reach VARCHAR(50), clicks VARCHAR(50),
            spend VARCHAR(50), unique_inline_link_clicks VARCHAR(50),
            inline_link_clicks VARCHAR(50), inline_post_engagement VARCHAR(50),
            estimated_ad_recallers VARCHAR(50), estimated_ad_recall_rate VARCHAR(50),
            objective VARCHAR(50), unique_clicks VARCHAR(50),
            actions JSONB, action_values JSONB, outbound_clicks JSONB,
            unique_actions JSONB, unique_outbound_clicks JSONB,
            video_p25_watched_actions JSONB, video_p50_watched_actions JSONB,
            video_p75_watched_actions JSONB, video_p95_watched_actions JSONB,
            results JSONB, cost_per_result JSONB,
            collected_at timestamp without time zone DEFAULT now()
        )
        """
    )
    connection.commit()


def main():
    print(f"[START] intraday_insights\t{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}", flush=True)

    try:
        FacebookAdsApi.init(access_token=access_token, api_version="v" + graph_api_version)
    except FacebookRequestError as e:
        code = e._body["error"]["code"]
        print(f"[ERROR] FB API init failed (code={code}): {e}", flush=True)
        sys.exit(1)

    cur, connection = connection_to_database(host, port, database, user, password)
    ensure_intraday_table(cur, connection, table)

    me = User(fbid="me")
    id_list = [x.export_all_data()["account_id"] for x in me.get_ad_accounts()]
    id_list = get_allowed_account_ids(id_list)

    pending_accounts = list(id_list)
    retry_counts = {}

    while pending_accounts:
        account_id = pending_accounts.pop(0)
        try:
            throttle = check_limit(account_id, access_token, graph_api_version)
            if rate_limit_exceeded(throttle):
                retries = retry_counts.get(account_id, 0)
                if retries < MAX_RETRIES:
                    retry_counts[account_id] = retries + 1
                    print(f"[WARN] Throttle high for {account_id}, sleep 30s "
                          f"(attempt {retries + 1}/{MAX_RETRIES})", flush=True)
                    time.sleep(30)
                    pending_accounts.append(account_id)
                else:
                    print(f"[WARN] Throttle still high for {account_id} after {MAX_RETRIES} retries, skipping", flush=True)
                continue

            params = {
                "level": "ad",
                "date_preset": "today",
                "action_attribution_windows": action_attribution_windows,
                "fields": fields,
            }
            response = AdAccount("act_" + account_id).get_insights(params=params, is_async=False)
            data = list(response)

            # Only delete + insert once the fetch succeeded — a partial/failed fetch
            # never overwrites the previous good snapshot.
            cur.execute(f"DELETE FROM {table} WHERE account_id = %s", (account_id,))
            if data:
                data = [obj.export_all_data() for obj in data]
                for row in data:
                    modify_object(row)
                store_data(cur, data, table)
            connection.commit()
            print(f"[INFO] intraday account={account_id} rows={len(data)}", flush=True)

        except FacebookRequestError as error:
            connection.rollback()
            if is_token_error(error):
                print("[CRITICAL] Token expired during processing. Halting.", flush=True)
                sys.exit(2)
            if is_rate_limit_error(error):
                print(f"[WARN] Rate-limit error for {account_id}, sleeping 20s", flush=True)
                time.sleep(20)
                pending_accounts.append(account_id)
                continue
            code = error._body["error"]["code"]
            print(f"[WARN] FacebookRequestError for {account_id}: code={code}", flush=True)
        except Exception as error:
            connection.rollback()
            print(f"[WARN] Unexpected error for {account_id}: {error}", flush=True)

    print(f"[END] intraday_insights\t{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}", flush=True)


if __name__ == "__main__":
    main()
