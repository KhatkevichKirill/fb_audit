"""
actions — change-history loader (ad activities), env-overridable.

Standalone, .env-driven version of actions.ipynb so it can be driven in
date-range batches by scripts/backfill/backfill_actions.sh.

Incremental: an (account, day) already recorded in actions_log is skipped. The
date range defaults to the last 30 days and can be overridden with
ACTIONS_START_DATE / ACTIONS_END_DATE.

Config comes from a .env file (FB_ACCESS_TOKEN, DB_*, FB_GRAPH_API_VERSION); path overridable via FB_AUDIT_ENV.
"""

import os
import sys
from datetime import datetime, timedelta

import psycopg2
from dotenv import load_dotenv
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adactivity import AdActivity
from facebook_business.adobjects.user import User
from facebook_business.api import FacebookAdsApi
from facebook_business.exceptions import FacebookRequestError, FacebookBadObjectError

from utils import (
    connection_to_database,
    reconnection_to_database,
    get_allowed_account_ids,
    modify_action_data,
    is_token_error,
)


load_dotenv(os.environ.get("FB_AUDIT_ENV", ".env"))

access_token = os.environ["FB_ACCESS_TOKEN"]
host = os.environ["DB_HOST"]
port = os.environ["DB_PORT"]
database = os.environ["DB_NAME"]
user = os.environ["DB_USER"]
password = os.environ["DB_PASSWORD"]

graph_api_version = os.environ.get("FB_GRAPH_API_VERSION", "23.0")

table = "actions"
table_log = "actions_log"


def list_days(start_date, end_date):
    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    end = datetime.strptime(end_date, "%Y-%m-%d").date()
    days = []
    while start <= end:
        days.append(start.strftime("%Y-%m-%d"))
        start += timedelta(days=1)
    return days


def has_rows(items):
    return len(items) > 0


def store_actions(cur, actions):
    for act in actions:
        cols = ",".join(act.keys())
        vals = ",".join(map(lambda x: f"%({x})s", act.keys()))
        cur.execute(f"INSERT INTO {table} ({cols}) VALUES ({vals})", act)


def log_actions(cur, account_id, day, actions):
    cur.execute(
        f"INSERT INTO {table_log} VALUES (%s, %s, %s, %s)",
        (account_id, day, has_rows(actions), datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")),
    )


def main():
    print(f"[START] actions\t{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}", flush=True)

    start_date = (datetime.utcnow() - timedelta(days=30)).strftime("%Y-%m-%d")
    yesterday = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")
    start_date = os.environ.get("ACTIONS_START_DATE", start_date)
    yesterday = os.environ.get("ACTIONS_END_DATE", yesterday)

    try:
        FacebookAdsApi.init(access_token=access_token, api_version="v" + graph_api_version)
        me = User(fbid="me")
        id_list = list(me.get_ad_accounts())
        id_list = [x.export_all_data()["account_id"] for x in id_list]
        id_list = get_allowed_account_ids(id_list)
    except FacebookRequestError as e:
        code = e._body["error"]["code"]
        print(f"[ERROR] FB API init failed (code={code}): {e}", flush=True)
        sys.exit(1)

    fields_activity = list(set(AdActivity.Field.__dict__.keys()) - {"__module__", "__doc__"})

    cur, connection = connection_to_database(host, port, database, user, password)

    print(f"start uploading\t{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}", flush=True)

    for account_id in id_list:
        account = AdAccount("act_" + account_id)
        days = tuple(list_days(start_date, yesterday))

        cur.execute(
            "SELECT date FROM actions_log WHERE date = ANY(%s::date[]) AND id = %s",
            (list(days), account_id),
        )
        days_in_db = {row[0].strftime("%Y-%m-%d") for row in cur.fetchall()}
        days = set(days) - days_in_db

        for day in days:
            since = day
            until = (datetime.strptime(day, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
            params = {"since": since, "until": until}

            try:
                raw_data = account.get_activities(fields=fields_activity, params=params)
                actions = [x.export_all_data() for x in raw_data]
                for act in actions:
                    act["account_id"] = account_id
                    act["date"] = day
                actions = [modify_action_data(act) for act in actions]

                cur, connection = reconnection_to_database(host, port, database, user, password, connection)
                log_actions(cur, account_id, day, actions)
                store_actions(cur, actions)
                connection.commit()

            except FacebookRequestError as error:
                if is_token_error(error):
                    print("[CRITICAL] Token expired during processing. Halting.", flush=True)
                    sys.exit(2)
                code = error._body["error"]["code"]
                print(f"[WARN] error with account {account_id} on day {day}: code={code}", flush=True)
            except FacebookBadObjectError:
                print(f"[WARN] bad data for account {account_id} on day {day}", flush=True)

    print(f"[END] actions\t{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}", flush=True)


if __name__ == "__main__":
    main()
