"""
account_atribute — technical attributes of each ad account, written to
property_accounts.

Targets accounts that changed (per `actions`) or appear in `insights` but have no
property row yet, skipping tombstoned objects (`deleted_objects`). Writes only
columns that exist in the table (schema-safe via utils.store_object). A 404-style
Meta error (code=100, subcode 33/1487221) tombstones the object instead.

Standalone, .env-driven port of account_atribute.ipynb.
Config: .env file (FB_ACCESS_TOKEN, DB_*, FB_GRAPH_API_VERSION); path overridable via FB_AUDIT_ENV.
"""

import os
import sys
from datetime import datetime

import psycopg2
from dotenv import load_dotenv
from tqdm import tqdm
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.user import User
from facebook_business.api import FacebookAdsApi
from facebook_business.exceptions import FacebookRequestError

from utils import (
    connection_to_database,
    reconnection_to_database,
    get_allowed_account_ids,
    store_object,
    delete_object,
    store_deleted_object,
    modify_object_data,
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

object_type = "account"
table = "property_accounts"

exclude_list = [
    "__module__", "ad_account_creation_request", "ad_account_promotable_objects",
    "funding_source", "funding_source_details", "has_page_authorized_adaccount",
    "show_checkout_experience", "__doc__", "sold_to_address", "owner_business",
    "liable_address", "marketing_messages_settings", "send_bill_to_address", "viewable_business",
]

# Accounts that changed since last capture, or appear in insights with no property row.
SELECT_TARGETS = """SELECT DISTINCT t.*
FROM (SELECT DISTINCT actions.account_id as account_id,
property_accounts.recording_date is NULL as has_not_data
FROM actions LEFT JOIN property_accounts on actions.account_id = property_accounts.account_id
WHERE object_type = 'ACCOUNT' and
(property_accounts.recording_date is NULL or actions.event_time > property_accounts.recording_date)
UNION
SELECT DISTINCT insights.account_id, False as has_not_data
from insights
left join property_accounts on insights.account_id = property_accounts.account_id
where property_accounts.account_id is null) t
LEFT JOIN deleted_objects on t.account_id = deleted_objects.object_id
WHERE deleted_objects.object_id IS NULL"""


def main():
    print(f"[START] account_atribute\t{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}", flush=True)

    try:
        FacebookAdsApi.init(access_token=access_token, api_version="v" + graph_api_version)
        me = User(fbid="me")
        account_ids = [x.export_all_data()["account_id"] for x in me.get_ad_accounts()]
        account_ids = get_allowed_account_ids(account_ids)
    except FacebookRequestError as e:
        code = e._body["error"]["code"]
        print(f"[ERROR] FB API init failed (code={code}): {e}", flush=True)
        sys.exit(1)

    cur, connection = connection_to_database(host, port, database, user, password)

    fields = list(set(AdAccount.Field.__dict__.keys()) - set(exclude_list))

    cur.execute(SELECT_TARGETS)
    rows = cur.fetchall()
    allowed = set(account_ids)
    accounts = {row[0]: row[0] for row in rows if row[0] in allowed}

    print(f"start uploading\t{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}", flush=True)

    for account_id in tqdm(accounts.keys()):
        try:
            account_data = AdAccount("act_" + account_id).api_get(fields=fields).export_all_data()
            account_data = modify_object_data(account_data)
            cur, connection = reconnection_to_database(host, port, database, user, password, connection)
            if accounts[account_id]:
                store_object(cur, connection, table, account_data)
            else:
                delete_object(cur, table, "act_" + account_id)
                store_object(cur, connection, table, account_data)
        except FacebookRequestError as error:
            if is_token_error(error):
                print("[CRITICAL] Token expired during processing. Halting.", flush=True)
                sys.exit(2)
            code = error._body["error"]["code"]
            subcode = error._body["error"].get("error_subcode")
            if code == 100 and subcode in (33, 1487221):
                cur, connection = reconnection_to_database(host, port, database, user, password, connection)
                store_deleted_object(cur, connection, account_id, object_type, account_id, "deleted_objects")
            else:
                print(f"[WARN] FacebookRequestError for account {account_id}: code={code}", flush=True)

    print(f"[END] account_atribute\t{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}", flush=True)


if __name__ == "__main__":
    main()
