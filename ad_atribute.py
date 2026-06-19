"""
ad_atribute — technical attributes of each ad, written to property_ads.

Targets ads that changed (per `actions`, object_type=ADGROUP) or appear in
`insights` with no property row yet, skipping tombstoned objects. Uses an upsert
(ON CONFLICT (id) DO UPDATE) so a re-seen ad refreshes in place. A 404-style Meta
error tombstones the object instead.

Standalone, .env-driven port of ad_atribute.ipynb.
Config: .env file (FB_ACCESS_TOKEN, DB_*, FB_GRAPH_API_VERSION); path overridable via FB_AUDIT_ENV.
"""

import os
import sys
from datetime import datetime

import psycopg2
from dotenv import load_dotenv
from tqdm import tqdm
from facebook_business.adobjects.ad import Ad
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

object_type = "ad"
table = "property_ads"

exclude_list = [
    "__module__", "__doc__", "adset_spec", "audience_id", "date_format",
    "draft_adgroup_id", "execution_options", "is_autobid",
    "include_demolink_hashes", "filename", "placement",
]

SELECT_TARGETS = """SELECT DISTINCT t.*
FROM (SELECT DISTINCT actions.object_id as ad_id, actions.account_id,
property_ads.recording_date is NULL as has_not_data
FROM actions LEFT JOIN property_ads on actions.object_id = property_ads.id
WHERE object_type = 'ADGROUP' and
(property_ads.recording_date is NULL or actions.event_time > property_ads.recording_date)
UNION
SELECT DISTINCT insights.ad_id, insights.account_id, False as has_not_data
from insights
left join property_ads on insights.ad_id = property_ads.id
where property_ads.id is null) t
LEFT JOIN deleted_objects on t.ad_id = deleted_objects.object_id
WHERE deleted_objects.object_id IS NULL"""


def main():
    print(f"[START] ad_atribute\t{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}", flush=True)

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

    fields = list(set(Ad.Field.__dict__.keys()) - set(exclude_list))

    cur.execute(SELECT_TARGETS)
    rows = cur.fetchall()
    allowed = set(account_ids)
    ads = {row[0]: row[2] for row in rows if row[1] in allowed}       # ad_id -> has_not_data
    accounts = {row[0]: row[1] for row in rows if row[1] in allowed}  # ad_id -> account_id

    print(f"start uploading\t{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}", flush=True)

    for ad_id in tqdm(ads.keys()):
        try:
            ad_data = Ad(ad_id).api_get(fields=fields).export_all_data()
            ad_data = modify_object_data(ad_data)
            cur, connection = reconnection_to_database(host, port, database, user, password, connection)
            if ads[ad_id]:
                store_object(cur, connection, table, ad_data, upsert_on_id=True)
            else:
                delete_object(cur, table, ad_id)
                store_object(cur, connection, table, ad_data, upsert_on_id=True)
        except FacebookRequestError as error:
            if is_token_error(error):
                print("[CRITICAL] Token expired during processing. Halting.", flush=True)
                sys.exit(2)
            code = error._body["error"]["code"]
            subcode = error._body["error"].get("error_subcode")
            if code == 100 and subcode in (33, 1487221):
                cur, connection = reconnection_to_database(host, port, database, user, password, connection)
                store_deleted_object(cur, connection, ad_id, object_type, accounts[ad_id], "deleted_objects")
            else:
                print(f"[WARN] FacebookRequestError for ad {ad_id}: code={code}", flush=True)

    print(f"[END] ad_atribute\t{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}", flush=True)


if __name__ == "__main__":
    main()
