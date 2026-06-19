"""
creative_atribute — technical attributes of each ad creative, written to
property_creatives.

Targets creatives referenced by a property_ads row but not yet captured in
property_creatives, skipping tombstoned objects. A 404-style Meta error tombstones
the object instead.

Standalone, .env-driven port of creative_atribute.ipynb.
Config: .env file (FB_ACCESS_TOKEN, DB_*, FB_GRAPH_API_VERSION); path overridable via FB_AUDIT_ENV.
"""

import os
import sys
from datetime import datetime

import psycopg2
from dotenv import load_dotenv
from tqdm import tqdm
from facebook_business.adobjects.adcreative import AdCreative
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

object_type = "creative"
table = "property_creatives"

exclude_list = [
    "__module__", "__doc__", "call_to_action", "image_file", "is_dco_internal",
    "execution_options", "marketing_message_structured_spec",
]

SELECT_TARGETS = """SELECT DISTINCT ad.account_id, ad.id as ad_id, ad.creative ->> 'id' as creative_id,
True as has_not_data
FROM property_ads ad
LEFT JOIN property_creatives creative on creative.id = ad.creative ->> 'id'
LEFT JOIN deleted_objects on ad.creative ->> 'id' = deleted_objects.object_id
WHERE creative.id IS NULL and deleted_objects.object_id IS NULL"""


def main():
    print(f"[START] creative_atribute\t{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}", flush=True)

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

    fields = list(set(AdCreative.Field.__dict__.keys()) - set(exclude_list))

    cur.execute(SELECT_TARGETS)
    rows = cur.fetchall()
    allowed = set(account_ids)
    # columns: account_id, ad_id, creative_id, has_not_data
    creatives = {row[2]: row[3] for row in rows if row[0] in allowed}
    accounts = {row[2]: row[0] for row in rows if row[0] in allowed}

    print(f"start uploading\t{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}", flush=True)

    for creative_id in tqdm(creatives.keys()):
        try:
            creative_data = AdCreative(creative_id).api_get(fields=fields).export_all_data()
            creative_data = modify_object_data(creative_data)
            cur, connection = reconnection_to_database(host, port, database, user, password, connection)
            if creatives[creative_id]:
                store_object(cur, connection, table, creative_data)
            else:
                delete_object(cur, table, creative_id)
                store_object(cur, connection, table, creative_data)
        except FacebookRequestError as error:
            if is_token_error(error):
                print("[CRITICAL] Token expired during processing. Halting.", flush=True)
                sys.exit(2)
            code = error._body["error"]["code"]
            subcode = error._body["error"].get("error_subcode")
            if code == 100 and subcode in (33, 1487221):
                cur, connection = reconnection_to_database(host, port, database, user, password, connection)
                store_deleted_object(cur, connection, creative_id, object_type, accounts[creative_id], "deleted_objects")
            else:
                print(f"[WARN] FacebookRequestError for creative {creative_id}: code={code}", flush=True)

    print(f"[END] creative_atribute\t{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}", flush=True)


if __name__ == "__main__":
    main()
