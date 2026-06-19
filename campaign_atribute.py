"""
campaign_atribute — technical attributes of each campaign, written to
property_campaigns.

Targets campaigns that changed (per `actions`, object_type=CAMPAIGN_GROUP) or
appear in `insights` with no property row yet, skipping tombstoned objects. A
404-style Meta error tombstones the object instead.

Standalone, .env-driven port of campaign_atribute.ipynb.
Config: .env file (FB_ACCESS_TOKEN, DB_*, FB_GRAPH_API_VERSION); path overridable via FB_AUDIT_ENV.
"""

import os
import sys
from datetime import datetime

import psycopg2
from dotenv import load_dotenv
from tqdm import tqdm
from facebook_business.adobjects.campaign import Campaign
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

object_type = "campaign"
table = "property_campaigns"

exclude_list = [
    "__module__", "__doc__", "adbatch", "execution_options",
    "iterative_split_test_configs", "upstream_events", "budget_schedule_specs",
]

SELECT_TARGETS = """SELECT DISTINCT t.*
FROM (SELECT DISTINCT actions.object_id as campaign_id, actions.account_id,
property_campaigns.recording_date is NULL as has_not_data
FROM actions LEFT JOIN property_campaigns on actions.object_id = property_campaigns.id
WHERE object_type = 'CAMPAIGN_GROUP' and
(property_campaigns.recording_date is NULL or actions.event_time > property_campaigns.recording_date)
UNION
SELECT DISTINCT insights.campaign_id, insights.account_id, False as has_not_data
from insights
left join property_campaigns on insights.campaign_id = property_campaigns.id
where property_campaigns.id is null) t
LEFT JOIN deleted_objects on t.campaign_id = deleted_objects.object_id
WHERE deleted_objects.object_id IS NULL"""


def main():
    print(f"[START] campaign_atribute\t{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}", flush=True)

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

    fields = list(set(Campaign.Field.__dict__.keys()) - set(exclude_list))

    cur.execute(SELECT_TARGETS)
    rows = cur.fetchall()
    allowed = set(account_ids)
    campaigns = {row[0]: row[2] for row in rows if row[1] in allowed}
    accounts = {row[0]: row[1] for row in rows if row[1] in allowed}

    print(f"start uploading\t{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}", flush=True)

    for campaign_id in tqdm(campaigns.keys()):
        try:
            campaign_data = Campaign(campaign_id).api_get(fields=fields).export_all_data()
            campaign_data = modify_object_data(campaign_data)
            cur, connection = reconnection_to_database(host, port, database, user, password, connection)
            if campaigns[campaign_id]:
                store_object(cur, connection, table, campaign_data)
            else:
                delete_object(cur, table, campaign_id)
                store_object(cur, connection, table, campaign_data)
        except FacebookRequestError as error:
            if is_token_error(error):
                print("[CRITICAL] Token expired during processing. Halting.", flush=True)
                sys.exit(2)
            code = error._body["error"]["code"]
            subcode = error._body["error"].get("error_subcode")
            if code == 100 and subcode in (33, 1487221):
                cur, connection = reconnection_to_database(host, port, database, user, password, connection)
                store_deleted_object(cur, connection, campaign_id, object_type, accounts[campaign_id], "deleted_objects")
            else:
                print(f"[WARN] FacebookRequestError for campaign {campaign_id}: code={code}", flush=True)

    print(f"[END] campaign_atribute\t{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}", flush=True)


if __name__ == "__main__":
    main()
