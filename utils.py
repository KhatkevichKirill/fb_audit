import json
import os
import time
from datetime import datetime

import psycopg2
import requests as rq
from psycopg2 import OperationalError


def connection_to_database(host, port, database, user, password):
    connection = psycopg2.connect(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password,
    )
    cur = connection.cursor()
    return cur, connection


def reconnection_to_database(host, port, database, user, password, connection):
    if connection.closed > 0:
        connection = psycopg2.connect(
            host=host, port=port, database=database, user=user, password=password
        )
    else:
        try:
            connection.cursor().execute("SELECT 1")
        except OperationalError:
            connection = psycopg2.connect(
                host=host, port=port, database=database, user=user, password=password
            )
    return connection.cursor(), connection


def get_table_columns(cur, table):
    cur.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_name = %s",
        (table,),
    )
    return {row[0] for row in cur.fetchall()}


def dict2json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def modify_object_data(object_data):
    for key in object_data.keys():
        if isinstance(object_data[key], (list, dict)):
            object_data[key] = dict2json(object_data[key])
    object_data["recording_date"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    return object_data


def parse_fb_datetime(value):
    if not value or not isinstance(value, str):
        return value
    if " at " in value:
        try:
            return datetime.strptime(value, "%d/%m/%Y at %H:%M").strftime(
                "%Y-%m-%d %H:%M:%S"
            )
        except ValueError:
            pass
    try:
        return datetime.strptime(value[:19], "%Y-%m-%dT%H:%M:%S").strftime(
            "%Y-%m-%d %H:%M:%S"
        )
    except ValueError:
        return value


def modify_action_data(action):
    for field in ("date_time_in_timezone", "event_time"):
        if field in action:
            action[field] = parse_fb_datetime(action[field])
    if "extra_data" in action and isinstance(action["extra_data"], (dict, list)):
        action["extra_data"] = json.dumps(action["extra_data"], ensure_ascii=False)
    return action


def get_allowed_account_ids(fb_account_ids):
    raw = (os.environ.get("ACCOUNT_IDS") or "").strip()
    if not raw:
        return fb_account_ids

    allowed = {item.strip().removeprefix("act_") for item in raw.split(",") if item.strip()}
    filtered = [acc_id for acc_id in fb_account_ids if acc_id in allowed]
    return filtered


def store_object(cur, connection, table, object_data, upsert_on_id=False):
    valid_cols = get_table_columns(cur, table)
    filtered = {k: v for k, v in object_data.items() if k in valid_cols}
    if not filtered:
        return

    fields = ",".join(filtered.keys())
    values = ",".join(map(lambda x: f"%({x})s", filtered.keys()))

    if upsert_on_id and "id" in filtered:
        updates = ",".join(f"{k}=%({k})s" for k in filtered.keys() if k != "id")
        query = (
            f"INSERT INTO {table} ({fields}) values ({values}) "
            f"ON CONFLICT (id) DO UPDATE SET {updates}"
        )
    else:
        query = f"INSERT INTO {table} ({fields}) values ({values})"

    cur.execute(query, filtered)
    connection.commit()


def delete_object(cur, table, object_id):
    query = f"DELETE FROM {table} WHERE id = %s"
    cur.execute(query, vars=(object_id,))


def store_deleted_object(cur, connection, object_id, object_type, account_id, table):
    today = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    query = f"INSERT INTO {table} values (%s, %s, %s, %s)"
    cur.execute(query, vars=(object_id, account_id, object_type, today))
    connection.commit()


# ---------------------------------------------------------------------------
# Rate-limit handling (shared by every Meta Marketing API script)
#
# Centralised here so insights / actions / intraday / breakdowns all use the
# same thresholds and back-off policy. Ported from the production fb_audit
# scripts where each module previously carried its own copy.
# ---------------------------------------------------------------------------

# Meta error codes worth retrying with back-off (transient / rate related).
TRANSIENT_FB_CODES = {1, 2, 4, 17, 341}
# error_subcode that accompanies "Application request limit reached" (code=4).
RATE_LIMIT_SUBCODE = 1504022
# Token-level failure — never retry, halt the run.
TOKEN_INVALID_CODE = 190

# Throttle thresholds (percent of quota). Above these we pause/requeue.
APP_UTIL_THRESHOLD = 60
ACC_UTIL_THRESHOLD = 70

MAX_RETRIES = 3          # per (account[, day, pass]) before giving up
BACKOFF_BASE_SECONDS = 5  # exponential: 5s, 10s, 20s, ...


def graph_api_version():
    """Single source of truth for the Graph API version used in REST calls.

    Reads FB_GRAPH_API_VERSION (e.g. "23.0") and returns it without the leading
    'v'. Defaults to 23.0 so a missing env var doesn't silently break REST URLs.
    """
    return os.environ.get("FB_GRAPH_API_VERSION", "23.0")


def check_limit(account_id, access_token, version=None):
    """Read Meta's per-call throttle header for an account.

    Returns the parsed ``x-fb-ads-insights-throttle`` JSON, e.g.::

        {"app_id_util_pct": 3.2, "acc_id_util_pct": 1.1, "ads_api_access_tier": "..."}

    A single cheap insights GET surfaces the current utilisation so callers can
    decide whether to proceed, sleep, or requeue. Call it ONCE per account and
    reuse the result — calling it twice per check needlessly doubles call volume.
    """
    version = version or graph_api_version()
    response = rq.get(
        "https://graph.facebook.com/v"
        + version
        + "/act_"
        + account_id
        + "/insights?access_token="
        + access_token
    )
    usage = response.headers["x-fb-ads-insights-throttle"]
    return json.loads(usage)


def rate_limit_exceeded(throttle, app_threshold=APP_UTIL_THRESHOLD, acc_threshold=ACC_UTIL_THRESHOLD):
    """True when the throttle header reports usage above either threshold."""
    return (
        throttle.get("app_id_util_pct", 0) >= app_threshold
        or throttle.get("acc_id_util_pct", 0) >= acc_threshold
    )


def backoff_seconds(attempt, base=BACKOFF_BASE_SECONDS):
    """Exponential back-off delay for retry ``attempt`` (0-indexed): 5, 10, 20, ..."""
    return base * (2 ** attempt)


def is_rate_limit_error(error):
    """True when a FacebookRequestError is the 'request limit reached' rate error."""
    try:
        body = error._body["error"]
        return body["code"] == 4 and body.get("error_subcode") == RATE_LIMIT_SUBCODE
    except (AttributeError, KeyError, TypeError):
        return False


def is_token_error(error):
    """True when a FacebookRequestError is a token-invalid (code 190) failure."""
    try:
        return error._body["error"]["code"] == TOKEN_INVALID_CODE
    except (AttributeError, KeyError, TypeError):
        return False
