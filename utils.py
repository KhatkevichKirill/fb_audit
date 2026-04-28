import json
import os
from datetime import datetime

import psycopg2
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
