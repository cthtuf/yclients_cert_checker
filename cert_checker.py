from datetime import datetime, timedelta
from os import getenv

from google.cloud import datastore
from telegram_send.telegram_send import send
import requests

YC_REQUEST_URL = getenv("YC_REQUEST_URL")
YC_AUTH_COOKIE = getenv("YC_AUTH_COOKIE")
YC_GOOD_ID = getenv("YC_GOOD_ID")
TG_TOKEN = getenv("TG_TOKEN")
TG_CHAT_ID = getenv("TG_CHAT_ID")
TG_NOTIFICATION_MESSAGE = getenv("TG_NOTIFICATION_MESSAGE")


def get_last_count_from_fb(client, *, day):
    """Get last count value from firebase for given day."""
    key = client.key('cert-checker', day)

    entity = client.get(key=key)
    if not entity:
        print(f"No entities for {day} creating new one.")
        entity = datastore.Entity(key=key)
        entity.update({"count": 0})
        client.put(entity)

    return entity["count"]


def set_last_count_to_fb(client, *, day, count):
    """Set last count for given day to firebase."""
    key = client.key('cert-checker', day)
    entity = client.get(key=key)
    if not entity:
        notify("Error on update data in Firebase")
        raise ValueError(f"No entities for {day}, expecting one. Skip update.")

    entity["count"] = count
    client.put(entity)
    print(f"Set last_count={count} for {day}")


def get_last_count_from_yc(*, dt_from, dt_to):
    """Get last count from YClients."""
    url = f"{YC_REQUEST_URL}?date_start={dt_from}&date_end={dt_to}&good_id={YC_GOOD_ID}"

    headers = {
        "accept": "application/json",
        "cookie": f"auth={YC_AUTH_COOKIE};"
    }

    response = requests.request("GET", url, headers=headers)

    response.raise_for_status()
    data = response.json()
    if not data.get("success", False):
        notify("Error on check certs from YClients")
        raise ValueError(f"Non-successfull response. Response.content={response.content.decode()}")

    return data["count"]


def notify(message):
    """Send message to telegram."""
    config = {
        "token": TG_TOKEN,
        "chat_id": TG_CHAT_ID,
    }
    send(messages=[message], conf=config)


def checkcert_pubsub(event, context):
    """
    Entrypoint. Executed by Cloud Scheduler.

    It takes a count of items from list view YC_REQUEST_URL, compare it with last checked amount, if new item appears,
    it sends notification to telegram chat through forked telegram_send (check it in my repos list).
    """
    client = datastore.Client()
    today = (datetime.now().strftime("%d.%m.%Y"))
    tomorrow = ((datetime.now() + timedelta(days=1)).strftime("%d.%m.%Y"))

    last_count_yc = get_last_count_from_yc(dt_from=today, dt_to=tomorrow)
    last_count_fb = get_last_count_from_fb(client, day=today)
    if last_count_yc > last_count_fb:
        print("New cert found")
        notify(message=TG_NOTIFICATION_MESSAGE.format(dt_from=today, dt_to=tomorrow))  # format harcoded in env value
        set_last_count_to_fb(client, day=today, count=last_count_yc)
