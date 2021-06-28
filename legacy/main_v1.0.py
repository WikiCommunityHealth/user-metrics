import bz2

import pymongo
from pymongo import MongoClient, UpdateOne

from utils import scraper
from utils.dump import KEYS
from utils import dump
from utils.logger import log

YEAR_START = 2001
YEAR_END = 2021

CURRENT_YEAR = '2001'
CURRENT_MONTH = '01'

MAX_USERS = int(2.2e6)
USER_STATUS = [None] * MAX_USERS  # new - old - None
USER_DOCUMENT = [None] * MAX_USERS
USER_MONTH_EVENTS = [None] * MAX_USERS

EVENT = {
    'create': 'create',
    'create-page': 'create',
    'delete': 'delete',
    'restore': 'restore',
    'move': 'move',
    'merge': 'merge',
    'edit': 'edit'
}

client = MongoClient()
users_collection = client.get_database('wiki').get_collection('users')
users_collection.create_index([('id', pymongo.ASCENDING)], name='id_index', unique=True)


def get_tsv_year(tsv_file_name: str) -> int:
    return int(tsv_file_name.split('.')[::-1][1])


def new_user_obj(uid: int, username: str, creation_timestamp, registration_timestamp, first_edit_timestamp) -> dict:
    return {'id': uid, 'username': username, 'creation_timestamp': creation_timestamp, 'registration_timestamp': registration_timestamp, 'first_edit_timestamp': first_edit_timestamp, 'events': {'per_month': {}}}


def new_events_obj() -> dict:
    return {'create': 0, 'delete': 0, 'restore': 0, 'move': 0, 'merge': 0, 'edit': 0}


def new_namespace_obj() -> list:
    return {}


def two_digits(n: int) -> str:
    return str(n) if n > 9 else '0' + str(n)


def update_namespace(uid: str, namespace: str, event_type: str):
    if USER_MONTH_EVENTS[uid] is None:
        USER_MONTH_EVENTS[uid] = new_namespace_obj()

    user_month = USER_MONTH_EVENTS[uid]
    if namespace not in user_month:
        user_month[namespace] = {}

    user_month_namespace = user_month[namespace]
    if event_type not in user_month_namespace:
        user_month_namespace[event_type] = 1
    else:
        user_month_namespace[event_type] += 1


def analyze_page_or_revision(event_type: str, parts: list[str]):
    uid = dump.parse_int(parts[KEYS['event_user_id']])
    if uid is not None:
        if USER_STATUS[uid] is None:
            username = parts[KEYS['event_user_text']]
            creation_timestamp = dump.parse_date(parts[KEYS['event_user_creation_timestamp']])
            registration_timestamp = dump.parse_date(parts[KEYS['event_user_registration_timestamp']])
            first_edit_timestamp = dump.parse_date(parts[KEYS['event_user_first_edit_timestamp']])
            USER_DOCUMENT[uid] = new_user_obj(uid, username, creation_timestamp, registration_timestamp, first_edit_timestamp)
            USER_STATUS[uid] = 'new'
            USER_MONTH_EVENTS[uid] = new_namespace_obj()

        namespace = dump.parse_int(parts[KEYS['page_namespace']])
        namespace = f'n{namespace}' if namespace is not None else 'unknown'
        update_namespace(uid, namespace, EVENT[event_type])


def analyze_user(parts: list[str]):
    uid = dump.parse_int(parts[KEYS['user_id']])
    if uid is not None:
        if USER_STATUS[uid] is None:
            username = parts[KEYS['user_text']]
            creation_timestamp = dump.parse_date(parts[KEYS['user_creation_timestamp']])
            registration_timestamp = dump.parse_date(parts[KEYS['user_registration_timestamp']])
            first_edit_timestamp = dump.parse_date(parts[KEYS['user_first_edit_timestamp']])
            USER_DOCUMENT[uid] = new_user_obj(uid, username, creation_timestamp, registration_timestamp, first_edit_timestamp)
            USER_STATUS[uid] = 'new'
            USER_MONTH_EVENTS[uid] = new_namespace_obj()


def analyze_file(file_path: str) -> None:
    log('Start filling users')
    with bz2.open(file_path, 'rt') as input:
        for line in input:
            parts = line.split('\t')

            timestamp = dump.parse_date(parts[KEYS['event_timestamp']])

            check_if_new_month(timestamp, True)

            event_entity = parts[KEYS['event_entity']]
            event_type = parts[KEYS['event_type']]

            if event_entity == 'revision':
                analyze_page_or_revision('edit', parts)
            elif event_entity == 'page':
                analyze_page_or_revision(event_type, parts)
            elif event_entity == 'user' and event_type == 'create':
                analyze_user(parts)
    log('End fill users')


def check_if_new_month(timestamp, check: bool):
    global CURRENT_MONTH
    month = two_digits(timestamp.month)
    if (not check or month != CURRENT_MONTH):
        year = str(timestamp.year)
        log(f'New month {month}')
        update_db(year, month)
        CURRENT_MONTH = month


def convert_array_events_to_json(raw):
    result = {}

    for ns, events in enumerate(raw):
        result[f'ns{ns}'] = {
            'create': events[0],
            'delete': events[1],
            'restore': events[2],
            'move': events[3],
            'merge': events[4],
            'edit': events[5]
        }

    return result


def update_db(year: str, month: str):
    log(f'Uploading {year}/{month}')

    inserts = []
    updates = []

    def add_update(uid: str, obj: dict) -> None:
        updates.append(UpdateOne({'id': uid}, {'$set': {f'events.per_month.{year}.{month}': obj}}))

    for uid in range(0, MAX_USERS):
        status = USER_STATUS[uid]
        if status == 'new':
            inserts.append(USER_DOCUMENT[uid])
            USER_DOCUMENT[uid] = None
            USER_STATUS[uid] = 'old'
        if USER_MONTH_EVENTS[uid] is not None:
            add_update(uid, USER_MONTH_EVENTS[uid])
            USER_DOCUMENT[uid] = None
            USER_MONTH_EVENTS[uid] = None

    log('Gotten inserts and updates')
    if (len(inserts) > 0):
        users_collection.insert_many(inserts)
    log('Added inserts')
    if (len(updates) > 0):
        users_collection.bulk_write(updates)
    log('Added updates')
    log('Finished update')


scraper.download_wiki('itwiki')


for path in scraper.get_tsv_files('itwiki'):
    CURRENT_YEAR = str(get_tsv_year(path.stem))
    log(f'Doing {CURRENT_YEAR}')
    analyze_file(str(path))
    log('Done')
