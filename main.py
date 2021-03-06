import bz2

import math
import pymongo
from pymongo import MongoClient, UpdateOne
from collections import Counter
from sys import argv

from joblib import Parallel, delayed
import multiprocessing

from utils import scraper
from utils.dump import KEYS
from utils import dump
from utils.logger import log

lang = argv[1]
# scraper.sync_wikies(lang)


def solve_file(path: str, start_year: int, start_month: int) -> None:
    CURRENT_MONTH = two_digits(start_month) if start_month is not None else '01'

    USER_EXISTS = set()
    USER_DOCUMENT_UPDATE = {}
    USER_DOCUMENT = {}
    USER_MONTH_EVENTS = {}
    USER_ALTERS_GROUPS = {}
    USER_ALTERS_BLOCKS = {}
    USER_HELPER_INFO = {}

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
    users_collection = client.get_database('user_metrics').get_collection(f'{lang}wiki_users')
    users_collection.create_index([('id', pymongo.ASCENDING)], name='id_index', unique=True)

    def new_user_obj(uid: int, username: str, creation_timestamp, registration_timestamp, is_bot: bool, groups: list, blocks: list) -> dict:
        return {'id': uid, 'username': username, 'creation_timestamp': creation_timestamp, 'registration_timestamp': registration_timestamp, 'is_bot': is_bot, 'groups': groups, 'blocks': blocks, 'events': {'per_month': {}}}

    def new_user_update_obj(uid: int, username: str, creation_timestamp, registration_timestamp, is_bot: bool, groups: list, blocks: list) -> dict:
        return {'id': uid, 'username': username, 'creation_timestamp': creation_timestamp, 'registration_timestamp': registration_timestamp, 'is_bot': is_bot, 'groups': groups, 'blocks': blocks}

    def new_month_obj() -> list:
        return {'max_inact_interval': 0}

    def two_digits(n: int) -> str:
        return str(n) if n > 9 else '0' + str(n)

    def update_month_object(uid: str, namespace: str, event_type: str, timestamp, minor_edit: bool, page_id: str, page_seconds_since_previous_revision: str):
        if uid not in USER_MONTH_EVENTS:
            USER_MONTH_EVENTS[uid] = new_month_obj()
            user_month = USER_MONTH_EVENTS[uid]
            user_month['first_event'] = timestamp
            user_month['activity_days'] = 1
        else:
            user_month = USER_MONTH_EVENTS[uid]

        if 'namespaces' not in user_month:
            user_month['namespaces'] = {}

        user_month_namespaces = user_month['namespaces']

        if namespace not in user_month_namespaces:
            user_month_namespaces[namespace] = {}

        if uid not in USER_HELPER_INFO:
            USER_HELPER_INFO[uid] = {}
        user_helper_info = USER_HELPER_INFO[uid]

        user_month_namespace = user_month_namespaces[namespace]
        if event_type not in user_month_namespace:
            user_month_namespace[event_type] = 1
        else:
            user_month_namespace[event_type] += 1
        if minor_edit:
            if 'minor_edits' not in user_month_namespace:
                user_month_namespace['minor_edits'] = 1
            else:
                user_month_namespace['minor_edits'] += 1

        if 'last_event' in user_month:
            last_timestamp = user_month['last_event']
            day_diff = timestamp.day - last_timestamp.day
            if day_diff == 0:
                user_month['secs_since_same_day_event'] += (timestamp - last_timestamp).total_seconds()
                user_month['secs_since_same_day_event_count'] += 1
            else:
                user_month['activity_days'] += 1
                if day_diff > user_month['max_inact_interval']:
                    user_month['max_inact_interval'] = day_diff

        else:
            user_month['secs_since_same_day_event'] = 0
            user_month['secs_since_same_day_event_count'] = 0

        if page_id != '':
            if 'pages' not in user_helper_info:
                user_helper_info['pages'] = Counter()
            user_helper_info['pages'].update([page_id])

        if page_seconds_since_previous_revision is not None:
            if 'pages_seconds' not in user_month:
                user_month['pages_seconds'] = page_seconds_since_previous_revision
                user_month['pages_seconds_count'] = 1
            else:
                user_month['pages_seconds'] += page_seconds_since_previous_revision
                user_month['pages_seconds_count'] += 1

        user_month['last_event'] = timestamp

    def analyze_page_or_revision(event_type: str, timestamp, parts: list[str]):
        uid = dump.parse_int(parts[KEYS['event_user_id']])

        if uid != '' and uid is not None:
            if uid not in USER_EXISTS:
                username = parts[KEYS['event_user_text']]
                creation_timestamp = dump.parse_date(parts[KEYS['event_user_creation_timestamp']])
                registration_timestamp = dump.parse_date(parts[KEYS['event_user_registration_timestamp']])
                is_bot = parts[KEYS['event_user_is_bot_by']] != ''
                groups = dump.parse_str_array(parts[KEYS['event_user_groups']])
                blocks = dump.parse_str_array(parts[KEYS['event_user_blocks']])
                USER_DOCUMENT[uid] = new_user_obj(uid, username, creation_timestamp, registration_timestamp, is_bot, groups, blocks)
                USER_EXISTS.add(uid)

            minor_edit = parts[KEYS['revision_minor_edit']] == 'true'
            page_id = parts[KEYS['page_id']]
            page_seconds_since_previous_revision = dump.parse_int(parts[KEYS['page_seconds_since_previous_revision']])
            namespace = dump.parse_int(parts[KEYS['page_namespace']])
            namespace = f'n{namespace}' if namespace is not None else 'unknown'
            update_month_object(uid, namespace, EVENT[event_type], timestamp, minor_edit, page_id, page_seconds_since_previous_revision)

    def analyze_user_create(parts: list[str], timestamp):
        uid = dump.parse_int(parts[KEYS['user_id']])

        if uid != '' and uid is not None:
            username = parts[KEYS['user_text']]
            creation_timestamp = dump.parse_date(parts[KEYS['user_creation_timestamp']])
            registration_timestamp = dump.parse_date(parts[KEYS['user_registration_timestamp']])
            is_bot = parts[KEYS['user_is_bot_by']] != ''
            groups = dump.parse_str_array(parts[KEYS['user_groups']])
            blocks = dump.parse_str_array(parts[KEYS['user_blocks']])
            USER_DOCUMENT[uid] = new_user_obj(uid, username, creation_timestamp, registration_timestamp, is_bot, groups, blocks)
            USER_DOCUMENT_UPDATE[uid] = new_user_update_obj(uid, username, creation_timestamp, registration_timestamp, is_bot, groups, blocks)
            
            current_groups = dump.parse_str_array(parts[KEYS['event_user_groups_historical']])
            if uid not in USER_ALTERS_GROUPS:
                USER_ALTERS_GROUPS[uid] = [{'t': timestamp, 'g': current_groups}]
            else:
                USER_ALTERS_GROUPS[uid].append({'t': timestamp, 'g': current_groups})

            current_blocks = dump.parse_str_array(parts[KEYS['event_user_blocks_historical']])
            if uid not in USER_ALTERS_BLOCKS:
                USER_ALTERS_BLOCKS[uid] = [{'t': timestamp, 'g': current_blocks}]
            else:
                USER_ALTERS_BLOCKS[uid].append({'t': timestamp, 'g': current_blocks})

    def analyze_user_altergroups(parts: list[str], timestamp):
        uid = dump.parse_int(parts[KEYS['user_id']])
        if uid != '' and uid is not None:
            if uid not in USER_EXISTS:
                username = parts[KEYS['user_text']]
                creation_timestamp = dump.parse_date(parts[KEYS['user_creation_timestamp']])
                registration_timestamp = dump.parse_date(parts[KEYS['user_registration_timestamp']])
                is_bot = parts[KEYS['user_is_bot_by']] != ''
                groups = dump.parse_str_array(parts[KEYS['user_groups']])
                blocks = dump.parse_str_array(parts[KEYS['user_blocks']])
                USER_DOCUMENT[uid] = new_user_obj(uid, username, creation_timestamp, registration_timestamp, is_bot, groups, blocks)
                USER_EXISTS.add(uid)

            current_groups = dump.parse_str_array(parts[KEYS['event_user_groups_historical']])
            if uid not in USER_ALTERS_GROUPS:
                USER_ALTERS_GROUPS[uid] = [{'t': timestamp, 'g': current_groups}]
            else:
                USER_ALTERS_GROUPS[uid].append({'t': timestamp, 'g': current_groups})

            current_blocks = dump.parse_str_array(parts[KEYS['event_user_blocks_historical']])
            if uid not in USER_ALTERS_BLOCKS:
                USER_ALTERS_BLOCKS[uid] = [{'t': timestamp, 'g': current_blocks}]
            else:
                USER_ALTERS_BLOCKS[uid].append({'t': timestamp, 'g': current_blocks})

    def analyze_file(file_path: str) -> None:
        log('Start filling users')
        timestamp = None
        with bz2.open(file_path, 'rt') as input:
            for line in input:
                parts = line.split('\t')

                timestamp = dump.parse_date(parts[KEYS['event_timestamp']])

                check_if_new_month(timestamp, True)

                event_entity = parts[KEYS['event_entity']]
                event_type = parts[KEYS['event_type']]

                if event_entity == 'revision':
                    analyze_page_or_revision('edit', timestamp, parts)
                elif event_entity == 'page':
                    analyze_page_or_revision(event_type, timestamp, parts)
                elif event_entity == 'user' and event_type == 'create':
                    analyze_user_create(parts, timestamp)
                elif event_entity == 'user' and event_type == 'altergroups':
                    analyze_user_altergroups(parts, timestamp)

        check_if_new_month(timestamp, False)
        log('End fill users')

    def check_if_new_month(timestamp, check: bool):
        nonlocal CURRENT_MONTH
        month = two_digits(timestamp.month)
        if (not check or month != CURRENT_MONTH):
            year = str(timestamp.year)
            log(f'New month {month}')
            update_db(year, CURRENT_MONTH)
            CURRENT_MONTH = month

    def update_db(year: str, month: str):
        log(f'Uploading {year}/{month}')

        user_updates = []
        inserts = []
        updates = []
        alters = []

        def calc_entropy(counter: Counter, count: int) -> float:
            values = counter.values()
            sum_of_numbers = sum(value for value in values)
            probs = [value / sum_of_numbers for value in values]
            return -sum([prob * math.log(prob) for prob in probs])

        def parse_pages_counter(pages_counter: Counter) -> dict:
            n = len(pages_counter)
            most_common_5 = dict(pages_counter.most_common(5))
            entropy = calc_entropy(pages_counter, n)
            return {'n': n, 'most_common_5': most_common_5, 'entropy': entropy}

        def add_update(uid: str, obj: dict) -> None:
            updates.append(UpdateOne({'id': uid}, {'$set': {f'events.per_month.{year}.{month}': obj}}))

        def add_user_update(uid: str, obj: dict) -> None:
            updates.append(UpdateOne({'id': uid}, {'$set': obj}))

        def add_update_alter(uid: str, obj: list, key: str) -> None:
            alters.append(UpdateOne({'id': uid}, {'$push': {f'alter.{key}': {'$each': obj}}}))

        for uid in list(USER_DOCUMENT.keys()):
            inserts.append(USER_DOCUMENT[uid])
            USER_DOCUMENT.pop(uid)

        for uid in list(USER_DOCUMENT_UPDATE.keys()):
            if uid in USER_DOCUMENT_UPDATE:
                add_user_update(uid, USER_DOCUMENT_UPDATE[uid])
                USER_DOCUMENT_UPDATE.pop(uid)

        for uid in list(USER_MONTH_EVENTS.keys()):
            month_obj=USER_MONTH_EVENTS[uid]
            if uid in USER_HELPER_INFO:
                if 'pages' in USER_HELPER_INFO[uid]:
                    month_obj['pages']=parse_pages_counter(USER_HELPER_INFO[uid]['pages'])
                    USER_HELPER_INFO[uid].pop('pages')
            add_update(uid, month_obj)
            USER_MONTH_EVENTS.pop(uid)

        for uid in list(USER_ALTERS_GROUPS.keys()):
            if uid in USER_ALTERS_GROUPS:
                add_update_alter(uid, USER_ALTERS_GROUPS[uid], 'groups')
                USER_ALTERS_GROUPS.pop(uid)

        for uid in list(USER_ALTERS_BLOCKS.keys()):
            if uid in USER_ALTERS_BLOCKS:
                add_update_alter(uid, USER_ALTERS_BLOCKS[uid], 'blocks')
                USER_ALTERS_BLOCKS.pop(uid)

        log('Gotten inserts and updates')
        if (inserts):
            try:
                users_collection.insert_many(inserts, ordered=False)
            except:
                pass
        log('Added inserts')
        if (updates):
            users_collection.bulk_write(updates)
        log('Added updates')
        if (user_updates):
            users_collection.bulk_write(user_updates)
        log('Added user updates')
        if (alters):
            users_collection.bulk_write(alters)
        log('Added updates alters')
        log('Finished update')

    analyze_file(path)


def get_tsv_year_and_month(tsv_file_name: str):
    try:
        year=int(tsv_file_name.split('.')[::-1][1])
    except:
        year=None

    try:
        month=int(tsv_file_name.split('.')[::-1][1].split('-')[1])
    except:
        month=None

    return year, month


def start_process_file(path):
    year, month=get_tsv_year_and_month(path.stem)
    solve_file(str(path), year, month)


num_cores=multiprocessing.cpu_count()
Parallel(n_jobs=num_cores)(delayed(start_process_file)(path) for path in scraper.get_tsv_files(lang))
