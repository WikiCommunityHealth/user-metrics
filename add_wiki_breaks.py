from sys import argv
import bz2
from json import loads
from pymongo import MongoClient, UpdateOne

from utils.logger import log


def get_file_path(lang: str) -> str:
    file_path = f'wiki_breaks/{lang}wiki.json.bz2'
    return file_path


def get_users_collection(lang: str):
    client = MongoClient()
    users_collection = client.get_database('user_metrics').get_collection(f'{lang}wiki_users')
    return users_collection


def upload_sex(lang: str, file_path: str) -> None:
    log('Start updating users')
    with bz2.open(file_path, 'rt') as input:
        bulk_updates = []
        users_collection = get_users_collection(lang)
        for line in input:
            obj = loads(line.rstrip('\n'))
            username = obj['name']
            wikibreaks = obj['wikibreaks']
            bulk_updates.append(UpdateOne({'username': username}, {'$set': {'wikibreaks': wikibreaks}}))
            if len(bulk_updates) > int(1.5e6):
                print('uploading')
                users_collection.bulk_write(bulk_updates)
                bulk_updates = []
        users_collection.bulk_write(bulk_updates)
    log('Finished updating users')


lang = argv[1]
path = get_file_path(lang)
upload_sex(lang, path)
