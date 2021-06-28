from sys import argv
from pymongo import MongoClient, UpdateOne

from utils.logger import log

def parse_user(user):
    events_per_month = user['events']['per_month']
    events_per_year = {}
    years = sorted(events_per_month.keys())
    n_years = len(years)
    for iy, year in enumerate(years):
        events_per_year[year] = {'activity_days': 0}
        events_per_year_value = events_per_year[year]
        
        year_secs_since_same_day_event_tot = 0
        year_secs_since_same_day_event_count = 0

        year_max_inact_interval_tot = 0
        year_max_inact_interval_count = 0
        year_max_inact_interval_max = float('inf')
        year_max_inact_interval_min = -1

        year_diff_pages_n_tot = 0
        year_diff_pages_count = 0
        year_diff_pages_entropy_tot = 0
        year_diff_pages_entropy_count = 0

        year_value = events_per_month[year]

        months = sorted(year_value.keys())
        n_months = len(months)

        for im, month in enumerate(months):
            month_value = year_value[month]

            if im == 0:
                events_per_year_value['first_event'] = month_value['first_event']
            if im == n_months - 1:
                events_per_year_value['last_event'] = month_value['last_event']

            events_per_year_value['activity_days'] += month_value['activity_days']
            year_secs_since_same_day_event_tot += month_value['secs_since_same_day_event']
            year_secs_since_same_day_event_count += month_value['secs_since_same_day_event_count']

            max_inact_interval = month_value['max_inact_interval']
            year_max_inact_interval_tot += max_inact_interval
            year_max_inact_interval_count += 1
            if max_inact_interval > year_max_inact_interval_max:
                year_max_inact_interval_max = max_inact_interval
            if max_inact_interval > year_max_inact_interval_min:
                year_max_inact_interval_min = max_inact_interval

            year_diff_pages_count += 1
            year_diff_pages_n_tot += month_value['pages']['n']
            year_diff_pages_n_tot += month_value['pages']['n']

        events_per_year_value['secs_since_same_day_event_avg'] = year_secs_since_same_day_event_tot / year_secs_since_same_day_event_count
        events_per_year_value['max_inact_interval_avg'] = year_max_inact_interval_tot / year_max_inact_interval_count
        events_per_year_value['max_inact_interval_min'] = year_max_inact_interval_min
        events_per_year_value['max_inact_interval_max'] = year_max_inact_interval_max

        events_per_year_value['diff_pages'] = {
            'n_avg': year_diff_pages_n_tot / year_diff_pages_count
        }



def get_file_path(lang: str) -> str:
    file_path = f'wiki_sex/{lang}.tsv'
    return file_path

def get_users_collection(lang: str):
    client = MongoClient()
    users_collection = client.get_database('user_metrics').get_collection(f'{lang}wiki_users')
    return users_collection

def upload_sex(lang: str, file_path: str) -> None:
    log('Start updating users')
    with open(file_path, 'r') as input:
        bulk_updates = []
        for line in input:
            parts = line.split('\t')
            id = parts[0]
            sex = True if parts[2] == 'male' else (False if parts[2] == 'female' else None)
            bulk_updates.append(UpdateOne({'id': int(id)}, {'$set': {'sex': sex}}))
        users_collection = get_users_collection(lang)
        users_collection.bulk_write(bulk_updates)
    log('Finished updating users')

lang = argv[1]
path = get_file_path(lang)
upload_sex(lang, path)
