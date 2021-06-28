from pymongo import MongoClient
from json import dumps
from sys import argv

lang = argv[1]


client = MongoClient()
users_collection = client.get_database('user_metrics').get_collection(f'{lang}wiki_users_aggregated')

users_no_dead = users_collection.find({ 'events.per_month': { "$ne": {} }})

result = {
    'tot': 0,
    'per_anno': {
        
    },
    'per_mese': {

    }
}


for user in users_no_dead:
    events = user['events']['per_month']
    is_tot = False

    for year, events_year in events.items():
        is_year = False

        for month, events_month in events_year.items():
            try:
                tot_events = events_month['total']['tot']
            except:
                tot_events = 0

            if tot_events >= THRESHOLD:
                is_tot = True
                is_year = True
                month_obj = result['per_mese']
                month_key = f'{year}_{month}'
                try:
                    month_obj[month_key] += 1
                except:
                    month_obj[month_key] = 1

        if is_year:
            year_obj = result['per_anno']
            year_key = f'{year}'
            try:
                year_obj[year_key] += 1
            except:
                year_obj[year_key] = 1

    if is_tot:    
        result['tot'] += 1

with open(f'community_{lang}.json', 'w') as file:
    file.write(dumps(result))     

