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
result_maschio = {
    'tot': 0,
    'per_anno': {
        
    },
    'per_mese': {

    }
}
result_femmina = {
    'tot': 0,
    'per_anno': {
        
    },
    'per_mese': {

    }
}
result_mah = {
    'tot': 0,
    'per_anno': {
        
    },
    'per_mese': {

    }
}

THRESHOLD = 5

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
                month_key = f'{year}_{month}'
                try:
                    result['per_mese'][month_key] += 1
                except:
                    result['per_mese'][month_key] = 1
                try:
                    if user['sex'] == True:
                        try:
                            result_maschio['per_mese'][month_key] += 1
                        except:
                            result_maschio['per_mese'][month_key] = 1
                    elif user['sex'] == False:
                        try:
                            result_femmina['per_mese'][month_key] += 1
                        except:
                            result_femmina['per_mese'][month_key] = 1
                    elif user['sex'] is None:
                        try:
                            result_mah['per_mese'][month_key] += 1
                        except:
                            result_mah['per_mese'][month_key] = 1
                except:
                    pass

        if is_year:
            year_key = f'{year}'
            try:
                result['per_anno'][year_key] += 1
            except:
                result['per_anno'][year_key] = 1
            try:
                if user['sex'] is None:
                    try:
                        result_mah['per_anno'][year_key] += 1
                    except:
                        result_mah['per_anno'][year_key] = 1
                elif user['sex'] == True:
                    try:
                        result_maschio['per_anno'][year_key] += 1
                    except:
                        result_maschio['per_anno'][year_key] = 1
                elif user['sex'] == False:
                    try:
                        result_femmina['per_anno'][year_key] += 1
                    except:
                        result_femmina['per_anno'][year_key] = 1
            except:
                pass

    if is_tot:    
        result['tot'] += 1
        try:
            if user['sex'] == True:
                result_maschio['tot'] += 1
            elif user['sex'] == False:
                result_femmina['tot'] += 1
            elif user['sex'] is None:
                result_mah['tot'] += 1
        except:
            pass
        
        

with open(f'community_{lang}.json', 'w') as file:
    file.write(dumps(result))     

with open(f'community_{lang}_maschio.json', 'w') as file:
    file.write(dumps(result_maschio))   

with open(f'community_{lang}_femmina.json', 'w') as file:
    file.write(dumps(result_femmina))  

with open(f'community_{lang}_boh.json', 'w') as file:
    file.write(dumps(result_mah))    

