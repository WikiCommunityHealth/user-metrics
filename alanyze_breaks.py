from pymongo import MongoClient
from json import dumps
from sys import argv
from datetime import datetime
import calendar

from typing import Optional

lang = argv[1]

def two_digits(month: int) -> str:
    return str(month) if month >= 10 else f'0{month}'

def add_months(sourcedate: datetime, months = 1):
    month = sourcedate.month - 1 + months
    year = sourcedate.year + month // 12
    month = month % 12 + 1
    day = min(sourcedate.day, calendar.monthrange(year, month)[1])
    return datetime(year, month, day)


def months_range(von: str, bis: Optional[str]):
    from_date = datetime.fromisoformat(von)
    to_date = datetime.fromisoformat(bis) if bis else datetime(2021, 4, 1)
    result = []
    from_date = add_months(from_date, 1)
    while from_date.year < to_date.year or from_date.month < to_date.month:
        result.append((str(from_date.year), two_digits(from_date.month)))
        from_date = add_months(from_date, 1)
    return result

client = MongoClient()
users_collection = client.get_database('user_metrics').get_collection(f'{lang}wiki_users_aggregated')

users_with_breaks = users_collection.find({'wikibreaks': {"$ne": None}})

result = []
for user in users_with_breaks:
    breaks = user['wikibreaks']
    breaks_analisi = []
    for wikibreak in breaks:
        questa_analisi = { 'conflicts': [] }
        von = wikibreak['from_date']
        bis = wikibreak['to_date']

        pause_months = months_range(von, bis)

        questa_analisi['from'] = von
        questa_analisi['to'] = bis

        for year, month in pause_months:
            try:
                n_events = user['events']['per_month'][year][month]['total']['tot']
                questa_analisi['conflicts'].append({ 'year': year, 'month': month, 'n': n_events})
            except Exception as err:
                pass

        breaks_analisi.append(questa_analisi)

    result.append({
        'username': user['username'],
        'id': user['id'],
        'analisi': breaks_analisi
    })



analisi_collection = client.get_database('user_metrics').get_collection(f'{lang}breaks_analisi')
analisi_collection.delete_many({})
analisi_collection.insert_many(result)