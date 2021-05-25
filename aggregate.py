from pymongo import MongoClient

client = MongoClient()
users_collection = client.get_database('user_metrics').get_collection(f'cawiki_users')
users_collection_aggregated = client.get_database('user_metrics').get_collection(f'cawiki_users_aggregated')

users_no_dead = list(users_collection.find({'events.per_month': {"$ne": {}}}))

insert_queries = []

for user in users_no_dead:
    events = user['events']['per_month']
    tot_events = {}

    for year, events_year in events.items():
        tot_year_events = {}

        for month, events_month in events_year.items():
            tot_month_events = {}

            events_month_namespaces = events_month['namespaces']

            for namespace, namespace_events in events_month_namespaces.items():
                for event_type, events_count in namespace_events.items():
                    try:
                        tot_month_events[event_type] += events_count
                    except:
                        tot_month_events[event_type] = events_count

            events_month['total'] = tot_month_events
            events_month['total']['tot'] = sum([v for k, v in tot_month_events.items() if k != 'minor_edits'])
            tot_year_events = {k:  events_month['total'].get(k, 0) + tot_year_events.get(k, 0) for k in set( events_month['total']) | set(tot_year_events)}

        tot_events = {k: tot_events.get(k, 0) + tot_year_events.get(k, 0) for k in set(tot_events) | set(tot_year_events)}
        try:
            user['events']['per_year'][f'{year}'] = tot_year_events
        except Exception:
            user['events']['per_year'] = {f'{year}': tot_year_events}

    user['events']['total'] = tot_events

    insert_queries.append(user)
    if (len(insert_queries) > int(1e6)):
        users_collection_aggregated.insert_many(insert_queries)
        insert_queries = []

users_collection_aggregated.insert_many(insert_queries)
insert_queries = []
