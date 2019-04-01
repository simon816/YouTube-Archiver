import json

def get_ia_ids():

    with open('ia.json', 'r') as f:
        data = json.load(f)

    ids = map(lambda d: d['identifier'], data['response']['docs'])

    ids = filter(lambda id: id.startswith('youtube-'), ids)

    l = len('youtube-')

    ids = map(lambda id: id[l:], ids)

    return set(ids)

#print('hNbQcWzMh9c' in get_ia_ids())
