from urllib.request import urlopen, Request
from urllib.parse import urlencode
from urllib.error import HTTPError
import json

class Oauth2Handshake:

    def __init__(self, client_id, client_secret):
        self.client_id = client_id
        self.client_secret = client_secret
        self.device_code = None

    def stage1(self):
        endpoint = 'https://accounts.google.com/o/oauth2/device/code'
        scope = 'https://www.googleapis.com/auth/youtube'
        req = Request(endpoint, urlencode({
            'client_id': self.client_id,
            'scope': scope
        }).encode('utf8'))
        with urlopen(req) as res:
            data = json.load(res)
        self.device_code = data['device_code']
        return data['user_code'], data['verification_url']

    def stage2(self):
        endpoint = 'https://accounts.google.com/o/oauth2/token'
        grant = 'http://oauth.net/grant_type/device/1.0'
        req = Request(endpoint, urlencode({
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            'code': self.device_code,
            'grant_type': grant
        }).encode('utf8'))
        with urlopen(req) as res:
            data = json.load(res)
        return data['access_token'], data['refresh_token']

    def refresh(self, refresh_token):
        endpoint = 'https://accounts.google.com/o/oauth2/token'
        grant = 'refresh_token'
        req = Request(endpoint, urlencode({
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            'refresh_token': refresh_token,
            'grant_type': grant
        }).encode('utf8'))
        with urlopen(req) as res:
            data = json.load(res)
        return data['access_token']

class OAuth2AuthMode:

    def __init__(self, access_token, refresh_token, handshake):
        self.access_token = access_token
        self.refresh_token = refresh_token
        self.handshake = handshake

    def modify_request(self, params, headers):
        headers['Authorization'] = 'Bearer ' + self.access_token

class APIKeyAuthMode:

    def __init__(self, key):
        self.key = key

    def modify_request(self, params, headers):
        params['key'] = self.key

class YoutubeAPI:

    def __init__(self, auth_mode):
        self.auth_mode = auth_mode
        self.base_url = 'https://www.googleapis.com/youtube/v3'

    def make_request(self, endpoint, params={}, data=None, headers={}):
        if not endpoint.startswith('/'):
            endpoint = '/' + endpoint
        query = ''
        headers = dict(headers)
        params = dict(params)
        params['prettyPrint'] = False
        self.auth_mode.modify_request(params, headers)
        if params:
            query = '?' + urlencode(params)
        req = Request(self.base_url + endpoint + query, data, headers)
        try:
            with urlopen(req) as res:
                data = json.load(res)
        except HTTPError as e:
            print(json.load(e))
            raise e
        return data

    def make_paginated_request(self, endpoint, params={}, content_key='items'):
        first_page = self.make_request(endpoint, params)
        yield from first_page[content_key]
        data = first_page
        while 'nextPageToken' in data:
            new_params = {
                'pageToken': data['nextPageToken']
            }
            new_params.update(params)
            data = self.make_request(endpoint, new_params)
            yield from data[content_key]

    def get_videos(self, id_list, part='snippet', max_results=50):
        return self.make_paginated_request('/videos', {
            'part': part,
            'id': ','.join(id_list),
            'maxResults': max_results
        })

    def get_channel_datas(self, channel_ids, part='snippet', max_results=50):
        return self.make_paginated_request('/channels', {
            'part': part,
            'id': ','.join(channel_ids),
            'maxResults': max_results
        })

    def get_channel_for_username(self, username, part='snippet'):
        return self.make_paginated_request('/channels', {
            'part': part,
            'forUsername': username
        })

    def get_playlist_items(self, playlist_id, part='snippet', max_results=50):
        return self.make_paginated_request('/playlistItems', {
            'part': part,
            'playlistId': playlist_id,
            'maxResults': max_results
        })

    def compose_data(self, id_list, func, *parts):
        results = {id:{'id': id} for id in id_list}
        generators = {part: func(id_list, part=part) for part in parts}
        while generators:
            for part, gen in list(generators.items()):
                try:
                    result = next(gen)
                except StopIteration:
                    del generators[part]
                    continue
                rdict = results[result['id']]
                rdict[part] = result[part]
                # Got all the parts, can return now
                # +1 because 'id' in rdict
                if len(rdict) == len(parts) + 1:
                    yield rdict
