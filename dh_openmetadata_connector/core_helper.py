import requests

class CoreHelper:
    def getDataItems(apiUrl: str, token:str = None):
        if not apiUrl.endswith('/'):
            apiUrl += '/'
        apiUrl += 'dataitems'
        page = 0
        headers = {'Content-Type': 'application/json'}
        if token:
            headers['Authorization'] = 'Bearer ' + token
        last = False

        while not last:
            params = {'versions':'latest', 'page':page, 'size':1000}
            result = requests.request('GET', apiUrl, params=params, headers=headers)
            print(result.status_code)
            result.raise_for_status()
            if result.status_code == 200:
                json_result = result.json()
                content_list = json_result['content']
                for itemNode in content_list:
                    yield itemNode
                last = json_result['last']
                page += 1
            else:
                last = True

