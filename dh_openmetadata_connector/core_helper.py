import requests

class CoreHelper:
    def getDataItems(apiUrl):
        page = 0
        headers = {'content_type': 'application/json'}
        last = False

        while not last:
            params = {'versions':'latest', 'page':page, 'size':1000}
            result = requests.request('GET', apiUrl, params=params, headers=headers)
            #print(result.status_code)
            json_result = result.json()
            content_list = json_result['content']
            for itemNode in content_list:
                yield itemNode
            last = json_result['last']
            page += 1

    def checkApi(apiUrl) -> int:
        headers = {'content_type': 'application/json'}
        params = {'versions':'latest', 'page':0, 'size':1}
        result = requests.request('GET', apiUrl, params=params, headers=headers)
        return result.status_code


