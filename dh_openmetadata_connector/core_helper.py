import requests

class CoreHelper:
    def getDataItems(apiUrl):
        page = 0
        headers = {'content_type': 'application/json'}
        last = False

        while not last:
            params = {'page': page, 'size': 1000}
            result = requests.request('GET', apiUrl, params=params, headers=headers)
            #print(result.status_code)
            json_result = result.json()
            content_list = json_result['content']
            for itemNode in content_list:
                yield itemNode
            last = json_result['last']
            page += 1
