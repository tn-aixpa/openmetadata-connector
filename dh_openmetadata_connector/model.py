import json


class ModelParam:
    def __init__(self, n, v):
        self.name = n
        self.value = v


class ModelParser:

    def __init__(self, item):
        
        if item['metadata'].get('project'):
            self.project = item['metadata']['project'] 
        else:
            self.project = item['project']
        
        if item['metadata'].get('name'):
            self.name = item['metadata']['name'] 
        else:
            self.name = item['name']
        
        if item['metadata'].get('version'):
            self.version = item['metadata']['version'] 
        else:
            self.version = item['version']
        
        self.source = item['key']
        self.kind = item['kind'] 
        self.key = item['kind'] + "_" + item['project'] + "_" + item['name']
        self.path = item['spec']['path'] 
        
        if item['spec'].get('algorithm'):
            self.algorithm = item['spec']['algorithm']
        else:
            if item['spec'].get('framework'):
                self.algorithm = item['spec']['framework']
            else:
                self.algorithm = item['kind']
        
        if item['spec'].get('framework'):
            self.framework = item['spec']['framework']
        
        self.parameters : list[ModelParam] = []
        if item['spec'].get('parameters'):
            for n in item['spec']['parameters'].keys():
                v = item['spec']['parameters'][n]
                self.parameters.append(ModelParam(n, v))

class MlflowModelParser(ModelParser):
    def __init__(self, item):
        super().__init__(item)


class SKLearnModelParser(ModelParser):
    def __init__(self, item):
        super().__init__(item)


class HuggingFaceModelParser(ModelParser):
    def __init__(self, item):
        super().__init__(item)
