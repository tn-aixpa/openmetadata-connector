import json
from metadata.generated.schema.entity.data.table import (DataType, Constraint)

class TableColumn:
    def __init__(self, name, type):
        self.name:str = name
        self.type: DataType = type
        self.constraint: Constraint = None
        self.preview: list[str] = []


class DataItemParser:

    def getDataType(type):
        try:
            return DataType(type.upper())
        except Exception:
            return DataType.UNKNOWN

    def fillColumns(self, item):
        previewMap = {}
        if item['status'].get('preview') and item['status']['preview']['cols']:
            for col in item['status']['preview']['cols']:
                previewMap[col['name']] = col

        if item['spec'].get('schema') and item['spec']['schema'].get('fields'):
            for col in item['spec']['schema']['fields']:
                name = col['name']
                type = DataItemParser.getDataType(col['type'])
                column = TableColumn(name, type)
                if previewMap.get(name):
                    for value in previewMap[name]['value']:
                        column.preview.append(json.dumps(value))
                self.columns.append(column)


class PostgresParser(DataItemParser):
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
        strings = self.path.replace('sql://', '').split('/')
        if len(strings) == 3:
            self.dbName = strings[0]
            self.dbSchema = strings[1]
            self.dbTable = self.name
        else:
            self.dbName = strings[0]
            self.dbSchema = "public"
            self.dbTable = self.name

        self.columns: list[TableColumn] = []
        self.fillColumns(item)            


class S3Parser(DataItemParser):
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
        strings = self.path.replace('s3://', '').split('/')
        self.dbName = strings[0]
        self.dbTable = strings[len(strings)-1]
        self.dbSchema = ""
        for s in strings[1:len(strings)-1]:
            self.dbSchema += s + "_"
        self.dbSchema = self.dbSchema[:-1]

        self.columns: list[TableColumn] = []        
        self.fillColumns(item)            



