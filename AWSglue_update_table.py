import boto3
import pprint
import os
from datetime import datetime

database = '[your AWS glue database]'
tablename = '[your AWS glue database table]'
headfile = '[name of headers file (seperated by whitespace)]'

client = boto3.client('glue')
response = client.get_table(DatabaseName=database, Name=tablename)


def setter(dict_name, t='', *args):
    temp_arg = dict_name
    for arg in args:
        if arg in temp_arg:
            temp_arg = temp_arg[arg]
        else:
            return t
    return temp_arg


TableInput = {
    'Name': setter(response, '', 'Table', 'Name'),
    'Description': setter(response, '', 'Table', 'Description'),
    'Owner': setter(response, '', 'Table', 'Owner'),
    'LastAccessTime': setter(response, datetime.now(), 'Table', 'LastAccessTime'),
    'LastAnalyzedTime': setter(response, datetime.now(), 'Table', 'LastAnalyzedTime'),
    'Retention': setter(response, 0, 'Table', 'Retention'),
    'StorageDescriptor': {'Columns': setter(response, [], 'Table', 'StorageDescriptor', 'Columns'),
                          'Location': setter(response, '', 'Table', 'StorageDescriptor', 'Location'),
                          'InputFormat': setter(response, '', 'Table', 'StorageDescriptor', 'InputFormat'),
                          'OutputFormat': setter(response, '', 'Table', 'StorageDescriptor', 'OutputFormat'),
                          'Compressed': setter(response, '', 'Table', 'StorageDescriptor', 'Compressed'),
                          'NumberOfBuckets': setter(response, 0, 'Table', 'StorageDescriptor', 'NumberOfBuckets'),
                          'SerdeInfo': setter(response, {}, 'Table', 'StorageDescriptor', 'SerdeInfo'),
                          'SortColumns': setter(response, [], 'Table', 'StorageDescriptor', 'SortColumns')},
    'PartitionKeys': setter(response, [], 'Table', 'PartitionKeys'),
    'ViewOriginalText': setter(response, '', 'Table', 'ViewOriginalText'),
    'ViewExpandedText': setter(response, '', 'Table', 'ViewExpandedText')c ,
    'TableType': setter(response, '', 'Table', 'TableType'),
    'Parameters': setter(response, {}, 'Table', 'Parameters'),
}

columns = len(TableInput['StorageDescriptor']['Columns'])

with open(headfile, 'r') as fd:                                                                                                                                       
    x = fd.read()
headers = x.split()

for i, header in zip(range(columns), headers):
    TableInput['StorageDescriptor']['Columns'][i]['Name'] = header

# only handles a single partition key (automatically)
TableInput['PartitionKeys'][0]['Name'] = 'dt'
TableInput['PartitionKeys'][0]['Type'] = 'date'


client = boto3.client('glue')
response = client.update_table(DatabaseName=database, TableInput=TableInput)
response = client.get_table(DatabaseName=database, Name=tablename)
