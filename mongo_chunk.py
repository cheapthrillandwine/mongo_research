#!/usr/bin/python

import argparse
from pymongo import MongoClient as Client
from bson import BSON
from bson import json_util
import json
import os

# mongo client
client =Client("mongodb://cityvision:J5LpmY1wmSz4EGEz8Qrjv9rPKJTcWSQjcyqr7S4W63cUzl3LZzghuzN4QMdvC00osNyP8eXMxSZQHt5bdtfv4w==@cityvision.documents.azure.com:10255/?ssl=true&replicaSet=globaldb")

# script arguments
parser = argparse.ArgumentParser()
parser.add_argument('--db', metavar='d', type=str, help='specify a mongo db', dest='db',default='cityvision')
parser.add_argument('--collection', metavar='c', type=str, help='specify a mongo collection', dest='collection',default='result')
parser.add_argument('--offset', metavar='O', type=int, help='specify an offset ', dest='offset',default=10000)
parser.add_argument('--prefix', metavar='o', type=str, help='specify an output prefix', dest='prefix')
parser.add_argument('--dir', metavar='D', type=str, help='specify an existing directory', dest='directory', default=r'C:\Users\NSW00_906882\Desktop\mongo\json')
args = parser.parse_args()

# variables
db = client[args.db]
collection = db[args.collection]
offset = args.offset
prefix = args.prefix

print('Selected database: {}'.format(db))
print('Selected collection: {}'.format(collection))
print('Offset: {}'.format(offset))

last_offset = 0


def json_chunks(offset):
    offset = offset
    total_docs = collection.find().count()
    print('total_docs: {}'.format(total_docs))
    if total_docs < offset:

        print('! Error: offset is greater than number of total documents')
        exit()


    chunks = total_docs / offset
    chunk_idx = 0

    for i in range(0, total_docs + offset, offset):
        if i >= total_docs:

            print('export successfully finished')
            exit()
        dump = None
        docs = None
        json_file_path = os.path.join('{}'.format(args.directory), '{}_chunk__{}.json'.format(args.prefix, chunk_idx))
        # print(json_file_path)

        print('i: {}'.format(i))
        print('offset: {}'.format(i + offset))

        print('query: collection.find().skip({}).limit({})'.format(i, offset))

        if i + offset > total_docs:
            offset_diff = offset - total_docs
            offset = offset - offset_diffc

        docs = collection.find().skip(i).limit(offset)
        # print(docs)
        with open(json_file_path, 'w') as outfile:
            dump = json.dumps([doc for doc in docs], sort_keys=False, indent=4, default=json_util.default)
            print(dump)
            outfile.write(dump)
        chunk_idx += 1
    print('export successfully finished')

json_chunks(offset)
