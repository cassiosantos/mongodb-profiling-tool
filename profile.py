from time import sleep
import time
from datetime import datetime

from pymongo import MongoClient, ASCENDING
from pymongo.cursor import CursorType
from pymongo.errors import AutoReconnect

import getpass
import argparse

import os.path

# Time to wait for data or connection.
_SLEEP = 1
_HOUR_MASK = "%Y-%m-%dT%H"
_MINUTE_MASK = "%Y-%m-%dT%H:%M"

class Password:

    DEFAULT = 'Prompt if not specified'

    def __init__(self, value):
        if value == self.DEFAULT:
            value = getpass.getpass('MongoDB Password: ')
        self.value = value

    def __str__(self):
        return self.value


def process_cursor(cursor, resultmap, currentdatekey):
    for doc in cursor:
        stamp = doc['ts']

        stampconverted = datetime.fromtimestamp(stamp.time, None)
        date = stampconverted.strftime(consolidationmask)

        if currentdatekey is None:
            currentdatekey = date
        else:
            if currentdatekey != date:
                flush_result_to_file(resultmap)
                currentdatekey = date

        process_doc(doc, resultmap)


def process_doc(doc, resultmap):
    # "key = database, collection, operation, time"
    print("processing doc: " + str(doc))

    namespacesplit = doc['ns'].split(".")
    db = namespacesplit[0]
    collection = namespacesplit[1]
    operation = doc['op']

    stampconverted = datetime.fromtimestamp(doc['ts'].time, None)
    date = stampconverted.strftime(consolidationmask)

    key = db + "," + collection + "," + operation + "," + date
    if key in resultmap:
        counter = resultmap[key] + 1
    else:
        counter = 1

    resultmap[key] = counter


def flush_result_to_file(resultmap):
    print("flushing result to file: " + outputfile)
    file = open(outputfile,"a")

    if os.path.isfile(outputfile):
        file.write("database,collection,operation,datetime,counter\n")

    for key in resultmap:
        line = key + "," + str(resultmap[key]) + "\n"
        print(line)
        file.write(line)
    file.close()

    resultmap = {}


def main():
    resultmap = {}

    oplog = MongoClient(mongodbhost, username=mongodbusername, password=mongodbpassword,
                        authSource=mongodbauthsource).local.oplog.rs
    stamp = oplog.find().sort('$natural', ASCENDING).limit(-1).next()['ts']

    page = 0
    currentdatekey = None
    while True:
        kw = {}

        kw['filter'] = {'ts': {'$gt': stamp}}
        kw['cursor_type'] = CursorType.TAILABLE_AWAIT
        kw['oplog_replay'] = True

        cursor = oplog.find(**kw)

        try:
            while cursor.alive:
                page += 1
                print("starting page " + str(page) + " processing")
                process_cursor(cursor, resultmap, currentdatekey)
                print("page " + str(page) + " processed")
                sleep(_SLEEP)

        except AutoReconnect:
            print("sleeping")
            sleep(_SLEEP)

    flush_result_to_file(resultmap)
    print('finished')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Extracts and consolidate your MongoDB oplog registers in a analytics ready format')

    parser.add_argument('--host', default="localhost:27017", type=unicode,
                    help="MongoDB host:port")
    parser.add_argument('--username', default=None, type=unicode,
                    help="MongoDB username (must have rights to read the oplog)")
    parser.add_argument('--password', type=Password, help='MongoDB password',
                        default=Password.DEFAULT)
    parser.add_argument('--authenticationDatabase', default=None, type=unicode,
                    help="MongoDB authentication database")
    parser.add_argument('--outputfile', default="output.csv", type=unicode,
                    help="Output file path (only CSV supported)")
    parser.add_argument('--aggregateby', default="hour", type=unicode,
                    help="The way the data will be aggregate in the outputfile", choices=('hour', 'minute'))

    # TODO - add way to query just a specific period of the oplog

    args =  parser.parse_args()

    mongodbhost = args.host
    mongodbusername = args.username
    mongodbauthsource = args.authenticationDatabase
    outputfile = args.outputfile

    if args.aggregateby == "hour":
        consolidationmask = _HOUR_MASK
    else:
        consolidationmask = _MINUTE_MASK

    mongodbpassword = str(args.password)

    main()
