This is a profiling tool that analyzes the content of your oplog and saves in a analytics format to be open in any BI Tool.


## Installing
Just install from PyMongo:

```bash
$ pip install pymongo
```


## Running

```bash
$ python profile.py [-h] [--host HOST] [--username USERNAME]
                  [--password PASSWORD]
                  [--authenticationDatabase AUTHENTICATIONDATABASE]
                  [--outputfile OUTPUTFILE] [--aggregateby {hour,minute}]
```

## The result File

The result file will be a CSV in the following format

| database | collection | operation | date | counter |
|----------|------------|-----------|------|---------|
| db1      | foo        | u         | 2018-07-01T18 | 10 |
| db1      | foo        | i         | 2018-07-01T18 | 1 |
| db2      | bar        | i         | 2018-07-01T19 | 2 |

You can import this file in any BI Tool or in a Excel Dynamic Table