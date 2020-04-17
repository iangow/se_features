import os

os.environ["PGHOST"] = "10.101.13.99"
os.environ["PGDATABASE"] = "crsp"

conn_string = 'postgresql://' + os.environ['PGHOST'] + '/' + os.environ['PGDATABASE']

def getFileNames(ner_table, ner_schema, num_files=None):
    import pandas as pd
    from pandas import Timestamp
    from sqlalchemy import create_engine
    from sqlalchemy.types import DateTime
    
    engine = create_engine(conn_string)

    # Using LIMIT is much faster than getting all files and ditching unneeded ones.
    if num_files==None:
        limit_clause = ""
    else:
        limit_clause = "LIMIT %s" % (num_files)

    # Get a list of unprocessed files. Query differs according to whether
    # any files have been processed (i.e., ner_table exists)
    if engine.dialect.has_table(engine.connect(), ner_table, schema=ner_schema):
        sql = """
            WITH latest_call AS (
                SELECT file_name, max(last_update) as last_update
                FROM streetevents.speaker_data
                GROUP BY file_name)
            SELECT file_name, last_update
            FROM latest_call
            EXCEPT
            SELECT file_name, last_update
            FROM %s.%s
            %s
        """ % (ner_schema, ner_table, limit_clause)
    else:
        sql = """
            SELECT file_name, max(last_update) as last_update
            FROM streetevents.speaker_data
            GROUP BY file_name
            %s
        """ % (limit_clause)

    files = pd.read_sql(sql, engine)
    print("files got:", len(files))
    
    # To be checked: Can I use `utc = True` here?
    #files['last_update'] =  files['last_update'].apply(lambda d: to_datetime(str(d), utc = True))
    #files['last_update'] =  files['last_update'].astype(pd.Timestamp)
    #files['last_update'] =  files['last_update'].map(lambda x: str(x.astimezone('UTC')))
    
    files['last_update'] =files['last_update'].map(lambda x: Timestamp(x))
    # files['last_update'] = files['last_update'].astype(pd.Timestamp)
    
    files.to_sql(ner_table, engine, schema=ner_schema, if_exists='append',
              dtype = {'last_update': DateTime(timezone = True)},index=False)

    engine.dispose()

    return files

if __name__ == "__main__":
    from multiprocessing import Pool
    from itertools import repeat
    import argparse
    from create_table import check_and_create_tables
    from sqlalchemy.types import DateTime

    #checks to see if ner_class_alt_4 and ner_class_alt_7 tables exist and create if they do not.
    check_and_create_tables()

    parser = argparse.ArgumentParser(prog='run_NER.py')

    parser.add_argument('--ner-class', dest='ner_class', nargs='?',
                           help='Desired NER class (4 or 7; default: 7)',
                           default=7)
    args = parser.parse_args()
    ner_class=int(args.ner_class)

    from processFileNER import processFileNER, processFileNER_star

    ner_schema = 'se_features'
    ner_table = 'ner_class_alt_' + str(ner_class)

    # Get a list of files to work on.
    num_threads = int(os.environ['SLURM_CPUS_ON_NODE']) if (os.environ.get('SLURM_CPUS_ON_NODE') is not None) else 8
    files = getFileNames(ner_table, ner_schema)
    files = files['file_name']

    # Set up multiprocessing environment
    pool = Pool(num_threads)

    # Do the work!
    # Using functools.partial would seem to be a viable alternative here.
    print(files)
    args = zip(files, repeat(ner_class), repeat(ner_table), repeat(ner_schema))

    pool.map(processFileNER_star, args)
