#!/usr/bin/env python3
from multiprocessing import Pool
from liwc_add import add_liwc
from sqlalchemy import create_engine
import os
import pandas as pd
import time

conn_string = 'postgresql://' + os.environ['PGHOST'] + '/' + os.environ['PGDATABASE']

def getFileNames(output_table, output_schema, num_files = None):

    engine = create_engine(conn_string)

    # Using LIMIT is much faster than getting all files and ditching
    # unneeded ones.
    if not num_files:
        limit_clause = ""
    else:
        limit_clause = "LIMIT %s" % (num_files)

    # Get a list of unprocessed files. Query differs according to whether
    # any files have been processed (i.e., output_table exists)
    table_exists = engine.dialect.has_table(engine, output_table,
                                                schema=output_schema)
    engine.dispose()

    if table_exists:
        sql = """
            WITH
            latest_call AS (
                SELECT file_name, max(last_update) AS last_update
                FROM streetevents.calls AS a
                WHERE event_type = 1
                GROUP BY file_name
                EXCEPT
                SELECT file_name, last_update FROM %s.%s)
            SELECT DISTINCT * FROM latest_call
            %s
        """ % (output_schema, output_table, limit_clause)
    else:
        sql = """
            WITH
            latest_call AS (
                SELECT file_name, max(last_update) AS last_update
                FROM streetevents.calls AS a
                WHERE event_type = 1
                GROUP BY file_name)
            SELECT DISTINCT * FROM latest_call
            %s
        """ % (limit_clause)

    engine = create_engine(conn_string)
    files = pd.read_sql(sql, engine)
    engine.dispose()

    return files

output_schema = "bs_linguistics"
output_table  = "liwc"

# Get a list of files to work on.
files = getFileNames(output_table, output_schema)
print("n_files: %d" % len(files))

# Set up multiprocessing environment
num_threads = 14
pool = Pool(num_threads)

def chunk_list(l, n):
    """Yield n successive chunks from l."""
    size = round(len(l)/n) + 1
    for i in range(0, len(l), size):
        yield l[i:i + size]

# Chunk files into num_threads chunks
files_input = [(files, output_schema, output_table)
                    for files in chunk_list(files, num_threads)]
res = pool.map(add_liwc, files_input)

engine = create_engine(conn_string)
conn = engine.connect()
db_comment = "CREATED USING get_liwc_data.py from " + \
              "GitHub azakolyukina/bs_linguistics ON " + \
              time.asctime(time.gmtime()) + ' UTC'

conn.execute("COMMENT ON TABLE %s.%s IS '%s'" % (output_schema, output_table, db_comment))
conn.execute("ALTER TABLE %s.%s OWNER TO %s" % (output_schema, output_table, output_schema))
conn.execute("GRANT SELECT ON TABLE %s.%s TO %s_access" %
                (output_schema, output_table, output_schema))
conn.close()
engine.dispose()
