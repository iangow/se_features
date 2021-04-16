#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from multiprocessing import Pool, freeze_support
from non_answer_add import add_non_answers
import pandas as pd
import os
from sqlalchemy import create_engine
import time

output_schema = "se_features"
output_table  = "non_answers_test"

conn_string = 'postgresql://' + os.environ['PGHOST'] + '/' + \
                os.environ['PGDATABASE']

def getFileNames(output_table, output_schema,
                                num_files = None):

    engine = create_engine(conn_string)

    # Using LIMIT is much faster than getting all files and ditching
    # unneeded ones.
    if num_files==None:
        limit_clause = ""
    else:
        limit_clause = "LIMIT %s" % (num_files)

    # Get a list of unprocessed files. Query differs according to whether
    # any files have been processed (i.e., output_table exists)
    table_exists = engine.dialect.has_table(engine, output_table, schema=output_schema)
    engine.dispose()

    if table_exists:
        sql = """
            WITH

            new_files AS (
            	SELECT file_name, max(last_update) AS last_update
            	FROM streetevents.calls
                GROUP BY file_name),

            unprocessed_files AS (
            	SELECT file_name, last_update FROM new_files
            	EXCEPT
            	SELECT file_name, last_update FROM %s.%s)

            SELECT DISTINCT * FROM unprocessed_files
            %s
        """ % (output_schema, output_table, limit_clause)
        engine = create_engine(conn_string)
        files = pd.read_sql(sql, engine)
        engine.dispose()
    else:
        sql = """CREATE TABLE %s.%s
                (
                    file_name text NOT NULL,
                    last_update timestamp with time zone NOT NULL,
                    speaker_number integer,
                    section integer NOT NULL,
                    non_answers jsonb[],
                    PRIMARY KEY (file_name, last_update, speaker_number, section)
                );

               ALTER TABLE %s.%s OWNER TO %s;
            """ % (output_schema, output_table,
                   output_schema, output_table,
                   output_schema)

        engine = create_engine(conn_string)
        engine.execute(sql)

        # Create indexes on table.
        sql = """
                CREATE INDEX ON %s.%s (file_name);
                CREATE INDEX ON %s.%s (file_name, last_update);
            """ % (output_schema, output_table,
                   output_schema, output_table)

        engine = create_engine(conn_string)
        engine.execute(sql)

        sql = """
        	SELECT DISTINCT file_name, max(last_update) AS last_update
        	FROM streetevents.calls AS a
        	GROUP BY file_name
            %s
        """ % (limit_clause)

        files = pd.read_sql(sql, engine)
        engine.dispose()

    return files

def chunk_list(l, n):
    """Yield n successive chunks from l."""
    size = round(len(l)/n) + 1
    for i in range(0, len(l), size):
        yield l[i:i + size]

if __name__ == '__main__':
    freeze_support()
    
    # Get a list of files to work on.
    files = getFileNames(output_table, output_schema)
    print("n_files: %d" % len(files))

    num_threads = 4

    pool = Pool(num_threads)
    
    # Chunk files into num_threads chunks
    files_input = [(files, output_schema, output_table)
                        for files in chunk_list(files, num_threads)]
    res = pool.map(add_non_answers, files_input)
    
    engine = create_engine(conn_string)
    conn = engine.connect()
    db_comment = "CREATED USING non_answer/non_answer_run.py from " + \
                  "GitHub iangow/se_features ON " + \
                  time.asctime(time.gmtime()) + ' UTC'
    
    conn.execute("COMMENT ON TABLE %s.%s IS '%s'" % (output_schema, output_table, db_comment))
    conn.execute("ALTER TABLE %s.%s OWNER TO %s" % (output_schema, output_table, output_schema))
    conn.execute("GRANT SELECT ON TABLE %s.%s TO %s_access" %
                    (output_schema, output_table, output_schema))
    conn.close()
    engine.dispose()
    
