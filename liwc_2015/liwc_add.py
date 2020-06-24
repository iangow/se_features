import pandas as pd
import os
import time
from sqlalchemy import create_engine

from liwc_functions import add_liwc_counters, expand_json
from sqlalchemy.types import DateTime

conn_string = 'postgresql://' + os.environ['PGHOST'] + '/' + os.environ['PGDATABASE']

def add_liwc(args):

    file_names, output_schema, output_table  = args[0], args[1], args[2]
    engine = create_engine(conn_string)

    for index, file_w_date in file_names.iterrows():

        file_name = file_w_date['file_name']
        last_update = file_w_date['last_update']

        # Get speaker data
        sql = """
            SELECT file_name, last_update, speaker_name,
                context, section, speaker_number, speaker_text
            FROM streetevents.speaker_data
            WHERE speaker_name IS NOT NULL
                AND file_name ='%s' AND last_update = '%s'
            """ % (file_name, last_update)

        speaker_data = pd.read_sql(sql, engine)
        speaker_data['last_update'] = speaker_data['last_update']

        # Calculate LIWC, then drop speaker text
        speaker_data['add_liwc_counters'] = speaker_data['speaker_text'].apply(add_liwc_counters)
        speaker_data = speaker_data.drop(['speaker_text'], axis=1)

        # If no speaker_data returned, we still create a DataFrame to keep track
        # of files that have been processed.
        if len(speaker_data)==0:
            d = {'file_name': [file_name], 'last_update': [last_update]}
            speaker_liwc = pd.DataFrame(d)
            speaker_liwc['last_update'] = speaker_liwc['last_update']
        else:
            # Expand single JSON field to multiple columns
            speaker_liwc = expand_json(speaker_data, 'add_liwc_counters')

        # Export LIWC data from the call to Postgres
        conn = engine.connect()
        table_exists = engine.dialect.has_table(engine, output_table, schema=output_schema)
        speaker_liwc.to_sql(output_table,
                            conn, schema=output_schema, if_exists='append', index=False,
                            dtype = {'last_update': DateTime(timezone = True)})
        if not table_exists:
            sql = """
                CREATE INDEX ON %s.%s (file_name);
                CREATE INDEX ON %s.%s (file_name, last_update);
            """ % (output_schema, output_table,
                   output_schema, output_table)

            engine.execute(sql)
        conn.close()

    engine.dispose()
