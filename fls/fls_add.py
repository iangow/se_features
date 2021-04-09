#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import pandas as pd
import os
import json
from sqlalchemy import create_engine
from fls_functions import fls_fog
from sqlalchemy.types import DateTime

conn_string = 'postgresql://' + os.environ['PGHOST'] + '/' + os.environ['PGDATABASE']

def expand_json(df, col):
    return pd.concat([df.drop([col], axis=1),
                      df[col].map(lambda x: json.loads(json.dumps(x))).apply(pd.Series)], axis=1)

def add_word_counts(args):

    """Function to get word count data for all utterances in a call"""

    files_w_dates, output_schema, output_table = args[0], args[1], args[2]

    engine = create_engine(conn_string)

    # Get data. Data is JSON converted to text.
    for index, file_w_date in files_w_dates.iterrows():

        file_name = file_w_date['file_name']
        last_update = file_w_date['last_update']

        sql =  """
        SELECT file_name, last_update, speaker_number, context, section,
           speaker_text
        FROM streetevents.speaker_data
        WHERE file_name = '%s' AND last_update = '%s'
        """ % (file_name, last_update)

        speaker_data = pd.read_sql(sql, engine)
        speaker_data['last_update'] = speaker_data['last_update'].map(lambda x: str(x.astimezone('UTC')))
        speaker_data['fls_data'] = speaker_data['speaker_text'].apply(fls_fog)
        speaker_data = speaker_data.drop(['speaker_text'], axis=1)
        
        # If no speaker_data returned, we still create a DataFrame to keep track
        # of files that have been processed.
        if len(speaker_data)==0:
            d = {'file_name': [file_name], 'last_update': [str(last_update)],
                 'speaker_number': '0', 'context': 'pres', 'section':1 }
            speaker_data = pd.DataFrame(d)

        else:
            # Expand single JSON field to multiple columns
            speaker_data = expand_json(speaker_data, 'fls_data')

        conn = engine.connect()
        speaker_data.to_sql(output_table, conn, schema=output_schema, if_exists='append',
                            index=False,
                            dtype = {'last_update': DateTime(timezone=True)})
        conn.close()

    engine.dispose()
