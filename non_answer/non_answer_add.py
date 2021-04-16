#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import pandas as pd
import os
import json
from sqlalchemy import create_engine
from ling_features import non_answers
from sqlalchemy.types import DateTime
from sqlalchemy.dialects.postgresql import JSONB, ARRAY
from nltk import sent_tokenize

conn_string = 'postgresql://' + os.environ['PGHOST'] + '/' + os.environ['PGDATABASE']

def add_non_answers(args):

    """Function to get word count data for all utterances in a call"""

    files_w_dates, output_schema, output_table = args[0], args[1], args[2]

    engine = create_engine(conn_string)

    for index, file_w_date in files_w_dates.iterrows():

        file_name = file_w_date['file_name']
        last_update = file_w_date['last_update']

        sql =  """
        SELECT file_name, last_update, speaker_number, section,
           speaker_text
        FROM streetevents.speaker_data
        WHERE file_name = '%s' AND last_update = '%s'
            AND context='qa'
        """ % (file_name, last_update)

        speaker_data = pd.read_sql(sql, engine)
        speaker_data['last_update'] = speaker_data['last_update'].map(lambda x: str(x.astimezone('UTC')))
        speaker_data['sents'] = speaker_data['speaker_text'].apply(sent_tokenize)
        speaker_data['non_answers'] = speaker_data['sents'].apply(non_answers)
        speaker_data = speaker_data.drop(['speaker_text', 'sents'], axis=1)
        
        # If no speaker_data returned, we still create a DataFrame to keep track
        # of files that have been processed.
        if len(speaker_data)==0:
            d = {'file_name': [file_name], 'last_update': [str(last_update)],
                 'speaker_number':0, 'section':1, 'non_answers': None }
            speaker_data = pd.DataFrame(d)
            
        conn = engine.connect()
        speaker_data.to_sql(output_table, conn, schema=output_schema,
                            if_exists='append', index=False,
                            dtype = {'last_update': DateTime(timezone=True),
                                     'non_answers': ARRAY(JSONB)})
        conn.close()

    engine.dispose()
