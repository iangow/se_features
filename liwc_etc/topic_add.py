#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import pandas as pd
import os
from sqlalchemy import create_engine
from topic_functions import kls_domains_ind
from topic_functions import expand_json

conn_string = 'postgresql://' + os.environ['PGHOST'] + '/' + os.environ['PGDATABASE']

def get_topics(file_name):

    """Function to get KLS topic indicators for all utterances in a call"""
    engine = create_engine(conn_string)

    sql =  """
        SELECT file_name, last_update, context, section, speaker_number,
            speaker_text
        FROM streetevents.speaker_data
        WHERE file_name = '%s' AND speaker_name IS NOT NULL
    """ % (file_name)

    df = pd.read_sql(sql, engine)
    df['last_update'] = df['last_update'].astype(pd.Timestamp)
    df['topics'] = df['speaker_text'].apply(kls_domains_ind)
    df = df.drop(['speaker_text'], axis=1)

    engine.dispose()

    # Expand single JSON field to multiple columns
    df = expand_json(df, 'topics')

    return df

if __name__=="__main__":

    print(get_topics("1148631_T"))
