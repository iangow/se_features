import os
import re
import json
import pandas as pd

from sqlalchemy import create_engine
from pandas.io.json import json_normalize

conn_string = 'postgresql://' + os.environ['PGHOST'] + '/' + os.environ['PGDATABASE']
engine = create_engine(conn_string)

target_schema = "se_features"

engine.execute("SET search_path TO %s, public" % target_schema)
rv = engine.execute("SELECT category FROM %s.liwc_2015" % target_schema)

categories = [ r['category'] for r in rv]

plan = """
    SELECT word_list
    FROM %s.liwc_2015 """ % target_schema + "WHERE category = %s"

mod_word_list = {}
for cat in categories:
    rows = list(engine.execute(plan, [cat]))
    word_list = rows[0]['word_list']
    mod_word_list[cat] = [re.sub('\*(?:\s*$)?', '[a-z]*', word.lower())
                            for word in word_list]

# Pre-compile regular expressions.
regex_list = {}
for key in mod_word_list.keys():
    regex = r"\b(?:" + '|'.join(mod_word_list[key]) + r")[\b']"
    regex_list[key] = re.compile(regex)

def liwc_counts(the_text):
    """Function to return number of matches against a LIWC category in a text"""
    # Construct a counter of the words and return as JSON
    text = re.sub(u'\u2019', "'", the_text).lower()
    the_dict = {cat: len(re.findall(regex_list[cat], text)) for cat in categories}
    return json.dumps(the_dict)

def expand_json(df, col):
    return pd.concat([df.drop([col], axis=1),
                      df[col].map(lambda x: json.loads(x)).apply(pd.Series)], axis=1)
