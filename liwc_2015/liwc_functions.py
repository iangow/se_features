import os
import re
import json
import pandas as pd

from collections import Counter 
from sqlalchemy import create_engine
from pandas.io.json import json_normalize

conn_string = 'postgresql://' + os.environ['PGHOST'] + '/' + os.environ['PGDATABASE']
engine = create_engine(conn_string)

target_schema = "se_features"

### LIWC positive counter 
# Get positive word lists
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
    mod_word_list[cat] = [re.sub('\*(?:\s*$)?', "[a-z0-9]*(')?[a-z0-9]*", word.lower())
                            for word in word_list]
    
# Include numeric values in Number category
mod_word_list['Number'].append('[0-9]+(\.[0-9]+)?')
mod_word_list['Number'].append('\.[0-9]+')

# Include a word that is not shown in dict but used by LIWC
mod_word_list['Affect'].append('(would) like')
mod_word_list['Posemo'].append('(would) like')

# Pre-compile regular expressions.
regex_list = {}
for key in mod_word_list.keys():
    # NOTE: Deleted hyphen for testing
    regex = r"\b(?:" + "|".join(mod_word_list[key]) + r")(?=(?:[^a-zA-Z0-9_']|$))"
    regex_list[key] = re.compile(regex)
    
# Define a positive counter
def pos_liwc_counter(the_text):
    text = re.sub(u'\u2019', "'", the_text).lower()
    the_dict = Counter()
    for cat in categories:
        num = len(re.findall(regex_list[cat], text))
        the_dict[cat] += num
    return the_dict


### LIWC negative counter 
# Get negative word lists (counts need to be subtracted if a word appears)
engine.execute("SET search_path TO %s, public" % target_schema)
rv = engine.execute("SELECT id FROM %s.negative_liwc_2015" % target_schema)

neg_ids = [r['id'] for r in rv]

plan = """
    SELECT word_list
    FROM %s.negative_liwc_2015 """ % target_schema + "WHERE id = %s"

neg_mod_word_list = {}
for neg_id in neg_ids:
    rows = list(engine.execute(plan, [neg_id]))
    word_list = rows[0]['word_list']
    neg_mod_word_list[neg_id] = [re.sub('\*(?:\s*$)?', '[a-z]*', word.lower())
                            for word in word_list]
    
plan = """
    SELECT category
    FROM %s.negative_liwc_2015 """ % target_schema + "WHERE id = %s"

# Pre-compile regular expressions.
neg_cat_list = {}
neg_regex_list = {}

for key in neg_mod_word_list.keys():
    rows = list(engine.execute(plan, [key]))
    neg_cat = rows[0]['category']
    neg_cat_list[key] = neg_cat
    # NOTE: Deleted hyphen for testing
    neg_regex = r"\b(?:" + "|".join(neg_mod_word_list[key]) + r")(?=(?:[^a-zA-Z0-9_']|$))"
    neg_regex_list[key] = re.compile(neg_regex)

def neg_liwc_counter(the_text):
    text = re.sub(u'\u2019', "'", the_text).lower()
    the_neg_dict = Counter()
    for neg_id in neg_ids:
        neg_cat = neg_cat_list[neg_id]
        num = len(re.findall(neg_regex_list[neg_id], text))
        the_neg_dict[neg_cat] += num
    return the_neg_dict


### Add positive and negative counters together 
def add_liwc_counters(the_text):
    pos_counter = pos_liwc_counter(the_text)
    neg_counter = neg_liwc_counter(the_text)
    pos_counter.subtract(neg_counter.elements())
    return json.dumps(pos_counter)
    

def expand_json(df, col):
    return pd.concat([df.drop([col], axis=1),
                      df[col].map(lambda x: json.loads(x)).apply(pd.Series)], axis=1)
