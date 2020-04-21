from findner import tagger_init, findner_array
from run_NER import conn_string
from sqlalchemy import create_engine

def processFileNER(file_name, ner_class, ner_table, ner_schema):
    import pandas as pd
    from pandas import Timestamp
    from sqlalchemy.types import DateTime
    
    # Get file NER
    df = getQuestionData(file_name, ner_table, ner_schema)
    
    tagger_init(ner_class)
    
    df['ner_tags'] = findner_array(df['speaker_text'])
    df = df.drop(['speaker_text'], 1)

    # Submit dataframe to database
    engine = create_engine(conn_string)
    
    # This line seems unnecessary as we performed the same step in "getQuestionData" (line 40)
    # engine.execute("DELETE FROM %s.%s WHERE file_name ='%s'" % (ner_schema, ner_table, file_name))

    df['last_update'] =  df['last_update'].map(lambda x: Timestamp(x))
        
    df.to_sql(ner_table, engine, schema=ner_schema, if_exists='append',
              dtype = {'last_update': DateTime(timezone = True)},index=False)
    engine.dispose()

def processFileNER_star(args_izip):
    """Convert `f([1,2,3,4])` to `f(1,2,3,4)` function call. """
    processFileNER(*args_izip)

def getQuestionData(file_name, ner_table, ner_schema):
    from pandas.io.sql import read_sql
    print("file_name", file_name)
    engine = create_engine(conn_string)

    
    sql = "DELETE FROM %s.%s WHERE file_name='%s'" % (ner_schema, ner_table, file_name)
    engine.execute(sql)

    #return all the utterances corresponding to a file_name
    sql = """
        SELECT file_name, last_update, section,context,speaker_number, speaker_text
        FROM streetevents.speaker_data
        WHERE file_name='%s'
    """ % (file_name)
    
    
    df = read_sql(sql, engine)
    engine.dispose()
    return df
