#!/usr/bin/env python
from sqlalchemy import create_engine
from run_NER import conn_string
from datetime import date


the_schema="se_features"
table_ner_class_4="ner_class_alt_4"
table_ner_class_7="ner_class_alt_7"


def create_ner_class_table(engine,the_schema,the_table):
    
    sql = """
            CREATE TABLE %s.%s
            (
            file_name text,
            last_update timestamp with time zone,
            speaker_number int,
            section int,
            context text,
            ner_tags jsonb
            PRIMARY KEY (file_name, last_update, speaker_number, context, section)
            );

            CREATE INDEX ON %s.%s (file_name);
            CREATE INDEX ON %s.%s (file_name, last_update);

            ALTER TABLE %s.%s OWNER TO %s;
            GRANT SELECT ON %s.%s TO %s;

            comment on table %s.%s is 'CREATED USING iangow/se_features/ner/create_table.py ON %s';
        """ % (the_schema, the_table,
                the_schema, the_table,
                the_schema, the_table,
                the_schema,the_table,the_schema,
                the_schema, the_table, the_schema + "_access",
                the_schema, the_table,str(date.today()))
    engine.execute(sql)

def check_and_create_tables():
    
    engine = create_engine(conn_string)
    conn = engine.connect()
    ner_class_4_table_exists = engine.dialect.has_table(conn, table_ner_class_4, schema=the_schema)
    ner_class_7_table_exists = engine.dialect.has_table(conn, table_ner_class_7, schema=the_schema)
    conn.close()

    if not ner_class_4_table_exists:
        print("creating table",table_ner_class_4)
        create_ner_class_table(engine,the_schema,table_ner_class_4)

    if not ner_class_7_table_exists:
        print("creating table",table_ner_class_7)
        create_ner_class_table(engine,the_schema,table_ner_class_7)
    
    engine.dispose()

if __name__=="__main__":
    check_and_create_tables()
