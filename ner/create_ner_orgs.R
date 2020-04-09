table_name <- "ner_orgs"

library(dplyr)
library(RPostgreSQL)

# Number of conference calls to sample

pg <- src_postgres(host="aaz.chicagobooth.edu", dbname="postgres")

rs <- RPostgreSQL::dbGetQuery(pg$con, "
    CREATE OR REPLACE FUNCTION extract_orgs(jsonb)
    RETURNS SETOF text AS
    $CODE$
    	SELECT trim(both '\"'
            FROM json_array_elements(($1->'ORGANIZATION')::json)::text)
    $CODE$ LANGUAGE sql IMMUTABLE STRICT;")

ner_4 <-
    tbl(pg, sql("SELECT * FROM bs_linguistics.ner_class_alt_4"))

ner_7 <-
    tbl(pg, sql("SELECT * FROM bs_linguistics.ner_class_alt_7"))

qa_pairs <- tbl(pg, sql("SELECT * FROM streetevents.qa_pairs"))

rs <- dbGetQuery(pg$con,
                 paste0("DROP TABLE IF EXISTS bs_linguistics.", table_name))

ner_all <-
    ner_4 %>%
    union(ner_7) %>%
    mutate(org=extract_orgs(ner_tags)) %>%
    select(file_name, last_update, speaker_number, org)

answer_orgs <-
    qa_pairs %>%
    mutate(speaker_number=unnest(question_nums)) %>%
    inner_join(ner_all) %>%
    select(file_name, last_update, question_nums, org)

question_orgs <-
    qa_pairs %>%
    mutate(speaker_number=unnest(answer_nums)) %>%
    inner_join(ner_all) %>%
    select(file_name, last_update, question_nums, org)

qa_pair_orgs <-
    answer_orgs %>%
    union(question_orgs)

ner_orgs <-
    qa_pair_orgs %>%
    group_by(file_name, last_update, question_nums) %>%
    summarize(orgs=array_agg(org)) %>%
    compute(name = table_name,
            indexes=c("file_name", "last_update", "question_nums"),
            temporary=FALSE)

rs <- dbGetQuery(pg$con,
                 paste0("ALTER TABLE ", table_name,
                        " SET SCHEMA bs_linguistics"))

ner_orgs <- tbl(pg, sql(paste0("SELECT * FROM bs_linguistics.", table_name)))

rs <- dbGetQuery(pg$con,
                 paste0("ALTER TABLE bs_linguistics.", table_name,
                 " OWNER TO bs_linguistics"))
