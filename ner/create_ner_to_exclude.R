# List of common finance and regulator abbreviations to exclude from NER

library(dplyr)
library(RPostgreSQL)
library(googlesheets4)

pg <- dbConnect(PostgreSQL())

# Project schema
proj_schema <- "se_features_old"
rs <- dbGetQuery(pg, paste0("SET search_path = '", proj_schema, "'"))

# sheets_deauth()
# sheets_auth()
gs <- read_sheet("1VjNgTKUYRvbM0RbAll6ZuXl9zH64gq6c-_qDqWjQtfY")

# Write to PG ----

dbGetQuery(pg, "DROP TABLE IF EXISTS ner_to_exclude")

ner_to_exclude <- 
    gs %>%
    copy_to(pg, ., name = "ner_to_exclude", temporary = FALSE)

comment <- 'CREATED USING azakolyukina/se_features/create_ner_to_exclude.R'
db_comment <- paste0("COMMENT ON TABLE ner_to_exclude IS '",
                     comment, " ON ", Sys.time() , "'; ") 
dbGetQuery(pg, db_comment)

dbGetQuery(pg, "ALTER TABLE ner_to_exclude OWNER TO se_features")
dbGetQuery(pg, "GRANT SELECT ON TABLE ner_to_exclude TO se_features_access")
dbGetQuery(pg, "VACUUM FULL ANALYZE ner_to_exclude")

dbDisconnect(pg)
