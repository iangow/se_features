# Get LIWC table from Google Sheets
library(googlesheets)

target_schema <- "se_features"
# As a one-time thing per user and machine, you will need to run gs_auth()
# to authorize googlesheets to access your Google Sheets.
key <- "17GGtdEyQI1ioAwlnjHBMV4rgANY7IQkiN_peC2i3llw"
gs <- gs_key(key)

wordlist <- gs_read(gs, ws = "liwc_words")
names(wordlist) <- tolower(names(wordlist))

# Convert from "wide" to "long" format (in long format, columns
# are category and word)
df <- NULL
for (i in 1:(dim(wordlist)[2])) {
    temp <- wordlist[, i]
    df <- rbind(df, data.frame(category=names(wordlist)[i], word=unlist(temp)))
}
df <- subset(df, !is.na(word))

# Push data to PostgreSQL
library(RPostgreSQL)
pg <- dbConnect(PostgreSQL())
rs <- dbExecute(pg, sprintf("SET search_path TO %s", target_schema))
rs <- dbWriteTable(pg, "word_list_raw", df, overwrite=TRUE, row.names=FALSE)

# Create a new table in PostgreSQL where words are aggregated into an
# array of words for each category.

rs <- dbGetQuery(pg, sprintf("
    DROP TABLE IF EXISTS word_list;

    CREATE TABLE word_list AS
    SELECT category, array_agg(word) AS word_list
    FROM word_list_raw
    GROUP BY category;

    ALTER TABLE se_features.word_list OWNER TO se_features;
    ALTER TABLE word_list OWNER TO %s;

    DROP TABLE IF EXISTS word_list_raw", target_schema))

db_comment <- paste0("CREATED USING liwc_etc/",
                     "liwc_create_word_list.R and data in ",
                    "https://docs.google.com/spreadsheets/d/", key, " ON ",
                    Sys.time())
dbGetQuery(pg, paste0("COMMENT ON TABLE word_list IS '",
                      db_comment, "'"))

rs <- dbDisconnect(pg)
rm(wordlist, df)
