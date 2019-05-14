library(dplyr, warn.conflicts = FALSE)
library(DBI)

pg <- dbConnect(RPostgres::Postgres())
rs <- dbExecute(pg, "SET search_path TO non_answer, se_features")

fog <- tbl(pg, "fog_measure")
liwc <- tbl(pg, "liwc")
word_counts <- tbl(pg, "word_counts")

gold_standard <- tbl(pg, sql("SELECT * FROM non_answer.gold_standard"))

liwc_mod <-
    liwc %>%
    filter(section == 1L, context == "qa") %>%
    select(-context, -speaker_name, -section)

word_counts_mod <-
    word_counts %>%
    filter(section == 1L, context == "qa") %>%
    select(-context, -speaker_name, -section)

fog_mod <-
    fog %>%
    filter(section == 1L, context == "qa") %>%
    select(-context, -section, -(count:sum_num))


all_features <-
    liwc_mod %>%
    inner_join(word_counts_mod,
               by = c("file_name", "last_update", "speaker_number")) %>%
    inner_join(fog_mod,
               by = c("file_name", "last_update", "speaker_number"))

matched_features_raw <-
    gold_standard %>%
    select(file_name, answer_nums) %>%
    mutate(speaker_number = unnest(answer_nums)) %>%
    inner_join(all_features, by = c("file_name", "speaker_number"))

matched_features <-
    matched_features_raw %>%
    group_by(file_name, last_update, answer_nums) %>%
    summarize_all(sum, na.rm = TRUE) %>%
    select(-fog) %>%
    compute()

training_data <-
    gold_standard %>%
    filter(obs_type == "train") %>%
    select(-obs_type, -(is_unable:is_after_call)) %>%
    inner_join(matched_features, by = c("file_name", "answer_nums")) %>%
    collect()
