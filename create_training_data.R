library(dplyr, warn.conflicts = FALSE)
library(DBI)
library(tidyr)

pg <- dbConnect(RPostgres::Postgres(), bigint = "integer")
rs <- dbExecute(pg, "SET search_path TO non_answer, se_features")

fog <- tbl(pg, "fog_measure")
liwc <- tbl(pg, "liwc")
word_counts <- tbl(pg, "word_counts")
tone_measure <- tbl(pg, "tone_measure")

non_answers_tagged <- tbl(pg, sql("SELECT * FROM non_answer.non_answers_tagged"))
regexes <- tbl(pg, sql("SELECT * FROM non_answer.regexes"))
gold_standard <- tbl(pg, sql("SELECT * FROM non_answer.gold_standard"))

regex_matches <-
    non_answers_tagged %>%
    mutate(has_non_answer = !is.na(non_answers)) %>%
    mutate(non_answer = sql("unnest(non_answers)")) %>%
    mutate(regex_id = sql("(non_answer->'regex_id')::text::integer")) %>%
    left_join(regexes, by = "regex_id") %>%
    filter(!is.na(category)) %>%
    distinct(file_name, answer_nums, regex_id) %>%
    collect() %>%
    mutate(value = TRUE, regex_id = sprintf("regex_%02d", regex_id)) %>%
    spread(regex_id, value, fill = FALSE)

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
    select(-context, -section)

tone_measure_mod <-
    tone_measure %>%
    filter(section == 1L, context == "qa") %>%
    select(-context, -section)

all_features <-
    liwc_mod %>%
    inner_join(word_counts_mod,
               by = c("file_name", "last_update", "speaker_number")) %>%
    inner_join(fog_mod,
               by = c("file_name", "last_update", "speaker_number")) %>%
    inner_join(tone_measure_mod,
               by = c("file_name", "last_update", "speaker_number"))
    
matched_features_raw <-
    gold_standard %>%
    select(file_name, answer_nums) %>%
    mutate(speaker_number = unnest(answer_nums)) %>%
    inner_join(all_features, by = c("file_name", "speaker_number"))

matched_features <-
    matched_features_raw %>%
    select(-speaker_number) %>%
    group_by(file_name, last_update, answer_nums) %>%
    summarize_all(sum, na.rm = TRUE) %>%
    compute()

all_data <-
    gold_standard %>%
    select(-(is_unable:is_after_call)) %>%
    left_join(matched_features, by = c("file_name", "answer_nums")) %>%
    collect() %>%
    mutate(answer_nums = as.character(answer_nums)) %>%
    left_join(regex_matches, by=c("file_name", "answer_nums")) %>%
    mutate_at(vars(matches("^regex_")), function(x) coalesce(x, FALSE)) 

training_data <-
    all_data %>%
    filter(obs_type == "train") %>%
    select(-obs_type, -last_update)

rs <- dbDisconnect(pg)

objs <- ls()
rm(list = c(objs[objs != "training_data"], "objs"))
