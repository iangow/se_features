library(dplyr, warn.conflicts = FALSE)
library(DBI)
library(tidyr)

pg <- dbConnect(RPostgres::Postgres(), bigint = "integer")
rs <- dbExecute(pg, "SET search_path TO non_answer, se_features")
rs <- dbExecute(pg, "SET work_mem = '5GB'")

fog <- tbl(pg, "fog_measure")
liwc <- tbl(pg, "liwc")
word_counts <- tbl(pg, "word_counts")
tone_measure <- tbl(pg, "tone_measure")

non_answers_tagged <- tbl(pg, "non_answers_tagged")
regexes <- tbl(pg, "regexes")
gold_standard <- tbl(pg, "gold_standard")

gs_file_names <- gold_standard %>% distinct(file_name) %>% compute()

regex_matches <-
    non_answers_tagged %>%
    mutate(has_non_answer = !is.na(non_answers)) %>%
    mutate(non_answer = sql("unnest(non_answers)")) %>%
    mutate(regex_id = sql("(non_answer->'regex_id')::text::integer")) %>%
    left_join(regexes, by = "regex_id") %>%
    filter(!is.na(category)) %>%
    distinct(file_name, section, answer_nums, regex_id) %>%
    collect() %>%
    mutate(value = TRUE, regex_id = sprintf("regex_%02d", regex_id)) %>%
    spread(regex_id, value, fill = FALSE)

liwc_mod <-
    liwc %>%
    semi_join(gs_file_names, by = "file_name") %>%
    filter(context == "qa") %>%
    select(-context, -speaker_name) %>%
    compute()

word_counts_mod <-
    word_counts %>%
    semi_join(gs_file_names, by = "file_name") %>%
    filter(context == "qa") %>%
    select(-context, -speaker_name) %>%
    compute()

# Make fog zero is missing.
# See speaker_data %>% filter(file_name == "4865058_T", speaker_number == 7L)
fog_mod <-
    fog %>%
    semi_join(gs_file_names, by = "file_name") %>%
    filter(context == "qa") %>%
    select(-context) %>%
    mutate_at(.vars = vars(matches("^fog")), list(~coalesce(., 0))) %>%
    compute()

tone_measure_mod <-
    tone_measure %>%
    semi_join(gs_file_names, by = "file_name") %>%
    filter(context == "qa") %>%
    select(-context) %>%
    compute()

all_features <-
    liwc_mod %>%
    inner_join(word_counts_mod,
               by = c("file_name", "last_update", "section", "speaker_number")) %>%
    inner_join(fog_mod,
               by = c("file_name", "last_update", "section", "speaker_number")) %>%
    inner_join(tone_measure_mod,
               by = c("file_name", "last_update", "section", "speaker_number")) 

matched_features_raw <-
    gold_standard %>%
    select(file_name, section, answer_nums) %>%
    mutate(speaker_number = unnest(answer_nums)) %>%
    compute() %>% 
    left_join(all_features, by = c("file_name", "section", "speaker_number")) 

matched_features <-
    matched_features_raw %>%
    select(-speaker_number) %>%
    group_by(file_name, last_update, section, answer_nums) %>%
    summarize_all(sum, na.rm = TRUE) %>%
    compute()

all_data <-
    gold_standard %>%
    select(-(is_unable:is_after_call)) %>%
    left_join(matched_features, by = c("file_name", "section", "answer_nums")) %>%
    collect() %>%
    mutate(answer_nums = as.character(answer_nums)) %>%
    left_join(regex_matches, by=c("file_name", "section", "answer_nums")) %>%
    mutate_at(vars(matches("^regex_")), function(x) coalesce(x, FALSE)) 

training_data <-
    all_data %>%
    filter(obs_type == "train") %>%
    select(-obs_type, -last_update) %>%
    copy_to(pg, ., name = "training_data", overwrite=TRUE, temporary = FALSE)

rs <- dbExecute(pg, "ALTER TABLE training_data ADD PRIMARY KEY (file_name, section, answer_nums)")

db_comment <- paste0("CREATED USING create_training_data.R from ",
                     "GitHub iangow/se_features ON ", Sys.time())
rs <- dbExecute(pg, paste0("COMMENT ON TABLE training_data IS '",
                           db_comment, "';"))

rs <- dbExecute(pg, "ALTER TABLE training_data OWNER TO non_answer")
rs <- dbExecute(pg, "GRANT SELECT ON training_data TO non_answer_access")

rs <- dbDisconnect(pg)
