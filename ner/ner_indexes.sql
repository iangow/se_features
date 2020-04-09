SET maintenance_work_mem='1GB';

CREATE INDEX ON bs_linguistics.ner_class_alt_7 (file_name, last_update, speaker_number);
CREATE INDEX ON bs_linguistics.ner_class_alt_4 (file_name, last_update, speaker_number);