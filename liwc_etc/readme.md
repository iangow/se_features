## Creating linguistic data

#### Overview

| Data                 | Main Code           |Supplementary Codes                | Output table          | Primary key |
| :-------------       |:-------------:      |:-------------:             | :-----                | :------
| LIWC          | `liwc_run.py`       |  `liwc_add.py`, `liwc_functions.py` | `se_features.liwc`    | `(file_name, last_update, section, context, speaker_number)`|
| Word counts   | `word_count_run.py` | `word_count_add.py`, `word_count_functions.py` | `se_features.word_counts` | `(file_name, last_update, section, context, speaker_name)` |

Note that the code is designed to run in an incremental fashion. 
The code first examines which calls are available that are not found in the output table and only processes those calls.
So the code below can be run to update tables after updating StreetEvents.

#### Running word count code.

Run the code below in console from the `liwc_etc` directory.
```bash
export PGHOST="some_host.edu"
export PGDATABASE="db_name"
./word_count_run.py
```
The code will take files from `streetevents.calls` and find word counts for corresponding `speaker_text` data in `streetevents.speaker_data`.
By default, results will be saved in `se_features.word_counts`

(Specify `output_schema` and `output_table` in  `word_count_run.py` to change the output table. 
Specify `num_files` in `word_count_run.py` to run the code for a limited number of files for testing purposes.)

#### Running LIWC code.

Run the code below in console from the `liwc_etc` directory.

```bash
export PGHOST="some_host.edu"
export PGDATABASE="db_name"
./liwc_run.py
```

The code will take files from `streetevents.calls` and find LIWC characteristics for corresponding `speaker_text` values in `streetevents.speaker_data`. The result will be saved in `se_features.liwc`. 

(Specify `output_schema` and `output_table` in  `liwc_run.py` to change the output table. 
Specify `num_files` in `liwc_run.py` to restrict the number of files you are going to run.)

Note that the LIWC code uses the table `se_features.word_list`, which is created from a Google Sheets document by running the code in `liwc_create_word_list.R`.

If you do not want to show your password create [`~/.pgpass` file](https://www.postgresql.org/docs/9.4/static/libpq-pgpass.html) in your home directory. 
After creating the file run `chmod 0600 ~/.pgpass` in terminal.
