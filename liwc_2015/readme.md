## Creating linguistic data

#### Overview

| Data                 | Main Code           |Supplementary Codes                | Output table          | Primary key |
| :-------------       |:-------------:      |:-------------:             | :-----                | :------
| LIWC          | `liwc_run.py`       |  `liwc_add.py`, `liwc_functions.py` | `se_features.liwc_2015`    | `(file_name, last_update, section, context, speaker_number)`|

Note that the code is designed to run in an incremental fashion. 
The code first examines which calls are available that are not found in the output table and only processes those calls.
So the code below can be run to update tables after updating StreetEvents.

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

Note that the LIWC code uses the table `se_features.liwc_2015`.

If you do not want to show your password create [`~/.pgpass` file](https://www.postgresql.org/docs/9.4/static/libpq-pgpass.html) in your home directory. 
After creating the file run `chmod 0600 ~/.pgpass` in terminal.
