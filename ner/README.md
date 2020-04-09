# Stanford NER tagger

The code in this directory (`proper_nouns`) processes questions from earnings conference calls to extract named entity relations (NERs) using the [Stanford NER tagger](https://nlp.stanford.edu/software/CRF-NER.shtml).

We use `Stanford NER` with `4 class` and `7 class` models trained using different data (see [here](https://nlp.stanford.edu/software/CRF-NER.shtml) for details on training sets): 
* `4 class:`	Location, Person, Organization, Misc
* `7 class:`	Location, Person, Organization, Money, Percent, Date, Time

Data overview
-----------

There are two tables associated with code in `proper_nouns` directory, all found in the `bs_linguistics` schema:
1. `ner_class_alt_4` build by code in `proper_nouns` directory based on `4 class` model. NER tags are stored in (binary) JSON format.
2. `ner_class_alt_7` build by code in `proper_nouns` directory based on `7 class` model. NER tags are stored in (binary) JSON format.

**Note 1:** `4 class` model (and related tables `ner_class_alt_4` and `ner_4class_q`) seems to be less accurate than `7 class` model. 
E.g., `ner_organization` for `4 class` model includes `{"{OK}";23217, "{Hi}";5584, "{Sorry}";4621}` and `7 class` model does not include these as top entries. 

**Note 2:** `bs_linguistics.question_ner_features` records features of each question in `bs_linguistics.ner_class_alt_7` (excludes the list from `bs_linguistics.ner_to_exclude`, which has no impact on the current version of `ner_class_alt_7`). Proper nouns are expected to have types in `{location, organization, money, percent, date}`. For each type, the code calculates `has_type`(bool) and `num_type`(count).

**Note 3:** Only records from the `streetevents.speaker_data` table where `context='qa'` and `section=1` are considered. 

Running code
-----------

### Alternative 1: UChicago RCC

The code is set up to be run as a batch program on the UChicago RCC system.
The batch code is found in either `run_batch_7.sbatch` or `run_batch_4.sbatch` file depending on which ner-class you want to run.
To submit a job, you need to be in the `proper_nouns` directory and run either the `sbatch run_batch_7.sbatch` or `sbatch run_batch_4.sbatch` command.
If you want to submit multiple copies of this job you need to run either the `sbatch --array=1-n run_batch_7.sbatch` or `sbatch --array=1-n run_batch_4.sbatch` command where `n` is the number of copies of this job that you want to submit.

This program runs the `proper_nouns/run_NER.py` code.

The output of the program is stored in a PostgreSQL database.
The host name, database name, and username are hard-wired in `run_batch_7.sbatch` and `run_batch_4.sbatch` files.

### Alternative 2: Run `proper_nouns/run_NER.py` directly

An alternative is to run `proper_nouns/run_NER.py` directly.
If a single node suffices for the data to be processed, this is a simple approach. From the `proper_nouns` directory, enter the following in the terminal to run with the 4-class `ner-class` (alternative is 7-class):

```bash
run_NER.py --ner-class 4
```

The output of the program is stored in a PostgreSQL database.
The host name, database name, and username can be set with `PGHOST`,
`PGDATABASE`, and `PGUSER` environment variables respectively.

### Program details

Note that `run_NER.py` in turn depends on `processFileNER`, which contains the code for applying the core function `findner`,
which is imported from `findner.py`, to all the data for a given `file_name` (more or less the key variable for identifying calls).

External dependencies
--------------------
- PostgreSQL
  - `bs_linguistics.ner_class_alt_7`
  - `bs_linguistics.ner_class_alt_4`
- Stanford NER
  - Note that for the code to run, the Stanford NER tagger referenced in `findner.py` (currently we have `stanford_path = 'stanford-ner-2015-12-09'`) should be present in the `proper_nouns` directory.
  On a Unix system, this can be accomplished by running the following from the `proper_nouns` directory:
  ```bash
  wget http://nlp.stanford.edu/software/stanford-ner-2015-12-09.zip
  unzip stanford-ner-2015-12-09.zip
  ```
- Java 1.8 (or later)
- Python 3.x
  - The code also depends on a fairly recent version of the [Python NLTK](http://www.nltk.org/news.html) (it has been tested with version 3.2.1, which is the current version as of June 27, 2016). RCC has the NLTK installed.
  - SQLAlchemy
  - pandas
  - psycopg2
  
  See [here](https://github.com/azakolyukina/bs_linguistics/blob/master/proper_nouns/README.md) for details on these data and code.

### Finance abbreviations

Both `4 class` and `7 class` models for both `ner_organization` and `ner_location` contain some finance abbreviations such as `EPS` or `EBIT`. The list of finance abbreviations with total counts above `0.10%` of non-missing tags are in google sheet [ner_to_exclude](https://docs.google.com/spreadsheets/d/1VjNgTKUYRvbM0RbAll6ZuXl9zH64gq6c-_qDqWjQtfY/edit#gid=1888114586). This list is constructed as follows:
* for `ner_7class_q`, examine `ner_organization` and `ner_location` with total count above `0.10%` of non-missing tags, i.e., above 630 for `ner_organization` and above 480 for `ner_location`

```sql
SELECT count(*) FROM bs_linguistics.ner_7class_q
	WHERE ner_organization IS NOT NULL
630261

SELECT count(*) FROM bs_linguistics.ner_7class_q
	WHERE ner_location IS NOT NULL
483010
```
* perform Google search to confirm if this abbreviation stands for `regulator` or `finance`
* if an abbreviation possibly denotes `regulator` or `finance` term, perform context search in random questions to confirm the meaning, e.g., 
```sql
SELECT DISTINCT file_name, q_num, question, ner_organization 
	FROM bs_linguistics.ner_7class_q
	WHERE ner_organization[1] = 'EPS'
```
* drop all *not* in `regulator` or `finance`

Overall, none of finance abbreviations takes more than 1% of `ner` tags. Even if we do not exclude them, it won't be a big deal. This manual clean-up list just to make sure that we pay attention to the top ones. 

Table `bs_linguistics.ner_to_exclude` stores the financial abbreviations to be excluded.

**Note:** We should exclude `finance` abbreviations from `ner_organization`- or `ner_location`- based features.
