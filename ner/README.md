# Stanford NER tagger

The code in this directory (`ner`) processes questions from earnings conference calls to extract named entity relations (NERs) using the [Stanford NER tagger](https://nlp.stanford.edu/software/CRF-NER.shtml).

We use `Stanford NER` with `4 class` and `7 class` models trained using different data (see [here](https://nlp.stanford.edu/software/CRF-NER.shtml) for details on training sets): 
* `4 class:`	Location, Person, Organization, Misc
* `7 class:`	Location, Person, Organization, Money, Percent, Date, Time

Data overview
-----------

There are two tables associated with code in `ner` directory, all found in the `se_features` schema:
1. `ner_class_alt_4` build by code in `ner` directory based on `4 class` model. NER tags are stored in (binary) JSON format.
2. `ner_class_alt_7` build by code in `ner` directory based on `7 class` model. NER tags are stored in (binary) JSON format.

**Note 1:** `4 class` model (and related tables `ner_class_alt_4` and `ner_4class_q`) seems to be less accurate than `7 class` model. 
E.g., `ner_organization` for `4 class` model includes `{"{OK}";23217, "{Hi}";5584, "{Sorry}";4621}` and `7 class` model does not include these as top entries. 

**Note 2:** All records from the `streetevents.speaker_data` table are considered. 

Running code
-----------

### Run `ner/run_NER.py` directly

If a single node suffices for the data to be processed, this is a simple approach. From the `ner` directory, enter the following in the terminal to run with the 4-class `ner-class` (alternative is 7-class):

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
  - `se_features.ner_class_alt_7`
  - `se_features.ner_class_alt_4`
- Stanford NER
  - Note that for the code to run, the Stanford NER tagger referenced in `findner.py` (currently we have `stanford_path = 'stanford-ner-2018-10-16'`) should be present in the `ner` directory.
  On a Unix system, this can be accomplished by running the following from the `ner` directory:
  ```bash
  wget https://nlp.stanford.edu/software/stanford-ner-2018-10-16.zip
  unzip stanford-ner-2018-10-06.zip
  ```
- Java 1.8 (or later)
- Python 3.x
  - The code also depends on a fairly recent version of the [Python NLTK](http://www.nltk.org/news.html) (it has been tested with version 3.2.1, which is the current version as of June 27, 2016). RCC has the NLTK installed.
  - SQLAlchemy
  - pandas
  - psycopg2
