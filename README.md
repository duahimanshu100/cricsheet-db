cricsheet-db
============

[![Build Status](https://travis-ci.org/berianjames/cricsheet-db.svg?branch=master)](https://travis-ci.org/berianjames/cricsheet-db)
[![MIT License](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/berianjames/cricsheet-db/blob/master/LICENSE)

A tiny data model and reader for digitised historical cricket scoresheets, in honour of @cricsheet.
Load data from yaml to RDBMS(Postgres)

Input: the Yaml scoresheets [found here](https://cricsheet.org/downloads/).

Output: a sqlite or Postgres database


Execute
============

1. Run Postgres via docker --> `docker-compose -f docker/postgres/local.yml up`
2. Create Virtualenv --> `virtualenv venv`
3. Activate env --> `source venv/bin/activate`
4. Add some of the yaml files from cricsheet website in data directory.
5. Install requirements --> `pip install -r requirements.txt`
6. Run python file --> `python cricket_db.py`
