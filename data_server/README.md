# Data server

The data server allows to store and access transcripts. For example when mass downloading and transcribing podcasts in parallel, it can feed workers with new episodes. The output transcripts can then be stored in a centralized database while there may be many worker nodes on different computers that transcribe the data.

## How to create the Postgres schema

Create a new user and database, e.g. speechcatcher and then:

sudo -u speechatcher psql -d speechcatcher < schema.psql

## How crawl audio data

Go to ../podcasts and follow the instructions there to 

## Sanity check

Before generating a Kaldi/Espnet compatible dataset, you should run the sanity check script:

python3 sanity_check.py ~/podcasts/de/vtts/

This will sandbox vtt files in ~/podcasts/de/vtts/ that look like they are corrupted. With Whisper models the output sometimes contains hallucinated repetition loops and this script checks for files that only contain very limited vocabularly relative to the size of the file.

## Create the dataset

python3 create_dataset.py

This creats a dataset in Kaldi format, but it will be unsorted. There are further scripts to sort the data by utterance ids and make them compatible with the kaldi dataset validation script, see below:

## Use the Espnet speechcatcher recipe

The Espnet speechcatcher recipe in <todo> contains further utility scripts to refine and validate the data. 

