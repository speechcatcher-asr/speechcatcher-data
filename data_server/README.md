# Data server

The data server allows to store and access transcripts. For example when mass downloading and transcribing podcasts in parallel, it can feed workers with new episodes. The output transcripts can then be stored in a centralized database while there may be many worker nodes on different computers that transcribe the data.

## How to create the Postgres schema

Create a new user and database, e.g. speechcatcher and then:

sudo -u speechatcher psql -d speechcatcher < schema.psql

## How to crawl audio data

Go to ../podcasts and follow the instructions there to crawl audio data.

## Sanity check

Before generating a Kaldi/Espnet compatible dataset, you should run the sanity check script:

    python3 sanity_check.py ~/podcasts/de/vtts/

This will sandbox vtt files in ~/podcasts/de/vtts/ that look like they are corrupted. With Whisper models the output sometimes contains hallucinated repetition loops and this script checks for files that only contain very limited vocabularly relative to the size of the file. The script moves these files into a corrupted subdir and marks this in the DB as well..

## Create the dataset

    python3 create_dataset.py

This creats a dataset in Kaldi format, but it will be unsorted. There are further scripts to sort the data by utterance IDs and make them compatible with the kaldi dataset validation script, see below:

## Use the Espnet speechcatcher recipe

The Espnet speechcatcher recipe in <todo> contains further utility scripts to refine and validate the data.

The scripts are in egs2/speechcatcher/asr1. You should run the following:

    local/fix_wav.scp.py

This fixes issues with entries in wav.scp that are obsolete, because they don't have matching  (can happen due to filtering).

Then run sort data twice per set (you need to change the set at the beginning of the file, e.g. DIR="data/dev"). The following warning messages would be normal:

    root@C.5776893:~/espnet/egs2/speechcatcher/asr1$ ./sort_data.sh 
    utils/validate_data_dir.sh: spk2utt and utt2spk do not seem to match
    root@C.5776893:~/espnet/egs2/speechcatcher/asr1$ ./sort_data.sh 
    mv: cannot stat 'data/dev/utt2dur': No such file or directory
    utils/validate_data_dir.sh: Successfully validated data-directory data/dev

Sort_data.sh also runs validation with the validate_data_dir.sh script - it might alert you to other problems as well, like illegal unicode characters:
    
    utils/validate_text.pl: The line for utterance 9351fc5a7a3dddaaa101_9e497d4a36bbf3cb6a09_0000004 contains disallowed Unicode whitespaces

You'd need to fix these manually. Then rerun the sort data script to verify thet the data dir validates.

If you need to remove entire files or IDs, you can use the ./local/remove_id.sh script to remove them.

