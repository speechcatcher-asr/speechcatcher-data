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

The data can be used to train end-to-end ASR models. Punctuation isn't removed in the dataset creation and be used to train models that output them directly without a reconstruction step. Pre-trained speechcatcher models are currently trained with Espnet. The Espnet [speechcatcher recipe](https://github.com/speechcatcher-asr/espnet/tree/egs2-speechcatcher-de/egs2/speechcatcher/asr1) contains further utility scripts to refine and validate the Kaldi-formatted data.

The scripts are in [egs2/speechcatcher/asr1](https://github.com/speechcatcher-asr/espnet/tree/egs2-speechcatcher-de/egs2/speechcatcher/asr1). You should run the following first:

    local/fix_wav.scp.py
    mv data/train/wav.scp data/train/wav.scp.backup
    mv data/train/new_wav.scp data/train/wav.scp

This fixes issues with entries in wav.scp that are obsolete, because they don't have matching  (can happen due to filtering):

    e.g. [Lengths are /tmp/kaldi.Jrcr/recordings=36572 versus /tmp/kaldi.Jrcr/recordings.wav=36579]

Then run sort data twice per set (you need to change the set at the beginning of the file, e.g. DIR="data/dev"). The following warning messages would be normal:

    root@C.5776893:~/espnet/egs2/speechcatcher/asr1$ ./sort_data.sh 
    utils/validate_data_dir.sh: spk2utt and utt2spk do not seem to match
    root@C.5776893:~/espnet/egs2/speechcatcher/asr1$ ./sort_data.sh 
    mv: cannot stat 'data/dev/utt2dur': No such file or directory
    utils/validate_data_dir.sh: Successfully validated data-directory data/dev

Sort_data.sh also runs validation with the validate_data_dir.sh script - it might alert you to other problems as well, like illegal unicode characters:
    
    utils/validate_text.pl: The line for utterance 9351fc5a7a3dddaaa101_9e497d4a36bbf3cb6a09_0000004 contains disallowed Unicode whitespaces

You'd need to fix these manually and the validate script should tell you what ID caused the problem. Then rerun the sort data script to verify thet the data dir validates. The training will otherwise refuse to run, if the dataset can't be validated!

If you need to remove entire files or IDs, you can use the ./local/remove_id.sh script to remove them.

Note, wav.scp with ffmpeg piping also support https links (if you want to make a network compatible file). You can simply replace your local path, say /var/www/ with https:// using vim:

    :%s#/var/www/#https://#g

