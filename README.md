# Speechcatcher-data

Tools and scripts to mass transcribe audio data with Whisper and to generate datasets for ASR training datasets in Kaldi format.

# Data server

The data server allows to store and access transcripts. For example when mass downloading and transcribing podcasts in parallel, it can feed workers with new episodes. The output transcripts can then be stored in a centralized database while there may be many worker nodes on different computers that transcribe the data.

## Install requirements

   virtualenv -p python3.10 speechcatcher_data_env
   source speechcatcher_data_env/bin/activate
   pip3 install -r requirements.txt  

## How to create the Postgres schema

Create a new user and database, e.g. speechcatcher and then:

   sudo -u speechatcher psql -d speechcatcher < schema.psql

## Create the speechcatcher database and user

To create the speechcatcher database and user, log into postgres with:

   sudo -u postgres psql

and execute the following commands (you should change the password):

   CREATE USER speechcatcher WITH PASSWORD 'yourpassword42';
   CREATE DATABASE speechcatcher;
   GRANT ALL PRIVILEGES ON DATABASE speechcatcher TO speechcatcher;
   \q

## Config.yaml

You need to create a config.yaml to make a few settings, like the location of the downloaded data. Then you need to make this folder available with https:// URLs for the worker nodes too, for instance with nginx (can also be on your local network).

   cp config.yaml.sample config.yaml
   vim config.yaml #and setup your database user and pw, api key etc.

## Start the data server

   cd data_server
   ./start_wsgi.sh

## How to crawl audio data

Go to ./podcasts and follow the instructions there to crawl audio data.

There is some incomplete work on using and crawling TEDX data too, this is mainly for English. See ./tedx.

## Start transcribing

Once you have crawled some data, you can start transcribing it. You can also do this in parallel while downloading more data.

## Setup worker node

With a pytorch cloud instance for instance, you can setup a worker node quickly with: 

   sudo apt-get install -y wget screen vim htop
   pip3 install git+https://github.com/openai/whisper psycopg2-binary requests
   git clone https://github.com/speechcatcher-asr/speechcatcher-data
   
Then setup config.yaml or simply copy it from your server:

   vim config.yaml

You can now start the worker node with:

   CUDA_VISIBLE_DEVICES=0 python3 worker.py   

In case you have more than one GPU, simply use the CUDA_VISIBLE_DEVICES variable to assign workers to GPUs:

   CUDA_VISIBLE_DEVICES=1 python3 worker.py
   ...
   CUDA_VISIBLE_DEVICES=n python3 worker.py 

Note that you can start with the next steps before completing transcribing all of your data and create bigger and bigger datasets as you transcribe more data. 
Workers will randomly sample authors and then episodes from that auther. This means that you can create and export datasets early on that are diverse enough to start ASR training and scale it later.

You can use the html_stats.py in podcasts to generate a html page that shows you the transcription progress w.r.t. your complete dataset.

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

