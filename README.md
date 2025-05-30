# Speechcatcher-data

Speechcatcher-data is a collection of tools and scripts to mass transcribe audio data with Whisper or similar big teacher models and to generate datasets for ASR training datasets in Kaldi format. With Speechcatcher-data you can easily crawl and process very large quantities of raw speech data. You can then use the generated transcriptions to export it as an ASR dataset that you use to train new (and maybe more efficient!) student models. Currently, the focus is on generating training data for single language ASR models. Processing should be language independant, you may only need to make a few changes to the unicode character filtering step. 

Here is an overview over the architecture:

![Speechcatcher-data architecture](https://raw.githubusercontent.com/speechcatcher-asr/speechcatcher-data/main/architecture.svg)

# Data server

The data server allows to store and access transcripts. For example when mass downloading and transcribing podcasts in parallel, it can feed workers with new episodes. The output transcripts can then be stored in a centralized database while there may be many worker nodes on different computers that transcribe the data.

## Install requirements

    python3 -m venv venv
    source venv/bin/activate
    pip3 install -r requirements.txt  

## Create the speechcatcher database and user

To create the speechcatcher database and user, log into postgres with:

    sudo -u postgres psql

and execute the following commands (you should change the password):

    CREATE USER speechcatcher WITH PASSWORD 'yourpassword42';
    CREATE DATABASE speechcatcher;
    ALTER DATABASE speechcatcher OWNER TO speechcatcher;
    GRANT ALL PRIVILEGES ON DATABASE speechcatcher TO speechcatcher;
    GRANT ALL PRIVILEGES ON SCHEMA public TO speechcatcher;
    \q

Then edit the postgresql.conf file. The file is usually located in /etc/postgresql/<version>/main/postgresql.conf. Replace <version> with your PostgreSQL version number (e.g., 12, 13, etc.):

    sudo vim /etc/postgresql/<version>/main/postgresql.conf

Find the line that starts with listen_addresses and change it to:

    listen_addresses = 'localhost'

This setting allows PostgreSQL to listen for incoming connections on localhost.

Now edit the pg_hba.conf file to allow client authentication for the speechcatcher user. The file is usually located in /etc/postgresql/<version>/main/pg_hba.conf:

    sudo vim /etc/postgresql/<version>/main/pg_hba.conf

Add the following line to the end of the file to allow the speechcatcher user to connect using a password:

    host    speechcatcher    speechcatcher    127.0.0.1/32    md5

This line specifies that the speechcatcher user can connect to the speechcatcher database from localhost using MD5 password authentication.

## How to create the Postgres schema

If you created a new user and database like above then:

    psql -h 127.0.0.1 -U speechcatcher -d speechcatcher < data_server/schema.psql

## Config.yaml

You need to create a config.yaml to make a few settings, like the location of the downloaded data. Then you need to make this folder available with https:// URLs for the worker nodes too, for instance with nginx (can also be on your local network).

    cp config.yaml.sample config.yaml
    vim config.yaml #and setup your database user and pw, api key etc.

## Start the data server

    cd data_server
    ./start_wsgi.sh

## How to crawl audio data

To download podcast data you can use the simple_podcast_downloader.py script. You need to configure podcast_language, download_destination_folder and download_destination_url as well as the db connection in config.yaml.

    cd podcasts
    python3 simple_podcast_downloader.py your_rss_feed_list_change_me

Change your_rss_feed_list_change_me to one of the lists in podcast_lists or use generate_list_from_podcastindex.py and podcastindex_feeds.db to generate a new one. You can download podcastindex_feeds.db from https://podcastindex.org/:

    wget https://public.podcastindex.org/podcastindex_feeds.db.tgz

Note that the podcast downloader script can also be resumed - when you rerun it, it checks if you've already downloaded a particular episode.

## Start transcribing

Once you have crawled some data, you can start transcribing it. You can also do this in parallel while downloading more data.

## Setup worker nodes

With a pytorch cloud instance for instance, you can setup a worker node quickly with: 

    sudo apt-get install -y wget screen vim htop python3.12-venv git ffmpeg
    git clone https://github.com/speechcatcher-asr/speechcatcher-data
    cd speechcatcher-data
    python3 -m venv venv
    . venv/bin/activate
    pip3 install --pre torch torchvision torchaudio --index-url https://download.pytorch.org/whl/nightly/cu128
    pip3 install git+https://github.com/openai/whisper psycopg2-binary requests faster-whisper ffmpeg-python
   
Then setup config.yaml or simply copy it from your server:

    vim config.yaml

You can now start the worker node with (replace 'de' with the langauge you want to transcribe):

    CUDA_VISIBLE_DEVICES=0 python3 worker.py
    # wait for the model download to finish before starting more workers on the same machine!

In case you have more than one GPU, simply use the CUDA_VISIBLE_DEVICES variable to assign workers to GPUs:

    CUDA_VISIBLE_DEVICES=1 python3 worker.py
    ...
    CUDA_VISIBLE_DEVICES=n python3 worker.py

You can start two processes per 3090/4090 GPU with 24GB and this saturates the GPU better. Note that you can start with the next steps before completing transcribing all of your data and create bigger and bigger datasets as you go along and transcribe more data. 
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

This fixes issues with entries in wav.scp that are obsolete, because they don't have matching utterance ids (can happen due to filtering):

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

Note, wav.scp with ffmpeg piping also supports HTTP/HTTPS links (if you want to make a network compatible file and keep the dataset on a remote server). You can simply replace your local path, say /var/www/ with https:// using vim:

    :%s#/var/www/#https://#g

