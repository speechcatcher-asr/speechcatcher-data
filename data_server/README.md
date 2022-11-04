# Data server

The data server allows to store and access transcripts. For example when mass downloading and transcribing podcasts in parallel, it can feed workers with new episodes. The output transcripts can then be stored in a centralized database while there may be many worker nodes on different computers that transcribe the data.

## How to create the Postgres schema

Create a new user and database, e.g. speechcatcher and then:

sudo -u speechatcher psql -d speechcatcher < schema.psql
