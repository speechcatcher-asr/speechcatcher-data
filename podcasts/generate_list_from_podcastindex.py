import sqlite3
import random

# The file "podcastindex_feeds.db" can be obtained from https://podcastindex.org/
conn = sqlite3.connect('podcastindex_feeds.db')
c = conn.cursor()

# Note, podcastindex_feeds.db has "en" as lang but also "en-us", "en-uk" etc.
lang = 'en'

# Select urls where the language *is* {lang} (e.g. "en-us")
#c.execute(f"SELECT url FROM podcasts WHERE lower(language)='{lang}'")

# Select urls where the language *starts with* {lang} (e.g. "en")
c.execute(f"SELECT url FROM podcasts WHERE lower(language) LIKE '{lang}%'")

# Output urls to a file list in random order
urls = [row[0] for row in c.fetchall()]
random.shuffle(urls)

with open(f'podcast_lists/{lang}_index_feeds.txt', 'w') as f:
    for url in urls:
        f.write(url + '\n')

        # Close the database connection
        conn.close()
