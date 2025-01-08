import argparse
import sqlite3
import random

def main(database, language, vendor, output_dir):
    # Connect to the database
    conn = sqlite3.connect(database)
    c = conn.cursor()

    # Query for urls where the language starts with the specified language
    query = f"SELECT url FROM podcasts WHERE lower(language) LIKE '{language}%'"
    if vendor and not (vendor=='*' or vendor=='all'):
        query += f" AND lower(url) LIKE '%{vendor}%'"
    c.execute(query)

    # Shuffle and write urls to a file
    urls = [row[0] for row in c.fetchall()]
    random.shuffle(urls)

    with open(f'{output_dir}/{language}_{vendor}_index_feeds.txt', 'w') as f:
        for url in urls:
            f.write(url + '\n')

    # Close the database connection
    conn.close()

if __name__ == "__main__":
    # Setting up argparse to handle command line arguments
    parser = argparse.ArgumentParser(description="Fetch podcast URLs from the database.")
    parser.add_argument("--database", type=str, default="podcastindex_feeds.db",
                        help="Database file to connect to. Default is 'podcastindex_feeds.db'.")
    parser.add_argument("--language", type=str, default="en",
                        help="Language prefix to filter URLs. Default is 'en'.")
    parser.add_argument("--vendor", type=str, default="all",
                        help="Vendor filter for URLs. Default is 'all'.")
    parser.add_argument("--output_dir", type=str, default="podcast_lists",
                        help="Directory to save the output files. Default is 'podcast_lists'.")

    args = parser.parse_args()

    # Run main function with parsed arguments
    main(args.database, args.language, args.vendor, args.output_dir)
