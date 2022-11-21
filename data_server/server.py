import argparse
import flask
from flask import Flask, jsonify
from werkzeug.serving import WSGIRequestHandler

from utils import load_config, connect_to_db  

p_connection, p_cursor = None, None

app = Flask(__name__)
api_version = '/apiv1'

@app.route(api_version + '/get_work/<language>', methods=['GET'])
def get_work(language):
   
    #CREATE TABLE IF NOT EXISTS podcasts (podcast_episode_id serial PRIMARY KEY, podcast_title TEXT, episode_title TEXT, published_date TEXT, retrieval_time DECIMAL, authors TEXT, language|
#     VARCHAR(16), description TEXT, keywords TEXT, episode_url TEXT, episode_audio_url TEXT, cache_audio_url TEXT, cache_audio_file TEXT, transcript_file TEXT, duration REAL, type VARCHAR|
 #    (64), episode_json JSON);

    return_dict = {'error':'SQL query did not execute'}
    # first sample an author (with empty transcripts)

    p_cursor.execute('SELECT authors,count(podcast_episode_id) from podcasts '
                     'WHERE transcript_file=%s and language=%s GROUP BY authors ORDER BY RANDOM() '
                     'LIMIT 1', ('',language) )
    record = p_cursor.fetchone()

    print(record)

    if record is not None and len(record) > 0:
        authors, count_episodes = record

        p_cursor.execute('SELECT podcast_episode_id, episode_title, authors, language, episode_audio_url, cache_audio_url, '
                            'cache_audio_file, transcript_file FROM podcasts '
                            'WHERE transcript_file=%s and language=%s and authors=%s ORDER BY RANDOM() '
                            'LIMIT 1', ('',language, authors) )

        record = p_cursor.fetchone()

        if record is not None and len(record) > 0:
            print(record)
            podcast_episode_id, episode_title, authors, language, episode_audio_url, cache_audio_url, cache_audio_file, transcript_file = record
            return_dict = {"podcast_episode_id":podcast_episode_id, "episode_title":episode_title, "authors":authors,
                            "language":language, "episode_audio_url":episode_audio_url, "cache_audio_url":cache_audio_url,
                            "cache_audio_file":cache_audio_file, "transcript_file":transcript_file}
        else:
            return_dict = {'error':'No episodes without transcription for author: '+authors}

    else:
        return_dict = {'error':'No episodes left without transcriptions.'}

    return jsonify(return_dict)

@app.route(api_version + '/register_wip/<wid>', methods=['GET'])
def register_wip(wid):
    return

@app.route(api_version + '/upload_result/<wid>', methods=['GET'])
def upload_result(wid):
    return

@app.route(api_version + '/cancel_work/<wid>', methods=['GET'])
def cancel_work(wid):
    return

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Work distribution server for mass transcription jobs')
    parser.add_argument('-l', '--listen-host', default='127.0.0.1', dest='host', help='Host address to listen on.')
    parser.add_argument('-p', '--port', default=6000, dest='port', help='Port to listen on.', type=int)
    parser.add_argument('--debug', dest='debug', help='Start with debugging enabled',
                        action='store_true', default=False)

    args = parser.parse_args()

    config = load_config()
    p_connection, p_cursor = connect_to_db(database=config["database"], user=config["user"], password=config["password"], host=config["host"], port=config["port"])

    if args.debug:
        app.debug = True

    WSGIRequestHandler.protocol_version = 'HTTP/1.1'
    app.run(host=args.host, port=args.port, threaded=True, use_reloader=False, use_debugger=False)
    #,  ssl_context='adhoc')

