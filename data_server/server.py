import argparse
import flask
import traceback
import sys

from flask import Flask, jsonify, request
from werkzeug.serving import WSGIRequestHandler

from utils import load_config, connect_to_db, ensure_dir  

p_connection, p_cursor = None, None

# This flask server utility can distrbiute untranscribed episodes from the db to worker clients that transcribe it.

app = Flask(__name__)
api_version = '/apiv1'
api_secret_key = ''
vtt_dir = ''
sql_table = 'podcasts'
sql_table_ids = 'podcast_episode_id'

transcript_file_replace_prefix = '/var/www/'

podcast_columns = 'podcast_episode_id, podcast_title, episode_title, published_date, retrieval_time, ' \
            'authors, language, description, keywords, episode_url, episode_audio_url, ' \
                        'cache_audio_url, cache_audio_file, transcript_file, duration'
podcast_columns_list = podcast_columns.split(', ')

def make_local_url(my_url, config):
    if 'replace_local_audio_url' in config:
        try:
            a, b = config['replace_local_audio_url'].split('->')
            return my_url.replace(a, b)  # Make sure to return the modified URL
        except Exception as e:
            print('Warning, something went wrong trying to make local url out of:', my_url)
            print('Error:', str(e))
            print('Traceback:', traceback.format_exc())
            print('Using original link instead')
            return my_url
    else:
        print('Warning: replace_local_audio_url not in config, returning unmodified local link.')
        return my_url

# Returns all podcast titles
@app.route(api_version + '/get_podcast_list/<language>/<api_access_key>', methods=['GET'])
def get_podcast_list(language, api_access_key):
    if api_secret_key != api_access_key:
        return jsonify({'success':False, 'error':'api_access_key invalid'})
    

    try:
        p_cursor.execute(f'SELECT distinct(podcast_title), count(podcast_episode_id) from podcasts '
                     'WHERE language=%s GROUP BY podcast_title', (language,) )

        records = p_cursor.fetchall()
    except:
        traceback.print_exc()
        return_dict = {'success':False, 'error':'SQL query did not execute'}
        return jsonify(return_dict)

    podcast_titles = [{'title':record[0], 'count':record[1]} for record in records] 

    return jsonify(podcast_titles)

# Get list of all podcast episodes from a podcast title with available vtt files
@app.route(api_version + '/get_episode_list/<api_access_key>', methods=['POST'])
def get_episode_list(api_access_key):
    if api_secret_key != api_access_key:
        return jsonify({'success':False, 'error':'api_access_key invalid'})

    podcast_title = request.values.get('podcast_title')

    assert(podcast_title is not None)

    try:
        p_cursor.execute(f'SELECT {podcast_columns} from podcasts '
            'WHERE podcast_title=%s and transcript_file<>%s', (podcast_title, '') )

        records = p_cursor.fetchall()
    except:
        traceback.print_exc()
        return_dict = {'success':False, 'error':'SQL query did not execute'}
        return jsonify(return_dict)

    return_list = []
    for record in records:
        record_dict = dict(zip(podcast_columns_list,record))
        return_list.append(record_dict)
        record_dict['transcript_file_url'] = record_dict['transcript_file'].replace(transcript_file_replace_prefix, 'https://')

    return jsonify(return_list)

# Get list of all podcast episodes with available vtt files
# Note: can probably be refactored with the above function
@app.route(api_version + '/get_every_episode_list/<api_access_key>', methods=['GET'])
def get_every_episode_list(api_access_key):
    if api_secret_key != api_access_key:
        return jsonify({'success':False, 'error':'api_access_key invalid'})

    try:
        p_cursor.execute(f'SELECT {podcast_columns} from podcasts '
            'WHERE transcript_file<>%s', ('',) )
        records = p_cursor.fetchall()

    except:
        traceback.print_exc()
        return_dict = {'success':False, 'error':'SQL query did not execute'}
        return jsonify(return_dict)

    return_list = []
    for record in records:
        record_dict = dict(zip(podcast_columns_list,record))
        return_list.append(record_dict)
        record_dict['transcript_file_url'] = record_dict['transcript_file'].replace(transcript_file_replace_prefix, 'https://')

    return jsonify(return_list)


# Samples a new untranscribed episode from the db and sends the result as JSON
# to have more diversity early on, we first sample an author and then a random episode from that author
# this helps to not over sample from the authors with the most episodes early on
@app.route(api_version + '/get_work/<language>/<api_access_key>', methods=['GET'])
def get_work(language, api_access_key):
    # Security check for API access key
    if api_secret_key != api_access_key:
        return jsonify({'success': False, 'error': 'API access key invalid'}), 401

    # Validate language input
    if not language.isalpha():
        return jsonify({'success': False, 'error': 'Invalid language format'}), 400

    try:
        # Sample an author with untranscribed episodes in the given language
        p_cursor.execute("""
            SELECT authors, count(%s) as episode_count FROM podcasts
            WHERE transcript_file = %s AND language = %s
            GROUP BY authors
            ORDER BY RANDOM()
            LIMIT 1
        """, (sql_table_ids, '', language))
        author_record = p_cursor.fetchone()
        
        print("Language:", language)
        print("Sampled author:", author_record)

        if author_record:
            # Sample a random untranscribed episode from the sampled author
            p_cursor.execute(f"""
                SELECT {sql_table_ids}, episode_title, authors, language, episode_audio_url, cache_audio_url, 
                cache_audio_file, transcript_file, duration FROM {sql_table}
                WHERE transcript_file = %s AND language = %s AND authors = %s
                ORDER BY RANDOM()
                LIMIT 1
            """, ('', language, author_record['authors']))
            episode_record = p_cursor.fetchone()
            
            if episode_record:
                table_id, episode_title, authors, language, episode_audio_url, cache_audio_url, cache_audio_file, transcript_file, duration = episode_record
                return jsonify({
                    'wid': table_id,
                    'episode_title': episode_title,
                    'authors': authors,
                    'language': language,
                    'episode_audio_url': episode_audio_url,
                    'cache_audio_url': cache_audio_url,
                    'local_cache_audio_url': make_local_url(cache_audio_url, config),
                    'cache_audio_file': cache_audio_file,
                    'transcript_file': transcript_file,
                    'duration': duration,
                    'success': True
                })
            else:
                return jsonify({'success': False, 'error': f'No episodes without transcription for author: {author_record["authors"]}'}), 404
        else:
            return jsonify({'success': False, 'error': f'No episodes left without transcriptions for language {language}.'}), 404
    except Exception as e:
        app.logger.error('Unexpected error:', exc_info=True)
        return jsonify({'success': False, 'error': 'An unexpected error occurred'}), 500


@app.route(api_version + '/get_work_batch/<language>/<api_access_key>/<int:n>', methods=['GET'])
def get_work_batch(language, api_access_key, n):
    if api_secret_key != api_access_key:
        return jsonify({'success': False, 'error': 'api_access_key invalid'}), 401

    # Fetch optional min_duration from query parameters
    min_duration = request.args.get('min_duration', default=0, type=float)

    # Fetch up to n tasks with specified minimum duration and similar durations
    p_cursor.execute(f"""
        SELECT {sql_table_ids}, episode_title, authors, language, episode_audio_url, cache_audio_url, cache_audio_file, transcript_file, duration
        FROM {sql_table}
        WHERE transcript_file=%s and language=%s and duration >= %s
        ORDER BY duration, RANDOM()
        LIMIT %s
    """, ('', language, min_duration, n))

    tasks = []
    records = p_cursor.fetchall()

    if records:
        for record in records:
            if record:
                table_id, episode_title, authors, language, episode_audio_url, cache_audio_url, cache_audio_file, transcript_file, duration = record
                tasks.append({
                    'wid': table_id,
                    'episode_title': episode_title,
                    'authors': authors,
                    'language': language,
                    'episode_audio_url': episode_audio_url,
                    'cache_audio_url': cache_audio_url,
                    'local_cache_audio_url': make_local_url(cache_audio_url, config),
                    'cache_audio_file': cache_audio_file,
                    'transcript_file': transcript_file,
                    'duration': duration,
                    'success': True
                })
        return jsonify({'tasks': tasks, 'success': True})
    else:
        return jsonify({'success': False, 'error': 'No sufficient tasks available'}), 404

# Client worker registers that he is working on the transcript. Sets transcript_file = 'in_progress' in the db.
@app.route(api_version + '/register_wip/<wid>/<api_access_key>', methods=['GET'])
def register_wip(wid, api_access_key):

    if api_secret_key != api_access_key:
        return jsonify({'success': False, 'error':'api_access_key invalid'})

    p_cursor.execute(f'SELECT {sql_table_ids}, transcript_file FROM {sql_table} WHERE {sql_table_ids}=%s', (str(wid),))
    record = p_cursor.fetchone()

    table_id, transcript_file = record

    if transcript_file == 'in_progress':
        return jsonify({'success': False, 'error': str(wid)+' already in progress'})
    elif transcript_file != '':
        return jsonify({'success': False, 'error': str(wid)+' already transcribed'})

    p_cursor.execute(f"UPDATE {sql_table} SET transcript_file = 'in_progress' WHERE {sql_table_ids}=%s" , (str(wid),))
    p_connection.commit()

    return jsonify({'success': True})

@app.route(api_version + '/register_wip_batch/<api_access_key>', methods=['POST'])
def register_wip_batch(api_access_key):
    if api_secret_key != api_access_key:
        return jsonify({'success': False, 'error':'api_access_key invalid'})

    # Retrieve the list of wids from the POST request body
    wids = request.json.get('wids')
    if not wids:
        return jsonify({'success': False, 'error': 'No wids provided'})

    try:
        # Begin a transaction to ensure atomicity
        p_cursor.execute('BEGIN')

        # Cast wids to integers and check current status of each wid
        int_wids = list(map(int, wids))  # Ensure wids are integers
        p_cursor.execute(f"""
            SELECT {sql_table_ids}, transcript_file
            FROM {sql_table}
            WHERE {sql_table_ids} = ANY(%s)
        """, (int_wids,))

        wip_conflict = []
        already_transcribed = []
        to_update = []

        records = p_cursor.fetchall()
        for record in records:
            table_id, transcript_file = record
            if transcript_file == 'in_progress':
                wip_conflict.append(str(table_id))
            elif transcript_file != '':
                already_transcribed.append(str(table_id))
            else:
                to_update.append(table_id)

        if wip_conflict or already_transcribed:
            return jsonify({
                'success': False,
                'error': {
                    'already_in_progress': wip_conflict,
                    'already_transcribed': already_transcribed
                }
            })

        # Update the status to 'in_progress' for all applicable wids
        if to_update:
            p_cursor.execute(f"""
                UPDATE {sql_table}
                SET transcript_file = 'in_progress'
                WHERE {sql_table_ids} = ANY(%s)
            """, (to_update,))
            p_connection.commit()
            return jsonify({'success': True, 'updated': to_update})
        else:
            return jsonify({'success': False, 'error': 'No eligible work IDs to update'})
        
    except Exception as e:
        p_cursor.execute('ROLLBACK')
        return jsonify({'success': False, 'error': str(e)})

# Client worker uploads the resulting vtt file. Sets transcript_file to the path of the uploaded file in the db.
@app.route(api_version + '/upload_result/<wid>/<api_access_key>', methods=['POST'])
def upload_result(wid, api_access_key):
    if api_secret_key != api_access_key:
        return jsonify({'success': False, 'error':'api_access_key invalid'})

    # result upload needs a file
    if 'file' not in request.files:
        return jsonify({'success': False, 'error':'no file found in POST request'})

    p_cursor.execute(f'SELECT {sql_table_ids}, transcript_file, cache_audio_file, episode_audio_url FROM {sql_table} WHERE {sql_table_ids}=%s', (str(wid),))
    record = p_cursor.fetchone()
    table_id, transcript_file, cache_audio_file, episode_audio_url = record

    # Check if model parameter is present
    model_name = request.form.get('model', None)

    if transcript_file != 'in_progress':
        return jsonify({'success': False, 'error': str(wid)+' not in progress'})

    if cache_audio_file == '':
        return jsonify({'success': False, 'error': str(wid)+' does not have a cache file, this is currently unsupported'})

    myfile = request.files['file']

    if myfile:
        # Get the directory and filename to store the vtt file
        # The config variable can use {source_dir} as a variable for the directory where the source file is stored
        # We append .vtt to the input filename

        cache_audio_file_split = cache_audio_file.split('/')
        source_dir = '/'.join(cache_audio_file_split[:-1])
        full_dir = vtt_dir.replace('{source_dir}', source_dir) + '/'
        ensure_dir(full_dir)
        full_filename = full_dir + cache_audio_file_split[-1] + '.vtt'
        print('Saving vtt file to:', full_filename)
        myfile.save(full_filename)

        # Update the transcript_file and model columns
        if model_name:
            p_cursor.execute(f'UPDATE {sql_table} SET transcript_file=%s, model=%s WHERE {sql_table_ids}=%s',
                             (full_filename, model_name, str(wid)))
        else:
            p_cursor.execute(f'UPDATE {sql_table} SET transcript_file=%s WHERE {sql_table_ids}=%s',
                             (full_filename, str(wid)))

        p_connection.commit()
    else:
        return jsonify({'success': False, 'error': str(wid)+' could not access upload file'})

    return jsonify({'success': True})

@app.route(api_version + '/upload_result_batch/<api_access_key>', methods=['POST'])
def upload_result_batch(api_access_key):
    if api_secret_key != api_access_key:
        return jsonify({'success': False, 'error': 'api_access_key invalid'})

    # Retrieve the JSON payload containing wids and file paths
    results = request.json.get('results')
    if not results:
        return jsonify({'success': False, 'error': 'No results provided'})

    try:
        # Start a transaction to ensure atomicity
        p_cursor.execute('BEGIN')

        successful_uploads = []
        errors = []

        for result in results:
            wid = result.get('wid')
            file_path = result.get('file_path')
            model_name = result.get('model', None)

            # Ensure WID is an integer
            try:
                wid_int = int(wid)
            except ValueError:
                errors.append({'wid': wid, 'error': 'Invalid Work ID format'})
                continue

            # Fetch the current status and file details
            p_cursor.execute(f"""
                SELECT {sql_table_ids}, transcript_file, cache_audio_file, episode_audio_url
                FROM {sql_table}
                WHERE {sql_table_ids}=%s
            """, (wid_int,))
            record = p_cursor.fetchone()

            if not record:
                errors.append({'wid': wid, 'error': 'Work ID not found'})
                continue

            table_id, transcript_file, cache_audio_file, episode_audio_url = record

            if transcript_file != 'in_progress':
                errors.append({'wid': wid, 'error': 'Work ID not in progress'})
                continue

            if cache_audio_file == '':
                errors.append({'wid': wid, 'error': 'No cache file, currently unsupported'})
                continue

            # Update the transcript_file and model columns
            if model_name:
                p_cursor.execute(f"""
                    UPDATE {sql_table}
                    SET transcript_file=%s, model=%s
                    WHERE {sql_table_ids}=%s
                """, (file_path, model_name, wid_int))
            else:
                p_cursor.execute(f"""
                    UPDATE {sql_table}
                    SET transcript_file=%s
                    WHERE {sql_table_ids}=%s
                """, (file_path, wid_int))

            successful_uploads.append({'wid': wid, 'file_path': file_path})

        if errors:
            p_cursor.execute('ROLLBACK')
            return jsonify({'success': False, 'errors': errors})

        p_connection.commit()
        return jsonify({'success': True, 'uploaded': successful_uploads})

    except Exception as e:
        p_cursor.execute('ROLLBACK')
        return jsonify({'success': False, 'error': str(e)})

# Cancel work in progress. Sets transcript_file = '' in the db and makes it available for sampling again.
# Will throw an error if transcript_file wasn't previously set to in_progress.
@app.route(api_version + '/cancel_work/<wid>/<api_access_key>', methods=['GET'])
def cancel_work(wid, api_access_key):
    if api_secret_key != api_access_key:
        return jsonify({'error':'api_access_key invalid'})

    p_cursor.execute(f'SELECT {sql_table_ids}, transcript_file FROM {sql_table} WHERE {sql_table_ids}=%s', (str(wid),))
    record = p_cursor.fetchone()

    table_id, transcript_file = record

    if transcript_file != 'in_progress':
        if transcript_file != '':
            return jsonify({'success': False, 'error': str(wid)+' already transcribed'})
        return jsonify({'success': False, 'error': str(wid)+' not in progress'})

    p_cursor.execute(f"UPDATE {sql_table} SET transcript_file = '' WHERE {sql_table_ids}=%s" , (str(wid),))
    p_connection.commit()

    return jsonify({'success': True})

@app.route(api_version + '/cancel_work_batch/<api_access_key>', methods=['POST'])
def cancel_work_batch(api_access_key):
    if api_secret_key != api_access_key:
        return jsonify({'success': False, 'error': 'api_access_key invalid'})

    # Retrieve the list of wids from the POST request body
    wids = request.json.get('wids')
    if not wids:
        return jsonify({'success': False, 'error': 'No wids provided'})

    try:
        # Start a transaction to ensure atomicity
        p_cursor.execute('BEGIN')

        # Cast wids to integers
        int_wids = list(map(int, wids))

        # Fetch the current status of each wid to ensure they are all in 'in_progress'
        p_cursor.execute(f"""
            SELECT {sql_table_ids}, transcript_file
            FROM {sql_table}
            WHERE {sql_table_ids} = ANY(%s)
        """, (int_wids,))

        records = p_cursor.fetchall()
        update_candidates = []
        errors = []

        for record in records:
            table_id, transcript_file = record
            if transcript_file != 'in_progress':
                if transcript_file == '':
                    errors.append({'wid': table_id, 'error': 'Work ID not in progress'})
                else:
                    errors.append({'wid': table_id, 'error': 'Work ID already transcribed'})
            else:
                update_candidates.append(table_id)

        if errors:
            return jsonify({'success': False, 'errors': errors})

        # Update the status to '' for all applicable wids
        if update_candidates:
            p_cursor.execute(f"""
                UPDATE {sql_table}
                SET transcript_file = ''
                WHERE {sql_table_ids} = ANY(%s)
            """, (update_candidates,))
            p_connection.commit()
            return jsonify({'success': True, 'updated': update_candidates})
        else:
            return jsonify({'success': False, 'error': 'No valid wids to update'})

    except Exception as e:
        p_cursor.execute('ROLLBACK')
        return jsonify({'success': False, 'error': str(e)})

# must be outside __main__ for gunicorn
config = load_config()
api_secret_key = config["secret_api_key"]
vtt_dir = config["vtt_dir"]
WSGIRequestHandler.protocol_version = 'HTTP/1.1'
p_connection, p_cursor = connect_to_db(database=config["database"], user=config["user"], password=config["password"], host=config["host"], port=config["port"])

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Work distribution server for mass transcription jobs')
    parser.add_argument('-l', '--listen-host', default='127.0.0.1', dest='host', help='Host address to listen on.')
    parser.add_argument('-p', '--port', default=6000, dest='port', help='Port to listen on.', type=int)
    parser.add_argument('--debug', dest='debug', help='Start with debugging enabled',
                        action='store_true', default=False)

    args = parser.parse_args()

    print('Warning, you are using the builtin flask server. For deployment, you should run a gunicorn server. See start_wsgi.sh')

    if args.debug:
        app.debug = True

    app.run(host=args.host, port=args.port, threaded=True, use_reloader=False, use_debugger=False)
