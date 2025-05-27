import glob
import argparse
from tqdm import tqdm
from somajo import SoMaJo
import subprocess
import os
import re
import json
import gzip
from utils import *

sql_table = 'podcasts'

def get_duration(input_video):
    cmd = ["ffprobe", "-i", input_video, "-show_entries", "format=duration", "-v", "quiet", "-sexagesimal", "-of", "csv=p=0"]
    return subprocess.check_output(cmd).decode("utf-8").strip()

def simple_tokenizer(text):
    pattern = r'\b[\w-]+\b'
    tokens = re.findall(pattern, text)
    filtered_tokens = [token for token in tokens if re.match(r'^[\w]+(-[\w]+)*$', token)]
    return filtered_tokens

def calculate_compression_ratio(text):
    """Calculate the compression ratio of a text using gzip."""
    uncompressed_data = text.encode('utf-8')
    compressed_data = gzip.compress(uncompressed_data)
    compression_ratio = len(compressed_data) / len(uncompressed_data) if len(uncompressed_data) > 0 else 0.
    return compression_ratio

def find_media_file(base_path):
    """Find a media file with a common audio/video extension."""
    media_extensions = ['', '.mp3', '.mp4', '.aac', '.flac', '.wav', '.ogg', '.m4a', '.mov', '.avi', '.webm']
    for ext in media_extensions:
        candidate = base_path + ext
        if os.path.exists(candidate):
            return candidate
    return None

def has_audio(input_video):
    """Check if the media file has an audio stream."""
    cmd = ["ffprobe", "-i", input_video, "-show_streams", "-select_streams", "a", "-v", "quiet"]
    result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return len(result.stdout) > 0

def check_for_degenerate_vtts(vtt_dir, audio_dir='', file_type='vtt', language='de',
                              possibly_corrupted_outfile='possibly_corrupted.txt',
                              timestamps_tsv='timestamps.tsv',
                              p_connection=None, p_cursor=None, simulate=False, compression_threshold=None):
    files = glob.glob(f'{vtt_dir}/*.{file_type}')
    
    if audio_dir='':
        audio_files = []
    else:
        audio_files = glob.glob(f'{audio_dir}/*')

    ignore = ['-->','WEBVTT']
    vocab = {}

    if language == 'de':
        tokenizer = SoMaJo(language="de_CMC")
    elif language == 'en':
        tokenizer = SoMaJo(language="en_PTB")
    else:
        tokenizer = simple_tokenizer

    degen_files = []

    no_audio_files = 0

    if audio_dir != '':
        for file in tqdm(audio_files):
            # Check for audio
            if not has_audio(file):
                print(f"No audio found in {input_audio}")
                print('SQL:', f"DELETE FROM {sql_table} WHERE cache_file = %s" % (file))
                no_audio_files += 1
                if not simulate:
                    try:
                        p_cursor.execute(f"DELETE FROM {sql_table} WHERE cache_file = %s", (file,))
                        p_connection.commit()
                    except Exception as e:
                        print(f"WARNING! Database operation failed: {e}")

    for file in tqdm(files):
        lines = {}
        paras = []
        last_timestamp = None
        full_text = ""

        if file_type == 'vtt':
            with open(file) as input_vtt:
                for line in input_vtt:
                    if line[-1] == '\n':
                        line = line[:-1]
                    line_strip = line.strip()
                    if ignore[0] not in line_strip and ignore[1] not in line_strip and line_strip != '':
                        lines[line_strip] = True
                        paras.append(line_strip)
                        full_text += line_strip + " "
                    if '-->' in line:
                        last_timestamp = line
        elif file_type == 'json':
            with open(file) as input_json:
                data = json.load(input_json)
                for segment in data['segments']:
                    text = segment['text']
                    paras.append(text)
                    lines[text] = True
                    full_text += text + " "
                    last_timestamp = f"{segment['start']} --> {segment['end']}"

        if audio_dir != '':
            filename = os.path.basename(file)
            base_path = os.path.join(audio_dir, filename.rsplit('.', 1)[0])
            input_audio = find_media_file(base_path)
            if input_audio:
                ffprobe_timestamp = get_duration(input_audio)
                print(f'{file}', f'{last_timestamp=}', f'{ffprobe_timestamp=}')
                with open(timestamps_tsv, 'a') as timestamps_tsv_out:
                    timestamps_tsv_out.write(f'{file}\t{last_timestamp}\t{ffprobe_timestamp}\n')
            else:
                print(f"No valid media file found for {base_path}")

        if language in ['de', 'en']:
            sentences = tokenizer.tokenize_text(paras, parallel=8)
            for sentence in sentences:
                for token in sentence:
                    if token not in vocab:
                        vocab[token.text] = True
        else:
            for para in paras:
                tokens = tokenizer(para)
                for token in tokens:
                    if token not in vocab:
                        vocab[token] = True

        # Check compression ratio
        if compression_threshold is not None:
            compression_ratio = calculate_compression_ratio(full_text)
            if compression_ratio < compression_threshold:
                degen_files.append([file, len(lines), f"Compression ratio: {compression_ratio:.4f}"])
                continue

        if len(lines) < 5:
            degen_num_lines = len(list(lines.keys()))
            degen_files.append([file, degen_num_lines])

    print(f"Vocabulary: {len(vocab)} words.")

    if len(degen_files) > 0:
        corrupted_dir = "corrupted"
        corrupted_dir_full = str(os.path.join(os.path.dirname(degen_files[0][0]), corrupted_dir))
        print('creating dir', corrupted_dir_full, 'if it does not exist.')
        ensure_dir(corrupted_dir_full)
        with open(possibly_corrupted_outfile, 'w') as outfile:
            for file, degen_num_lines, *rest in degen_files:
                assert(file is not None)
                assert(file != '')
                assert(file.endswith(f".{file_type}"))

                print('file:', file, 'len distinct lines very small:', degen_num_lines, *rest)
                outfile.write(file + '\n')

                if not simulate:
                    if f'/{corrupted_dir}/' in file:
                        print('Warning, seems', file, 'was already moved. Ignoring.')
                        continue

                    new_path = str(os.path.join(os.path.dirname(file), corrupted_dir, os.path.basename(file)))
                    assert(new_path is not None)
                    assert(new_path != '')

                    if new_path != file:
                        try:
                            print('Move:', file, '->', new_path)
                            os.rename(file, new_path)
                            if p_connection is not None:
                                print('Execute SQL:', f"UPDATE {sql_table} SET transcript_file = %s WHERE transcript_file = %s" % (new_path, file))
                                p_cursor.execute(f"UPDATE {sql_table} SET transcript_file = %s WHERE transcript_file = %s", (new_path, file))
                                p_connection.commit()
                        except OSError as e:
                            print(f"WARNING! File operation failed: {e}")
                        except Exception as e:
                            print(f"WARNING! Database operation failed: {e}")

    all_files_len = float(len(files))
    degen_files_len = float(len(degen_files))

    degen_percent = (degen_files_len / all_files_len) * 100.
    print(f'Possibly corrupted files: about {round(degen_percent, 3)}%')
    if simulate:
        print(f'Number of files that would be changed: {len(degen_files)}')
        print(f'DB entries that would be deleted because the media file does not contain any audio: {no_audio_files}')
    else:
        print(f'Number of files that were flagged as corrupted: {len(degen_files)}')
        print(f'DB entries that were deleted because the media file did not contain any audio: {no_audio_files}')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Check vtts for unusual repetitions and vocabulary.')
    parser.add_argument('--audio_dir', help='The directory with audio files to check (lengths)', default='', type=str)
    parser.add_argument('--language', help='Language of the vtt files', choices=['de', 'en', 'other'], default='de', type=str)
    parser.add_argument('--file_type', help='Type of transcript files (vtt or json)', choices=['vtt', 'json'], default='vtt', type=str)
    parser.add_argument('--simulate', help='Simulate the operation without moving files or changing the database', action='store_true')
    parser.add_argument('--compression_threshold', help='Threshold for compression ratio to consider a file degenerate', type=float, default=None)

    # Positional argument, without (- and --)
    parser.add_argument('vtt_dir', help='The directory with vtts to check', type=str)

    config = load_config()

    p_connection, p_cursor = connect_to_db(database=config['database'], user=config['user'],
                        password=config['password'], host=config['host'], port=config['port'])

    args = parser.parse_args()

    check_for_degenerate_vtts(args.vtt_dir, args.audio_dir, file_type=args.file_type, language=args.language, p_connection=p_connection, p_cursor=p_cursor, simulate=args.simulate, compression_threshold=args.compression_threshold)
