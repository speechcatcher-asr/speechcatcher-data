import glob
import argparse
from tqdm import tqdm
from somajo import SoMaJo

import subprocess
import os
from utils import *

sql_table = 'podcasts'

def get_duration(input_video):
    cmd = ["ffprobe", "-i", input_video, "-show_entries", "format=duration", "-v", "quiet", "-sexagesimal", "-of", "csv=p=0"]
    return subprocess.check_output(cmd).decode("utf-8").strip()

def check_for_degenerate_vtts(vtt_dir, audio_dir='', 
                              possibly_corrupted_outfile='possibly_corrupted.txt',
                              timestamps_tsv='timestamps.tsv', 
                              p_connection=None, p_cursor=None):
    vtts = glob.glob(f'{vtt_dir}/*.vtt')

    ignore = ['-->','WEBVTT']

    vocab = {}

    tokenizer = SoMaJo(language="de_CMC")

    degen_vtts = []

    for vtt in tqdm(vtts):
        lines = {}
        with open(vtt) as input_vtt:
            paras = []
            for line in input_vtt:
                if line[-1] == '\n':
                    line = line[:-1]
                line_strip = line.strip()
                if ignore[0] not in line_strip and ignore[1] not in line_strip and line_strip != '':
                    #print(vtt, line_strip)
                    lines[line_strip] = True
                    paras.append(line_strip)
                if '-->' in line:
                    last_timestamp = line

            if audio_dir!='':
                vtt_filename = vtt.split('/')[-1]
                input_audio = audio_dir + '/' + vtt_filename[:-4]
                ffprobe_timestamp = get_duration(input_audio)
                print(f'{vtt}', f'{last_timestamp=}', f'{ffprobe_timestamp=}')
                timestamps_tsv.write(f'{vtt}\t{last_timestamp}\t{ffprobe_timestamp}\n')
                # TODO: check for overflows

            sentences = tokenizer.tokenize_text(paras, parallel=8)
            for sentence in sentences:
                for token in sentence:
                    if token not in vocab:
                        vocab[token.text] = True
        if len(lines) < 5:
            degen_num_lines = len(list(lines.keys()))
            #print('vtt:',vtt,'len distinct lines very small:', degen_num_lines)
            degen_vtts.append([vtt, degen_num_lines])
    
    print(f"Vocabulary: {len(vocab)} words.")


    if len(degen_vtts) > 0:
        corrupted_dir = "corrupted"
        with open(possibly_corrupted_outfile, 'w'):
            for vtt, degen_num_lines in degen_vtts:
                print('vtt:',vtt,'len distinct lines very small:', degen_num_lines)
                possibly_corrupted_outfile.write(vtt + '\n')
                new_path = os.path.join(os.path.dirname(file), corrupted_dir, os.path.basename(file))
                print('Would move file to:', new_path)
                # os.rename(file, new_path)
                if p_connection is not None:
                    print('Would execute SQL:', f"UPDATE {sql_table} SET transcript_file = %s WHERE transcript_file = %s" % (new_path, file))
                    #p_cursor.execute(f"UPDATE {sql_table} SET transcript_file = %s WHERE transcript_file = %s", (new_path, file))
                    #
                    #p_connection.commit()

    all_vtts_len = float(len(vtts))
    degen_vtts_len = float(len(degen_vtts))

    degen_percent = (degen_vtts_len / all_vtts_len) * 100.
    print(f'Possibly corrupted files: about {round(degen_percent,3)}%')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Check vtts for unusual repetitions and vocabulary.')
    parser.add_argument('--audio_dir', help='The directory with audio files to check (lengths)', default='', type=str)

    # Positional argument, without (- and --)
    parser.add_argument('vtt_dir', help='The directory with vtts to check', type=str)

    config = load_config()

    p_connection, p_cursor = connect_to_db(database=config['database'], user=config['user'],
                        password=config['password'], host=config['host'], port=config['port'])

    args = parser.parse_args()

    check_for_degenerate_vtts(args.vtt_dir, args.audio_dir, p_connection=p_connection, p_cursor=p_cursor)
