import glob
import argparse
from collections import Counter
import csv
import os
import unicodedata
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor, as_completed

# A few problematic Unicode characters that we replace with ASCII chars
CHAR_REPLACEMENTS = {
    '\u200b': ' ',   # ZERO WIDTH SPACE
    '\u200c': ' ',   # ZERO WIDTH NON-JOINER
    '\u202c': ' ',   # POP DIRECTIONAL FORMATTING
    '\u00ad': '-',   # SOFT HYPHEN
    '\u0007': ' ',   # BELL character
    '\u200e': ' ',   # LEFT-TO-RIGHT MARK
    '\ue000': ' ',   # PRIVATE USE AREA CHAR
}

def clean_line(line):
    cleaned = []
    for c in line:
        if c == '\n':
            cleaned.append(c)  # preserve newline
        elif c in CHAR_REPLACEMENTS:
            cleaned.append(CHAR_REPLACEMENTS[c])
        elif unicodedata.category(c).startswith('C'):
            cleaned.append(' ')
        else:
            cleaned.append(c)
    return ''.join(cleaned)

def process_file(file):
    ignore = ['-->','WEBVTT']
    character_counter = Counter()

    with open(file, 'r', encoding='utf-8') as input_vtt:
        for line in input_vtt:
            line_strip = clean_line(line.strip())
            if ignore[0] not in line_strip and ignore[1] not in line_strip and line_strip != '':
                character_counter.update(line_strip)

    return character_counter

def compute_character_frequencies(vtt_dir):
    files = glob.glob(f'{vtt_dir}/*.vtt')
    total_counter = Counter()

    with ProcessPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(process_file, file): file for file in files}
        for future in tqdm(as_completed(futures), total=len(futures), desc="Processing VTT files"):
            file_counter = future.result()
            total_counter.update(file_counter)

    return total_counter

def save_frequencies_to_csv(character_counter, output_file):
    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['frequency', 'char']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for char, frequency in character_counter.most_common():
            writer.writerow({'frequency': frequency, 'char': char})

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Compute character frequencies in VTT files.')
    parser.add_argument('vtt_dir', help='The directory with VTT files to check', type=str)
    parser.add_argument('output_file', help='The output CSV file to save the character frequencies', type=str)

    args = parser.parse_args()

    character_counter = compute_character_frequencies(args.vtt_dir)
    save_frequencies_to_csv(character_counter, args.output_file)

    print(f'Character frequencies have been saved to {args.output_file}')

