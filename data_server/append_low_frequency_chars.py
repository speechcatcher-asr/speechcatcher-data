import argparse
import csv

def read_exclusion_chars(exclusion_file):
    with open(exclusion_file, 'r', encoding='utf-8') as file:
        return set(char.strip() for char in file if char.strip())

def read_char_frequencies(csv_file):
    char_frequencies = {}
    with open(csv_file, 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            char_frequencies[row['char']] = int(row['frequency'])
    return char_frequencies

def append_low_frequency_chars(exclusion_chars, char_frequencies, threshold, output_file):
    with open(output_file, 'a', encoding='utf-8') as file:
        for char, frequency in char_frequencies.items():
            if frequency < threshold and char not in exclusion_chars:
                file.write(char + '\n')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Append low-frequency characters to exclusion list.')
    parser.add_argument('--csv_file', help='The input CSV file with character frequencies', default='chars_es.csv', type=str)
    parser.add_argument('--exclusion_file', help='The file with exclusion characters', default='exclusion_chars/es.txt', type=str)
    parser.add_argument('--output_file', help='The output file to append low-frequency characters', default='exclusion_chars/es.txt', type=str)
    parser.add_argument('--threshold', help='The frequency threshold', default=100, type=int)

    args = parser.parse_args()

    exclusion_chars = read_exclusion_chars(args.exclusion_file)
    char_frequencies = read_char_frequencies(args.csv_file)
    append_low_frequency_chars(exclusion_chars, char_frequencies, args.threshold, args.output_file)

    print(f'Low-frequency characters have been appended to {args.output_file}')
