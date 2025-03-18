import os
import argparse
import time
import subprocess
import requests
import re
import jiwer

import numpy as np
import matplotlib.pyplot as plt

from whisper_single_file import WhisperOriginal, FasterWhisper, WhisperX
from whisper_multiple_files import BatchedTransformerWhisper
from utils import load_config

config = load_config()

# Configuration
api_base_url = config['server_api_url']  # Base URL of the API
api_access_key = config['secret_api_key']  # API secret key

def simple_tokenizer(text):
    # Regular expression to match words, hyphenated words, and alphanumeric combinations
    # This regex will:
    # - Keep words that may contain hyphens, such as 'co-op', 'mother-in-law'
    # - Keep numbers and words with numbers like '123', '2nd'
    # - Support Unicode characters for European languages
    pattern = r'\b[\w-]+\b'

    # Find all matches of the pattern
    tokens = re.findall(pattern, text)

    # Filter out tokens that are just hyphens or have hyphens at boundaries
    # This step ensures that stray hyphens are not treated as tokens
    filtered_tokens = [token for token in tokens if re.match(r'^[\w]+(-[\w]+)*$', token)]

    return filtered_tokens

def fetch_batch(language, n, min_duration):
    """Fetch a batch of work from the server."""
    url = f"{api_base_url}/get_work_batch/{language}/{api_access_key}/{n}?min_duration={min_duration}"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()['tasks']
    else:
        print("Failed to fetch batch:", response.text)
        return []

def transcribe_with_cli(audio_url, output_path, language='en'):
    cmd = [
        "whisper",
        "--model", "large-v3",
        "--output_dir", output_path,
        "--language", language,
        "--output_format", "vtt",
        audio_url
    ]

    print('Transcribe with CLI cmd:', cmd)

    subprocess.run(cmd, check=True)

def extract_text_from_vtt(vtt_content):
    """Extract pure text from a vtt file, without the timestamps"""
    lines = vtt_content.splitlines()
    text_lines = []
    skip_header = True

    for line in lines:
        # Skip empty lines
        if not line.strip():
            continue

        # Skip header lines
        if skip_header:
            if line.strip() == 'WEBVTT':
                continue
            else:
                skip_header = False

        # Skip timestamp lines
        if '-->' in line:
            continue

        # Add text lines to the list
        text_lines.append(line.strip())

    text_only = '\n'.join(text_lines).strip()
    return text_only

def calculate_wer_cer(reference_path, hypothesis_path, language="en", wer_lower_case=True):
    with open(reference_path, 'r') as ref_file, open(hypothesis_path, 'r') as hyp_file:
        ref_content = ref_file.read()
        hyp_content = hyp_file.read()

        ref_text = extract_text_from_vtt(ref_content).replace('\n', ' ').replace('  ', ' ').strip()
        hyp_text = extract_text_from_vtt(hyp_content).replace('\n', ' ').replace('  ', ' ').strip()

        # Tokenize the texts with a crappy and simple word tokenizer
        ref_tokens = simple_tokenizer(ref_text)
        hyp_tokens = simple_tokenizer(hyp_text)

        if wer_lower_case:
            ref_tokens = [token.lower() for token in ref_tokens]
            hyp_tokens = [token.lower() for token in hyp_tokens]

        joined_ref_tokens = ' '.join(ref_tokens)
        joined_hyp_tokens = ' '.join(hyp_tokens)

        # Run wer computation on lower cased words with no punctuation (default settings)
        wer_score = jiwer.wer(joined_ref_tokens, joined_hyp_tokens)
        # Compare transcripts on the character level with punctuation and casing
        cer_score = jiwer.cer(ref_text, hyp_text)

        return wer_score, cer_score

def main():
    parser = argparse.ArgumentParser(description='Batch Transcribe Audio Files with Whisper')
    parser.add_argument('--language', type=str, default='en', help='Language code for transcription')
    parser.add_argument('--batch_size', type=int, default=4, help='Number of audio files to process in a batch')
    parser.add_argument('--beam_size', type=int, default=5, help='Decoding beam size')
    parser.add_argument('--min_duration', type=float, default=280.0, help='Minimum duration of audio files in seconds')
    parser.add_argument('--implementation', choices=['original', 'faster', 'X', 'batched_transformer'], default='batched_transformer', help='Select the whisper implementation to use')
    parser.add_argument('--force-cli-reference-rerun', action='store_true', help='Force rerun of Whisper CLI for reference transcriptions even if they exist')
    args = parser.parse_args()

    # Directory setup
    output_base_dir = 'benchmark_output'
    implementation_dir = os.path.join(output_base_dir, args.implementation)
    reference_dir = os.path.join(output_base_dir, 'whisper_og_reference')
    os.makedirs(implementation_dir, exist_ok=True)
    os.makedirs(reference_dir, exist_ok=True)

    # Fetching batch
    tasks = fetch_batch(args.language, args.batch_size, args.min_duration)
    if not tasks:
        print("No tasks fetched, nothing to transcribe.")
        return

    audio_urls = [(task['local_cache_audio_url'], task['duration']) for task in tasks]
    print('audio_urls:', audio_urls)

    # Transcription based on the selected implementation
    if args.implementation == 'original':
        transcriber = WhisperOriginal(beam_size=args.beam_size)
    elif args.implementation == 'faster':
        transcriber = FasterWhisper(beam_size=args.beam_size)
    elif args.implementation == 'X':
        transcriber = WhisperX(beam_size=args.beam_size)
    elif args.implementation == 'batched_transformer':
        transcriber = BatchedTransformerWhisper(beam_size=args.beam_size)
    elif args.implementation == 'cpp':
        transcriber = WhisperCpp(beam_size=args.beam_size)
    else:
        raise NotImplementedError("Not implemented:", args.implementation)

    start_time = time.time()
    transcriber.load_model()
    model_load_time = time.time() - start_time

    wers = []
    cers = []
    total_audio_duration = 0.  # in seconds
    total_processing_time = 0.  # in seconds
    do_plot = False

    if args.implementation == 'batched_transformer':
        # Transcribe the entire batch at once
        start_time = time.time()
        audio_urls_list = [url for url, _ in audio_urls]
        runs = 1
        
        if runs > 1:
            do_plot = True
        
        multi_transcriptions = transcriber.transcribe_batch(audio_urls_list, language=args.language, runs=runs)
        total_processing_time = time.time() - start_time

        results = {}

        if runs==1:
            multi_transcriptions = [multi_transcriptions]

        for i,transcriptions in enumerate(multi_transcriptions):
            for (audio_url, duration), transcription in zip(audio_urls, transcriptions):
                total_audio_duration += duration
                filename = os.path.splitext(os.path.basename(audio_url))[0] + '.vtt'
                multi_filename = os.path.splitext(os.path.basename(audio_url))[0] + '.' + str(i) + '.vtt'

                file_path = os.path.join(implementation_dir, multi_filename)

                with open(file_path, 'w') as file_out:
                    transcriber.write_vtt(transcription, file_out)

                # Transcribe with CLI for reference
                reference_file_path = os.path.join(reference_dir, filename)
                if args.force_cli_reference_rerun or not os.path.exists(reference_file_path):
                    transcribe_with_cli(audio_url, reference_dir, language=args.language)
                else:
                    if i==0:
                        print(f'Not overwriting {reference_file_path} with Whisper CLI since it already exists.'
                          'You can force to redo the reference transcription with --force-cli-reference-rerun.')
    
                # Calculate WER and CER using extracted text
                wer, cer = calculate_wer_cer(reference_file_path, file_path)
                print(f'[run {i}] WER and CER is:', wer, cer)
                wers.append(wer)
                cers.append(cer)

                # Collecting data for plotting
                file_key = os.path.basename(audio_url)
                if file_key not in results:
                    results[file_key] = {'wer': [], 'cer': []}
                results[file_key]['wer'].append(wer)
                results[file_key]['cer'].append(cer)

        if do_plot:
            plt.figure(figsize=(12, 6))
            colors = plt.cm.tab10.colors

            for idx, (file, metrics) in enumerate(results.items()):
                plt.plot(range(runs), metrics['wer'], label=f'{file[:20]} WER', marker='o', color=colors[idx % len(colors)])
                plt.plot(range(runs), metrics['cer'], label=f'{file[:20]} CER', marker='x', linestyle='--', color=colors[idx % len(colors)])

            plt.title('WER and CER across runs for each file')
            plt.xlabel('Run')
            plt.ylabel('Error Rate')
            plt.xticks(range(runs))
            plt.ylim(bottom=0)
            plt.grid(True)
            plt.legend()
            plt.tight_layout()

            plot_filename = f'benchmark_{int(time.time())}.pdf'
            plt.savefig(plot_filename)
            print(f'Plot saved to {plot_filename}')

    else:
        # Transcribe each file individually
        for audio_url, duration in audio_urls:
            total_audio_duration += duration
            start_time = time.time()
            transcription = transcriber.transcribe(audio_url, language=args.language, duration=duration)
            transcription_time = time.time() - start_time
            total_processing_time += transcription_time

            filename = os.path.splitext(os.path.basename(audio_url))[0] + '.vtt'
            file_path = os.path.join(implementation_dir, filename)
            with open(file_path, 'w') as file_out:
                transcriber.write_vtt(transcription, file_out)

            # Transcribe with CLI for reference
            reference_file_path = os.path.join(reference_dir, filename)
            if args.force_cli_reference_rerun or not os.path.exists(reference_file_path):
                transcribe_with_cli(audio_url, reference_dir, language=args.language)
            else:
                print(f'Not overwriting {reference_file_path} with Whisper CLI since it already exists.'
                      'You can force to redo the reference transcription with --force-cli-reference-rerun.')

            # Calculate WER and CER using extracted text
            wer, cer = calculate_wer_cer(reference_file_path, file_path)
            print('WER and CER is:', wer, cer)
            wers.append(wer)
            cers.append(cer)

    # transcription_speed in in hours per hour, or minutes per minute, or seconds per second
    transcription_speed = total_audio_duration / total_processing_time

    # Output summary report
    print("\nSummary Report:")
    print(f"Model load time: {model_load_time:.2f} seconds")
    print(f"Total transcription processing time: {total_processing_time:.2f} seconds")
    print(f"Total audio time transcribed: {total_audio_duration:.2f} seconds")
    print(f"Average WER: {sum(wers) / len(wers):.4f}")
    print(f"Average CER: {sum(cers) / len(cers):.4f}")
    print(f"Transcription Speed: {transcription_speed:.2f} hours per hour")

if __name__ == "__main__":
    main()

