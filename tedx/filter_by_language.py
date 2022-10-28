import spacy
import sys
from collections import defaultdict

from spacy.language import Language

from spacy_language_detection import LanguageDetector

inputfile = "tedx_yt_videolist"

def get_lang_detector(nlp, name):
    return LanguageDetector(seed=42)

nlp_model = spacy.load("en_core_web_sm")
Language.factory("language_detector", func=lambda nlp, name: LanguageDetector(seed=42))
nlp_model.add_pipe('language_detector', last=True)

lang_dist = defaultdict(int)

with open(inputfile, newline='') as infile: 
    for line in infile:
        print(line)
        doc = nlp_model(line)
        language = doc._.language
        print(language['language'])
      
        lang_dist[language['language']] += 1

print('language distribution:', lang_dist)

print(lang_dist['en'])
print(lang_dist['de'])
      
        #sys.exit(0)
