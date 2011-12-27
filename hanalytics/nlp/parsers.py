import re
from nltk.tokenize import sent_tokenize
from nltk.tokenize.punkt import PunktSentenceTokenizer

#PunktSentenceTokenizer

def tokenize_sents(text):
#    return sent_tokenize(text)
    return filter(None, re.split(r'(?<! hon)\.', text))

def strip_tags(text):
    return re.sub(r'<[^>]*?>', '', text).strip()