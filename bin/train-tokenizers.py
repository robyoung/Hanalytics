#!/usr/bin/env python
"""
Tokenize strings into sentences or sentence parts. This tool accepts input in MongoDB
export format, one json object per line. It looks for text in the 'text' field. If
tokenizing sentences it is assumed that the text field is either a list of paragraphs
or a raw string. If tokenizing sentence parts it is assumed that the text field is a list
of sentences.

Usage:
> mongoexport -d hanalytics -c speech -q '{"foo":"bar"}' | python train-tokenizers.py -t sent -o sent-tokenizer.pickle
"""
import argparse
import json
import logging
import os
import pickle
import sys
from nltk.tokenize.punkt import PunktTrainer
from pymongo import json_util
from hanalytics.nlp.parsers import strip_tags

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

parser = argparse.ArgumentParser(description="Train and save tokenizers on hansard text.")
parser.add_argument('-t', '--type', dest="type", choices=["sent"],
                    help="The type of tokenizer to train.")
parser.add_argument('-o', '--output', dest="outfile", type=argparse.FileType('w+'), required=True,
                    help="The target file to write the pickled tokenizer to")
parser.add_argument('-r', '--rate', dest="rate", type=float, default=0.1,
                    help="The rate at which to sample the stream of items.")
parser.add_argument('-l', '--log-level', dest="loglevel", choices=["DEBUG", "INFO", "WARNING", "ERROR"], default="INFO",
                    help = "Set the debug level, DEBUG is the most verbose, ERROR the least.")
args = parser.parse_args()

# TODO: move out
logger = logging.getLogger()
logger.setLevel(getattr(logging, args.loglevel))
logger.addHandler(logging.StreamHandler())

trainer = PunktTrainer()

def do_train(text):
    trainer.train(text, finalize=False)

mod = 1/args.rate
for i, line in enumerate(sys.stdin):
    data = json.loads(line, object_hook=json_util.object_hook)
    if i % 10000 is 0:
        print data['date']
    if i % mod is 0:
        map(do_train, map(strip_tags, data['text']))

trainer.finalize_training()
pickle.dump(trainer, args.outfile)