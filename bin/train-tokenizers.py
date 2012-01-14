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
import argparse, logging, sys, os
import multiprocessing as mp
import datetime
import random
import pymongo
import pickle
from nltk.tokenize.punkt import PunktTrainer

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, PROJECT_ROOT)

from hanalytics.nlp.parsers import strip_tags

parser = argparse.ArgumentParser(description="Train and save tokenizers on hansard text.")
parser.add_argument('-r', '--root-dir', dest="root_dir", default=os.path.join(PROJECT_ROOT, 'data', 'tokenizers'),
                    help="The target directory to write the pickled tokenizers to")
parser.add_argument('-w', '--workers', dest="num_workers", type=int, default=mp.cpu_count() + 1,
                    help = "The number of workers to use, defaults to the number of processors plus one.")
parser.add_argument('-t', '--type', dest="type", choices=["sent"],
                    help="The type of tokenizer to train.")
parser.add_argument('-l', '--log-level', dest="loglevel", choices=["DEBUG", "INFO", "WARNING", "ERROR"], default="INFO",
                    help = "Set the debug level, DEBUG is the most verbose, ERROR the least.")
parser.add_argument('-a', '--rate', dest="rate", type=float, default=0.1,
                    help="The rate at which to sample the stream of items.")
parser.add_argument('-o', '--window', dest="window", type=int, default=730,
                    help="Window size to use for each tokenizer.")
args = parser.parse_args()

# TODO: move out
logger = logging.getLogger()
logger.setLevel(getattr(logging, args.loglevel))
logger.addHandler(logging.StreamHandler())

if not os.path.exists(args.root_dir):
    os.makedirs(args.root_dir)

collection = pymongo.Connection()['hanalytics']['speech']
window_size = datetime.timedelta(days=args.window)
earliest_date = collection.find_one(sort=[("date", pymongo.ASCENDING)])['date']
latest_date   = collection.find_one(sort=[("date", pymongo.DESCENDING)])['date']

trainers = []
while earliest_date < latest_date:
    next_date = earliest_date + window_size
    trainers.append(((earliest_date, next_date), PunktTrainer()))
    earliest_date = next_date
logger.debug("Created trainers")

def find_trainer(trainers, item):
    for trainer in trainers:
        if trainer[0][0] <= item['date'] <= trainer[0][1]:
            return trainer[1]

collection.ensure_index("date")
for id in collection.find(fields={"_id"}, sort=[("date", pymongo.ASCENDING)]):
    if random.random() < args.rate:
        # fetch this item (PARALLISE!!)
        item = collection.find_one({"_id":id['_id']})
        trainer = find_trainer(trainers, item)
        map(lambda text: trainer.train(text, finalize=True), map(strip_tags, item['text']))
logger.debug("Training done")

for span, trainer in trainers:
    trainer.finalize_training()
    filename = "%i-%02i-%02i.pickle" % (span[0].year, span[0].month, span[0].day)
    with open(os.path.join(args.root_dir, filename), "w+") as f:
        pickle.dump(trainer, f)
#def do_train(text):
#    trainer.train(text, finalize=False)
#
#mod = 1/args.rate
#for i, line in enumerate(sys.stdin):
#    data = json.loads(line, object_hook=json_util.object_hook)
#    if i % 10000 is 0:
#        print data['date']
#    if i % mod is 0:
#        map(do_train, map(strip_tags, data['text']))
#
#trainer.finalize_training()
#pickle.dump(trainer, args.outfile)