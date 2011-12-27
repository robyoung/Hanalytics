import unittest, itertools
from tests import TestHelpMixin
from hanalytics.nlp.parsers import tokenize_sents, strip_tags
from nltk.tokenize.punkt import PunktSentenceTokenizer
import nltk

class CommonsSpeechParserTest(unittest.TestCase, TestHelpMixin):
    def setUp(self):
        self.speeches = [speech for speech in self.read_fixture("hanalytics/writers/speeches.jsonlines")]

    def tearDown(self):
        del self.speeches

    def xtest_strip_tags(self):
        text = strip_tags(self.speeches[0]['text'][0])
        self.assertEqual(text, "I do not have that figure to hand, but I am happy to let the hon. Gentleman have it after the debate. Of course, we have a structured system that ensures that the commission has the overall supervision of complaints, which I will come to, and that it deals directly with the most serious complaints. That is as it should be.")


    def test_tokenize(self):

        train = "\n".join(itertools.imap(strip_tags, itertools.chain(*(speech['text'] for speech in self.speeches[0:10]))))

        print train
        tokenizer = PunktSentenceTokenizer(train)



        sents = tokenizer.tokenize(strip_tags(self.speeches[0]['text'][0]))

        sents = tokenize_sents(strip_tags(self.speeches[0]['text'][0]))

        self.assertEqual(len(sents), 3)

    def test_hard_tokenize(self):

        text = "CELULAR COMMUNICATIONS INC. sold 1,550,00 common shares at $21.75 each yesterday, according to lead underwriter L.F. Rothschild & Co. This was very good."

        sentences = nltk.sent_tokenize(text)

        print sentences


        