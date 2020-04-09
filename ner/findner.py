#!/usr/bin/env python2
# -*- coding: utf-8 -*-

import json
import os
import sys
from nltk.tag.stanford import StanfordNERTagger as NERTagger
from nltk.tokenize import sent_tokenize, word_tokenize
from collections import defaultdict

global tagger

# Point these toward your download of Stanford NER
# Need to figure out which is appropriate classifier to use
stanford_path = 'stanford-ner-2018-02-27'
NER_JAR = os.path.join(stanford_path, 'stanford-ner.jar')

def get_positions(sentence_lists):
    """ Function to get a list of positions for a list of lists of
        sentences. This allows reconstruction from a collapsed list.

        Suppose sentence_lists consists of a list of lists of sentences
        with [len(s) for s in sentences_list] equal to
            [2, 21, 32, 7, 16].

        That is the first list has 2 sentences, the second list 21 sentences, ...

        In this case, get_positions will return:
            [[0, 2],
             [2, 23],
             [23, 55],
             [55, 62],
             [62, 78]]
    """
    num_sentences=[0] + [len(sent_list) for sent_list in sentence_lists]
    return [[sum(num_sentences[0:i]), sum(num_sentences[0:i+1])]
                     for i in range(1, len(num_sentences))]

def flatten(a_list):
    """Function to flatten a list of lists of Xs in to a list of Xs"""
    return [l for sublist in a_list for l in sublist]

def reassemble(tagged, positions):
    """ Function to reassemble list of sentences into
        list of lists of sentences using positions of sentences in original
        list of lists
    """
    return [flatten(tagged[position[0]:position[1]]) for position in positions]

def processTags(tag):
    """Function to filter out "O" tags and convert tags to JSON"""
    nerTags = defaultdict(list)
    for val, key in tag:
        if key != "O":
            nerTags[key].append(val)

    return json.dumps(nerTags)

def findner(text):
    """A function that takes text and a classifier and returns
    NER tags as JSON.
    """

    # Tokenize into sentences, then pass to the NER tagger.
    # Performance is much worse than for that of findner_array.
    sents = sent_tokenize(text)
    tagged = tagger.tag_sents([word_tokenize(s) for s in sents])

    # Return the result as JSON.
    return processTags(flatten(tagged))

def findner_array(text_array):
    """Function that takes an array of text and returns an array of JSON
        where JSON is the NER tags associated with each element of the
        text array.
    """

    # Tokenize into sentences, then pass a flattened, word-tokenized
    # list to the NER tagger. This enhances performance.
    sents = [sent_tokenize(speaker_text) for speaker_text in text_array]
    tagged = tagger.tag_sents([word_tokenize(s) for s in flatten(sents)])

    # Reassemble the flattened, tagged list, then return the result as JSON.
    combined = reassemble(tagged, get_positions(sents))
    return [ processTags(tag) for tag in combined]

# Create Stanford NER Tagger
def tagger_init(ner_class=7):

    global tagger

    if ner_class == 4:
        classifier = "english.conll.4class.distsim.crf.ser.gz"
    elif ner_class == 7:
        classifier = "english.muc.7class.distsim.crf.ser.gz"
    else:
        print('Invalid ner_class, should be 4 or 7')

    NER_CLASSIFIER = os.path.join(stanford_path,
                              "classifiers", classifier)

    tagger = NERTagger(NER_CLASSIFIER, NER_JAR)
    return True

if __name__ == "__main__":
    tagger_init()

    res = findner("This is a sample passage. " +
                  "Анастасия is a professor at the University of Chicago.")
    print(res)

    res = findner("This is a sample passage. " +
                   "Ian Gow is a professor at Harvard Business School.")
    print(res)
