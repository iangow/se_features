import re, nltk, sys
import string
from nltk.corpus import wordnet
import pyphen

def fog_measure(raw):
    """
        Function to count the number of words in a passage with at min_length of syllables.
    """
    tokens = nltk.word_tokenize(raw)
    tokens = [word.lower() for word in tokens if word.isalpha()]
    n_words = len([word for word in tokens if len(word) >= min_length])
    
    sent_tokenizer = nltk.data.load('tokenizers/punkt/english.pickle')
    sents = sent_tokenizer.tokenize(raw)
    n_sentences = len(sents)
    
    dic = pyphen.Pyphen(lang='en')
    pc_complex = len([word for word in tokens if dic.inserted(word).count('-') >= 2])
    
    return 0.4 * (n_words/n_sentences + pc_complex)
#   fog = 0.4 * (n_words/n_sentences + pc_complex) # Figure out how to pass each value of the fog measure on to the table
#   return n_words, n_sentences, pc_complex, 