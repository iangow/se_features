import re, nltk, sys

def word_count(raw, min_length = 1):
    """ Function to count the number of words in a passage of text.
        Supplying parameter 'min_length' gives number of words with
        at least min_length letters.
    """
    tokens = nltk.word_tokenize(raw)
    return len([word for word in tokens if len(word) >= min_length])

def sent_count(raw):
    """
        Function to count the number of sentences in a passage.
    """
    sent_tokenizer = nltk.data.load('tokenizers/punkt/english.pickle')
    sents = sent_tokenizer.tokenize(raw)
    return len(sents)

def syllables_count(raw):
    """
        Function to count the number of words in a passage with at min_length of syllables.
    """
    tokens = nltk.word_tokenize(raw)
    d = nltk.corpus.cmudict.dict
    #syl = [len(list(y for y in x if y[-1].isdigit())) for x in d[tokens.lower()]] 
    #return 10#len([word for word in tokens if syl >= min_length])
    words = tokens[0]
    print((words[0]))
    print([len(list(y for y in x if y[-1].isdigit())) for x in d[words.lower()]])
    return [len(list(y for y in x if y[-1].isdigit())) for x in d[words.lower()]]


def fog_measure(raw):
    #Not working
    """
        Function for fog measure.
    """
    n_words = word_count(raw)
    n_sentences = sent_count(raw)
    pc_complex = syllables_count(raw)
    print(n_words, pc_complex)
    fog = 0.4 * (n_words/n_sentences + pc_complex)
    return fog