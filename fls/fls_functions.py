import nltk, re
from collections import Counter

from ling_features import fog, fog_agg, fls, tone_count

# Function to aggregate fog over a list of texts
def tone_agg(texts, prefix=""):
    counter = Counter()
    for text in texts: 
        counter.update(tone_count(text))
    
    return { (prefix + key):counter[key] for key in counter.keys()}

def fls_fog(the_text):
    
    sentences = nltk.tokenize.sent_tokenize(the_text)
    fl_vals = [fls(sent) for sent in sentences]

    # This seems complicated, but saves running fls over the data twice
    fl_sents =  [sent for is_fl, sent in zip(fl_vals, sentences) if is_fl]
    nfl_sents = [sent for is_fl, sent in zip(fl_vals, sentences) if not is_fl]
    
    return {'fl_sents': len(fl_sents),
            'nfl_sents': len(nfl_sents),
            **fog_agg(fl_sents, "fl_"),
            **fog_agg(nfl_sents, "nfl_"),
            **tone_agg(fl_sents, "fl_"),
            **tone_agg(nfl_sents, "nfl_")}