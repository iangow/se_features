#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue May  7 20:13:01 2019

@author: carrascod
"""

import pandas as pd
import requests
import io

base_url = "http://www3.nd.edu/~mcdonald/Data/Finance_Word_Lists"

url  = ["LoughranMcDonald_Positive.csv",
                   "LoughranMcDonald_Negative.csv",
                   "LoughranMcDonald_Uncertainty.csv",
                   "LoughranMcDonald_Litigious.csv",
                   "LoughranMcDonald_ModalStrong.csv",
                   "LoughranMcDonald_ModalWeak.csv"]

category = ["positive", "negative", "uncertainty",
            "litigious", "modal_strong", "modal_weak"]

r = requests.post(base_url+'/'+url[0])

print(r)


if r.ok:
    data = r.content.decode('utf8')
    df = pd.read_csv(io.StringIO(data))

df.to_csv("lm_words_pythonb.csv")