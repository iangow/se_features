#!/usr/bin/env bash
cd fog_measure
./fog_run.py
cd ../liwc_etc
./liwc_run.py
cd ../tone_measure
./tone_measure_run.py
cd ../word_count
./word_count_run.py