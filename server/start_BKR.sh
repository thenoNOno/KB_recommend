#!/bin/bash
#source activate python3;
source /app/env/miniconda2/bin/activate python3;
cd /usr/local/KB_recommend/bin/
nohup python -u forecast.py>>../log/KBR_log.log 2>&1 &
