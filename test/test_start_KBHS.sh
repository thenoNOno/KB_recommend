#!/bin/bash
source activate python3;
cd /usr/local/KB_recommend/test/
nohup python -u KB_httpserver.py>KBHS_log.log 2>&1 &
