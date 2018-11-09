#!/bin/bash
source activate python3;
cd /usr/local/KB_recommend/server/
nohup python -u KB_httpserver.py>>../log/KBHS_log.log 2>&1 &
