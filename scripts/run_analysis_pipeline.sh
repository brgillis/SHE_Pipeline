#!/usr/bin/bash

echo password | sudo -S systemctl restart euclid-ial-wfm
sleep 1
pipeline_runner.py --configure --config=/home/user/Work/Projects/SHE_Pipeline/euclid_prs_app.cfg --serverurl="http://localhost:50000"

pipeline_runner.py --pipeline=analysis_pipeline.py --data=/home/user/Work/Projects/SHE_Pipeline/params/analysis_isf.txt --serverurl="http://localhost:50000"
