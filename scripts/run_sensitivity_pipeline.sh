#!/usr/bin/bash

# echo password | sudo -S systemctl restart euclid-ial-wfm
# sleep 1

pipeline_runner.py --configure --config=/cvmfs/euclid-dev.in2p3.fr/CentOS7/EDEN-2.0/opt/euclid/SHE_Pipeline/euclid_prs_app.cfg --serverurl="http://localhost:50000"
pipeline_runner.py --pipeline=sensitivity_pipeline.py --data=/cvmfs/euclid-dev.in2p3.fr/CentOS7/EDEN-2.0/opt/euclid/SHE_Pipeline/params/sensitivity_isf.txt --serverurl="http://localhost:50000"
