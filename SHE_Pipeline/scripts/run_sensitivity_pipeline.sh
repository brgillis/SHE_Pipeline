#!/usr/bin/bash

# echo password | sudo -S systemctl restart euclid-ial-wfm
# sleep 1

pipeline_runner.py --configure --config=/cvmfs/euclid-dev.in2p3.fr/CentOS7/EDEN-2.0/opt/euclid/SHE_Pipeline/0.3/InstallArea/x86_64-co7-gcc48-o2g/auxdir/SHE_Pipeline/euclid_prs.app --serverurl="http://localhost:50000"
pipeline_runner.py --pipeline=sensitivity_pipeline.py --data=/cvmfs/euclid-dev.in2p3.fr/CentOS7/EDEN-2.0/opt/euclid/SHE_Pipeline/0.3/InstallArea/x86_64-co7-gcc48-o2g/auxdir/SHE_Pipeline/sensitivity_isf.txt --serverurl="http://localhost:50000"
