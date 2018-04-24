#!/bin/bash

# script to be run on the dataproc instance as a boostrapping script

sudo apt -y install python-pip python-dev
pip install --upgrade click google-cloud