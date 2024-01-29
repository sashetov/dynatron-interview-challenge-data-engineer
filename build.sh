#!/bin/bash
rm -rf venv
python3 -m venv venv
source venv/bin/activate
sudo venv/bin/python3 -m pip install --no-cache-dir -r requirements.txt
deactivate
