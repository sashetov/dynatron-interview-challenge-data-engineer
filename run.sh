#!/bin/bash
rm -f db.sqlite3 # delete any old db from previous runs
source venv/bin/activate
echo "RUNNING TESTS:"
python3 test.py
echo -ne "\nRUNNING CHALLENGE:\n"
python3 challenge.py
