import logging
import os
import sys
import sqlite3
import pandas as pd
import xml.etree.ElementTree as ET

# set up logging
logger = logging.getLogger('errLogger')
logger.setLevel(logging.ERROR)
handler = logging.StreamHandler(sys.stderr)
handler.setLevel(logging.ERROR)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

class RO:
    def __init__(self, order_id, date_time, status, cost):
        self.order_id = order_id
        self.date_time = date_time
        self.status = status
        self.cost = cost

def read_files_from_dir(dir):
    xml_contents = []
    for filename in os.listdir(dir):
        if filename.endswith('.xml'):
            with open(os.path.join(dir, filename), 'r') as file:
                xml_contents.append(file.read())
    return xml_contents

def parse_xml(files):
    rows = []
    for file in files:
        try:
            event = ET.fromstring(file)
        except ET.ParseError as e:
            logger.error(f"Failed to parse XML file:\n{file}{e}")
            continue
        row = {
            'order_id': event.find('order_id').text,
            'date_time': pd.to_datetime(event.find('date_time').text),
            'status': event.find('status').text,
            'cost': float(event.find('cost').text),
        }
        rows.append(row)
    return pd.DataFrame(rows)

def window_by_datetime(data, window):
    grouped = data.groupby(pd.Grouper(key='date_time', freq=window))
    latest_events = {
        time: group.sort_values('date_time').iloc[-1] \
                for time, group in grouped
    }
    return latest_events

def process_to_RO(data):
    ros = []
    for window, df in data.items():
        ro = RO(df['order_id'], df['date_time'], df['status'], df['cost'])
        ros.append(ro)
    return ros

def write_to_db(ros, db_path):
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    try:
        c.execute('''CREATE TABLE IF NOT EXISTS repair_orders 
                     (order_id text, date_time text, status text, cost real)''')
        for ro in ros:
            c.execute("INSERT INTO repair_orders VALUES (?, ?, ?, ?)",
                      (ro.order_id, str(ro.date_time), ro.status, ro.cost))
        conn.commit()
    except sqlite3.Error as e:
        print("Database error:", e)
    finally:
        conn.close()

def select_all_from_db(db_path):
    rows = []
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    try:
        c.execute("SELECT * FROM repair_orders")
        records = c.fetchall()
        rows = [row for row in records]
    except sqlite3.Error as e:
        print("Database error:", e)
    finally:
        conn.close()
    return rows


