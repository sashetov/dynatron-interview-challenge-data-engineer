import unittest
import tempfile
import os
import pandas as pd
from challenge_lib import *

class TestReadFilesFromDir(unittest.TestCase):
    def test_read_files(self):
        # Create a temporary directory and files for testing
        with tempfile.TemporaryDirectory() as tmpdirname:
            file1 = os.path.join(tmpdirname, "file1.xml")
            file2 = os.path.join(tmpdirname, "file2.xml")
            with open(file1, "w") as f:
                f.write("<event></event>")
            with open(file2, "w") as f:
                f.write("<event></event>")
            result = read_files_from_dir(tmpdirname)
            self.assertEqual(len(result), 2)
            self.assertTrue("<event></event>" in result)

class TestParseXml(unittest.TestCase):
    def test_parse_xml(self):
        xml_sample = [
            "<event><order_id>123</order_id><date_time>2023-08-10T12:34:56</date_time><status>Completed</status><cost>100.50</cost></event>",
            "<event><order_id>124</order_id><date_time>2023-08-11T13:34:56</date_time><status>In Progress</status><cost>200.00</cost></event>"
        ]
        expected_output = pd.DataFrame({
            'order_id': ['123', '124'],
            'date_time': [pd.Timestamp('2023-08-10 12:34:56'), pd.Timestamp('2023-08-11 13:34:56')],
            'status': ['Completed', 'In Progress'],
            'cost': [100.50, 200.00]
        })
        result = parse_xml(xml_sample)
        pd.testing.assert_frame_equal(result, expected_output)


class TestWindowByDatetime(unittest.TestCase):
    def test_window_by_datetime(self):
        data = pd.DataFrame({
            'order_id': [1, 2, 3],
            'date_time': [pd.to_datetime('2023-08-10 13:00:00'),
                          pd.to_datetime('2023-08-10 14:00:00'),
                          pd.to_datetime('2023-08-10 19:00:00')],
            'status': ['A', 'B', 'C'],
            'cost': [420.69, 6.66, 777.7]
        })
        windowed_data = window_by_datetime(data, '1D')
        key = pd.to_datetime('2023-08-10 00:00:00')
        self.assertTrue(key in windowed_data)
        self.assertEqual(len(windowed_data[key]), 4)


class TestProcessToRO(unittest.TestCase):
    def test_process_to_RO(self):
        key = pd.to_datetime('2023-08-10 00:00:00')
        windowed_data = {
            key : pd.Series({
                'order_id':  3,
                'date_time': pd.to_datetime('2023-08-10 19:00:00'),
                'status': 'C',
                'cost': 777.7
            })
        }
        ro_list = process_to_RO(windowed_data)
        self.assertIsInstance(ro_list, list)
        self.assertTrue(len(ro_list) > 0)
        self.assertIsInstance(ro_list[0], RO)  # Assuming you have defined the RO class

if __name__ == '__main__':
    unittest.main()
