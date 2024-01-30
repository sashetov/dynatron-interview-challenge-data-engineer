from challenge_lib import *

def pipeline(dir, db_path):
    xml_contents = read_files_from_dir(dir)
    df = parse_xml(xml_contents)
    print("df:", df)
    windowed_data = window_by_datetime(df, '1D')
    print("windowed_data:", windowed_data)
    ros = process_to_RO(windowed_data)
    print("ros:", ros)
    write_to_db(ros, db_path)
    rows_written = select_all_from_db(db_path)
    print("rows_written", rows_written)

pipeline("data", "db.sqlite3")
