import sys
sys.dont_write_bytecode = True

import os
import unittest
from pathlib import Path
import io
import http.client
from urllib import request, parse

from questdb_query import numpy_query

# Import the code we can use to download and run a test QuestDB instance
sys.path.append(str(
    Path(__file__).resolve().parent.parent /
    'c-questdb-client' / 'system_test'))
from fixture import \
    QuestDbFixture, install_questdb, install_questdb_from_repo, CA_PATH, AUTH, \
    retry


QUESTDB_VERSION = '7.3.7'
QUESTDB_INSTALL_PATH = None


def may_install_questdb():
    global QUESTDB_INSTALL_PATH
    if QUESTDB_INSTALL_PATH:
        return

    install_path = None
    if os.environ.get('QDB_REPO_PATH'):
        repo = Path(os.environ['QDB_REPO_PATH'])
        install_path = install_questdb_from_repo(repo)
    else:
        url = ('https://github.com/questdb/questdb/releases/download/' +
            QUESTDB_VERSION +
            '/questdb-' +
            QUESTDB_VERSION +
            '-no-jre-bin.tar.gz')
        install_path = install_questdb(QUESTDB_VERSION, url)
    QUESTDB_INSTALL_PATH = install_path


def upload_csv(qdb, table, csv_path):
    with open(csv_path, 'rb') as file:
        file_data = file.read()

    boundary = "2cdcb4a05801c5ab05f174836624949d"
    body = io.BytesIO()
    body.write(f'--{boundary}\r\n'.encode('utf-8'))
    body.write(f'Content-Disposition: form-data; name="data"; filename="{table}"\r\n'.encode('utf-8'))
    body.write(b'Content-Type: text/csv\r\n\r\n')
    body.write(file_data)
    body.write(f'\r\n--{boundary}--\r\n'.encode('utf-8'))

    # Get the byte data from BytesIO
    body_bytes = body.getvalue()

    # Prepare headers
    headers = {
        'Content-Type': f'multipart/form-data; boundary={boundary}',
        'Content-Length': str(len(body_bytes))
    }

    url = f'/imp?name={table}'

    # Send the HTTP POST request
    try:
        conn = http.client.HTTPConnection(qdb.host, qdb.http_server_port)
        conn.request('POST', url, body_bytes, headers)
        response = conn.getresponse()
        return response.read().decode()
    finally:
        conn.close()


def load_test_data(qdb):
    qdb.http_sql_query('''
        CREATE TABLE 'trips' (
            cab_type SYMBOL capacity 256 CACHE,
            vendor_id SYMBOL capacity 256 CACHE,
            pickup_datetime TIMESTAMP,
            dropoff_datetime TIMESTAMP,
            rate_code_id SYMBOL capacity 256 CACHE,
            pickup_latitude DOUBLE,
            pickup_longitude DOUBLE,
            dropoff_latitude DOUBLE,
            dropoff_longitude DOUBLE,
            passenger_count INT,
            trip_distance DOUBLE,
            fare_amount DOUBLE,
            extra DOUBLE,
            mta_tax DOUBLE,
            tip_amount DOUBLE,
            tolls_amount DOUBLE,
            ehail_fee DOUBLE,
            improvement_surcharge DOUBLE,
            congestion_surcharge DOUBLE,
            total_amount DOUBLE,
            payment_type SYMBOL capacity 256 CACHE,
            trip_type SYMBOL capacity 256 CACHE,
            pickup_location_id INT,
            dropoff_location_id INT
            ) timestamp (pickup_datetime) PARTITION BY MONTH WAL;
    ''')

    trips_csv = Path(__file__).resolve().parent / 'trips.csv'
    upload_csv(qdb, 'trips', trips_csv)

    def check_table():
        try:
            resp = qdb.http_sql_query('SELECT count() FROM trips')
            if not resp.get('dataset'):
                return False
            if resp['dataset'][0][0] == 10000:
                return True
            return False
        except:
            return None

    # Wait until the apply job is done.
    return retry(check_table, timeout_sec=10)


class TestModule(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.qdb = None
        may_install_questdb()

        cls.qdb = QuestDbFixture(
            QUESTDB_INSTALL_PATH, auth=False, wrap_tls=True, http=True)
        cls.qdb.start()

        load_test_data(cls.qdb)

    @classmethod
    def tearDownClass(cls):
        if cls.qdb:
            cls.qdb.stop()

    def test_function_to_test(self):
        self.assertEqual(1, 1)

if __name__ == '__main__':
    unittest.main()
