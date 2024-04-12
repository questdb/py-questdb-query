import sys

import numpy as np
sys.dont_write_bytecode = True

import os
import unittest
from pathlib import Path
import io
import http.client

import questdb_query.asynchronous as qdbq_a
import questdb_query.synchronous as qdbq_s
from questdb_query import Endpoint
import pandas as pd
from pandas.testing import assert_frame_equal

try:
    # When running a single test.
    from .mock_server import HttpServer
except ImportError:
    # When discovered by unittest.
    from mock_server import HttpServer

# Import the code we can use to download and run a test QuestDB instance
sys.path.append(str(
    Path(__file__).resolve().parent.parent /
    'c-questdb-client' / 'system_test'))
from fixture import \
    QuestDbFixture, install_questdb, install_questdb_from_repo, CA_PATH, AUTH, \
    retry


QUESTDB_VERSION = '7.4.0'
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


def load_all_types_table(qdb):
    qdb.http_sql_query('''
        CREATE TABLE almost_all_types (
            id int,
            active boolean,
            ip_address ipv4,
            age byte,
            temperature short,
            grade char,
            account_balance float,
            currency_symbol symbol,
            description string,
            record_date date,
            event_timestamp timestamp,
            revenue double,
            user_uuid uuid,
            long_number long,
            crypto_hash long256
        ) timestamp (event_timestamp) PARTITION BY DAY WAL;
    ''')
    qdb.http_sql_query('''
        INSERT INTO almost_all_types (
            id, 
            active, 
            ip_address, 
            age, 
            temperature, 
            grade, 
            account_balance, 
            currency_symbol, 
            description, 
            record_date, 
            event_timestamp, 
            revenue, 
            user_uuid, 
            long_number, 
            crypto_hash
        ) VALUES
            (1, true, '192.168.1.1', 25, 72, 'A', 1000.5, 'USD', 'Test record 1', '2023-01-01T00:00:00.000Z', '2023-01-01T00:00:00.000000Z', 200.00, '123e4567-e89b-12d3-a456-426614174000', 123456789012345, '0x7fffffffffffffffffffffffffffffff'),
            (2, false, NULL, 30, 68, 'B', 1500.3, 'EUR', 'Test record 2', NULL, '2023-01-02T00:00:00.000000Z', 300.00, '123e4567-e89b-12d3-a456-426614174001', 987654321098765, NULL),
            (3, NULL, '10.0.0.1', 35, NULL, 'C', NULL, 'JPY', 'Test record 3', '2023-01-03T00:00:00.000Z', '2023-01-03T00:00:00.000000Z', NULL, '123e4567-e89b-12d3-a456-426614174002', NULL, '0x1fffffffffffffffffffffffffffffff');
    ''')

def load_trips_table(qdb):
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


class TestModule(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        cls.qdb = None
        may_install_questdb()

        cls.qdb = QuestDbFixture(
            QUESTDB_INSTALL_PATH, auth=False, wrap_tls=True, http=True)
        cls.qdb.start()

        load_all_types_table(cls.qdb)
        load_trips_table(cls.qdb)

    @classmethod
    def tearDownClass(cls):
        if cls.qdb:
            cls.qdb.stop()

    def _get_endpoint(self):
        return Endpoint(self.qdb.host, self.qdb.http_server_port)

    def s_numpy_query(self, query, *, chunks=1):
        endpoint = self._get_endpoint()
        return qdbq_s.numpy_query(query, endpoint=endpoint, chunks=chunks)
    
    async def a_numpy_query(self, query, *, chunks=1):
        endpoint = self._get_endpoint()
        return await qdbq_a.numpy_query(query, endpoint=endpoint, chunks=chunks)
    
    def s_pandas_query(self, query, *, chunks=1):
        endpoint = self._get_endpoint()
        return qdbq_s.pandas_query(query, endpoint=endpoint, chunks=chunks)
    
    async def a_pandas_query(self, query, *, chunks=1):
        endpoint = self._get_endpoint()
        return await qdbq_a.pandas_query(query, endpoint=endpoint, chunks=chunks)

    def test_count_pandas(self):
        act = self.s_pandas_query('SELECT count() FROM trips')
        exp = pd.DataFrame({'count': pd.Series([10000], dtype='Int64')})
        assert_frame_equal(act, exp, check_column_type=True)

    def test_count_numpy(self):
        act = self.s_numpy_query('SELECT count() FROM trips')
        exp = {'count': np.array([10000], dtype='int64')}
        self.assertEqual(act, exp)

    def test_head_pandas(self):
        act = self.s_pandas_query('SELECT * FROM trips LIMIT 5')
        exp = pd.DataFrame({
            'cab_type': pd.Series([
                'yellow', 'yellow', 'green', 'yellow', 'yellow'],
                dtype='string'),
            'vendor_id': pd.Series([
                'VTS', 'VTS', 'VTS', 'CMT', 'VTS'],
                dtype='string'),
            'pickup_datetime': pd.Series(pd.to_datetime([
                '2016-01-01T00:00:00.000000',
                '2016-01-01T00:00:00.000000',
                '2016-01-01T00:00:01.000000',
                '2016-01-01T00:00:01.000000',
                '2016-01-01T00:00:02.000000']),
                dtype='datetime64[ns]'),
            'dropoff_datetime': pd.Series(pd.to_datetime([
                '2016-01-01T00:26:45.000000',
                '2016-01-01T00:18:30.000000',
                '2016-01-01T00:02:10.000000',
                '2016-01-01T00:11:55.000000',
                '2016-01-01T00:11:08.000000']),
                dtype='datetime64[ns]'),
            'rate_code_id': pd.Series([
                'Standard rate',
                'Standard rate',
                'Standard rate',
                'Standard rate',
                'Standard rate'],
                dtype='string'),
            'pickup_latitude': pd.Series([
                -73.9940567, -73.9801178, -73.92303467, -73.97942352, -73.99834442],
                dtype='float'),
            'pickup_longitude': pd.Series([
                40.71998978, 40.74304962, 40.70674515, 40.74461365, 40.72389603],
                dtype='float'),
            'dropoff_latitude': pd.Series([
                40.78987122, 40.76314163, 40.70864487, 40.7539444, 40.68840027],
                dtype='float'),
            'dropoff_longitude': pd.Series([
                -73.966362, -73.9134903, -73.92714691, -73.99203491, -73.995849610000],
                dtype='float'),
            'passenger_count': pd.Series([
                2, 2, 1, 1, 1],
                dtype='Int32'),
            'trip_distance': pd.Series([
                7.45, 5.52, 0.34, 1.2, 3.21],
                dtype='float'),
            'fare_amount': pd.Series([
                26.0, 19.0, 3.5, 9.0, 11.5],
                dtype='float'),
            'extra': pd.Series([
                0.5, 0.5, 0.5, 0.5, 0.5],
                dtype='float'),
            'mta_tax': pd.Series([
                0.5, 0.5, 0.5, 0.5, 0.5],
                dtype='float'),
            'tip_amount': pd.Series([
                0.0, 0.0, 0.0, 0.0, 0.0],
                dtype='float'),
            'tolls_amount': pd.Series([
                0.0, 0.0, 0.0, 0.0, 0.0],
                dtype='float'),
            'ehail_fee': pd.Series([
                0.0, 0.0, 0.0, 0.0, 0.0],
                dtype='float'),
            'improvement_surcharge': pd.Series([
                0.3, 0.3, 0.3, 0.3, 0.3],
                dtype='float'),
            'congestion_surcharge': pd.Series([
                0.0, 0.0, 0.0, 0.0, 0.0],
                dtype='float'),
            'total_amount': pd.Series([
                27.3, 20.3, 4.8, 10.3, 12.8],
                dtype='float'),
            'payment_type': pd.Series([
                'Cash', 'Cash', 'Cash', 'Cash', 'Cash'],
                dtype='string'),
            'trip_type': pd.Series([
                'na', 'na', 'na', 'na', 'na'],
                dtype='string'),
            'pickup_location_id': pd.Series([
                0, 0, 0, 0, 0],
                dtype='Int32'),
            'dropoff_location_id': pd.Series([
                0, 0, 0, 0, 0],
                dtype='Int32')})
        assert_frame_equal(act, exp, check_column_type=True)

    def test_head_numpy(self):
        act = self.s_numpy_query('SELECT * FROM trips LIMIT 5')
        exp = {
            'cab_type': np.array([
                'yellow', 'yellow', 'green', 'yellow', 'yellow'],
                dtype='object'),
            'vendor_id': np.array([
                'VTS', 'VTS', 'VTS', 'CMT', 'VTS'],
                dtype='object'),
            'pickup_datetime': np.array([
                '2016-01-01T00:00:00.000000',
                '2016-01-01T00:00:00.000000',
                '2016-01-01T00:00:01.000000',
                '2016-01-01T00:00:01.000000',
                '2016-01-01T00:00:02.000000'],
                dtype='datetime64[ns]'),
            'dropoff_datetime': np.array([
                '2016-01-01T00:26:45.000000',
                '2016-01-01T00:18:30.000000',
                '2016-01-01T00:02:10.000000',
                '2016-01-01T00:11:55.000000',
                '2016-01-01T00:11:08.000000'],
                dtype='datetime64[ns]'),
            'rate_code_id': np.array([
                'Standard rate',
                'Standard rate',
                'Standard rate',
                'Standard rate',
                'Standard rate'],
                dtype='object'),
            'pickup_latitude': np.array([
                -73.9940567, -73.9801178, -73.92303467, -73.97942352, -73.99834442],
                dtype='float'),
            'pickup_longitude': np.array([
                40.71998978, 40.74304962, 40.70674515, 40.74461365, 40.72389603],
                dtype='float'),
            'dropoff_latitude': np.array([
                40.78987122, 40.76314163, 40.70864487, 40.7539444, 40.68840027],
                dtype='float'),
            'dropoff_longitude': np.array([
                -73.966362, -73.9134903, -73.92714691, -73.99203491, -73.995849610000],
                dtype='float'),
            'passenger_count': np.array([
                2, 2, 1, 1, 1],
                dtype='int32'),
            'trip_distance': np.array([
                7.45, 5.52, 0.34, 1.2, 3.21],
                dtype='float'),
            'fare_amount': np.array([
                26.0, 19.0, 3.5, 9.0, 11.5],
                dtype='float'),
            'extra': np.array([
                0.5, 0.5, 0.5, 0.5, 0.5],
                dtype='float'),
            'mta_tax': np.array([
                0.5, 0.5, 0.5, 0.5, 0.5],
                dtype='float'),
            'tip_amount': np.array([
                0.0, 0.0, 0.0, 0.0, 0.0],
                dtype='float'),
            'tolls_amount': np.array([
                0.0, 0.0, 0.0, 0.0, 0.0],
                dtype='float'),
            'ehail_fee': np.array([
                0.0, 0.0, 0.0, 0.0, 0.0],
                dtype='float'),
            'improvement_surcharge': np.array([
                0.3, 0.3, 0.3, 0.3, 0.3],
                dtype='float'),
            'congestion_surcharge': np.array([
                0.0, 0.0, 0.0, 0.0, 0.0],
                dtype='float'),
            'total_amount': np.array([
                27.3, 20.3, 4.8, 10.3, 12.8],
                dtype='float'),
            'payment_type': np.array([
                'Cash', 'Cash', 'Cash', 'Cash', 'Cash'],
                dtype='object'),
            'trip_type': np.array([
                'na', 'na', 'na', 'na', 'na'],
                dtype='object'),
            'pickup_location_id': np.array([
                0, 0, 0, 0, 0],
                dtype='int32'),
            'dropoff_location_id': np.array([
                0, 0, 0, 0, 0],
                dtype='int32')}
        self.assertEqual(act.keys(), exp.keys())
        for k in act:
            np.testing.assert_array_equal(act[k], exp[k])
            self.assertEqual(act[k].dtype, exp[k].dtype)

    def _test_chunked_pandas(self, limit=None):
        qry = f'SELECT * FROM trips'
        if limit is not None:
            qry += f' limit {limit}'
        orig = self.s_pandas_query(qry, chunks=1)
        chunkings = [1, 2, 3, 7, 10, 11, 20, 100, 117]
        others = [self.s_pandas_query(qry, chunks=c) for c in chunkings]
        for other in others:
            assert_frame_equal(orig, other, check_column_type=True)

    def test_chunked_pandas_10(self):
        self._test_chunked_pandas(10)

    def test_chunked_pandas_133(self):
        self._test_chunked_pandas(133)

    def test_chunked_pandas(self):
        self._test_chunked_pandas()

    def test_almost_all_types(self):
        act = self.s_pandas_query('SELECT * FROM almost_all_types')
        schema = {
            name: str(val)
            for name, val
            in act.dtypes.to_dict().items()}
        exp_schema = {
            'id': 'Int32',
            'active': 'bool',
            'ip_address': 'string',
            'age': 'int8',
            'temperature': 'int16',
            'grade': 'string',
            'account_balance': 'float32',
            'currency_symbol': 'string',
            'description': 'string',
            'record_date': 'datetime64[ns]',
            'event_timestamp': 'datetime64[ns]',
            'revenue': 'float64',
            'user_uuid': 'string',
            'long_number': 'Int64',
            'crypto_hash': 'string',
            }
        self.assertEqual(exp_schema.keys(), schema.keys())
        for key in exp_schema:
            self.assertEqual((key, exp_schema[key]), (key, schema[key]))

    async def test_async_pandas(self):
        act = await self.a_pandas_query('SELECT count() FROM trips')
        exp = pd.DataFrame({'count': pd.Series([10000], dtype='Int64')})
        assert_frame_equal(act, exp, check_column_type=True)

    async def test_async_numpy(self):
        act = await self.a_numpy_query('SELECT count() FROM trips')
        exp = {'count': np.array([10000], dtype='int64')}
        self.assertEqual(act, exp)

    def test_basic_auth(self):
        endpoint = Endpoint(self.qdb.host, self.qdb.http_server_port, auth=AUTH)
        act = qdbq_s.pandas_query('SELECT count() FROM trips', endpoint=endpoint)
        exp = pd.DataFrame({'count': pd.Series([10000], dtype='Int64')})
        assert_frame_equal(act, exp, check_column_type=True)

    def _do_auth_test(self, exp_auth_header, username=None, password=None, token=None):
        with HttpServer() as server:
            server.responses.append((
                0,
                200,
                'application/json',
                (
                    b'{"columns": [{"name": "count", "type": "LONG"}], ' +
                    b'"count": 1, "dataset": [[10000]], "query": "SELECT count() ' +
                    b'FROM trips", "timestamp": -1}'
                )))
            server.responses.append((
                0,
                200,
                'text/csv',
                b'"count"\r\n10000\r\n'
                ))

            endpoint = Endpoint(
                'localhost',
                server.port,
                username=username,
                password=password,
                token=token)
            act = qdbq_s.pandas_query('SELECT count() FROM trips', endpoint=endpoint)
            exp = pd.DataFrame({'count': pd.Series([10000], dtype='Int64')})
            assert_frame_equal(act, exp, check_column_type=True)

        auth0 = server.headers[0]['Authorization']
        auth1 = server.headers[1]['Authorization']
        self.assertEqual(auth0, auth1)
        self.assertEqual(auth0, exp_auth_header)

    def test_basic_auth(self):
        self._do_auth_test(
            'Basic YWRtaW46cXVlc3Q=',
            username='admin',
            password='quest')
        
    def test_token_auth(self):
        self._do_auth_test(
            'Bearer 1234567890',
            token='1234567890')


if __name__ == '__main__':
    unittest.main()
