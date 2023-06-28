# py-questdb-query
This library allows you to perform fast queries over HTTP(S)/CSV for QuestDB, a high-performance time-series database.

Query results are obtained as either Pandas dataframes or dicts of numpy arrays. 

## Installation

The library can be installed using the following command:

```shell
python3 -m pip install -U git+https://github.com/questdb/py-questdb-query.git#questdb_query
```

To uninstall the library, you can use the command:

```shell
python3 -m pip uninstall questdb_query
```

## Basic Usage, querying into Numpy

Once installed, you can use the library to query a QuestDB database. Here's an example that demonstrates how to query
CPU utilization data using the library against a database running on `localhost`.

```python
from questdb_query import numpy_query

np_arrs = numpy_query('''
    select
        timestamp, hostname, datacenter, usage_user, usage_nice
    from
        cpu
    limit 10''')
```

The `np_arrs` object is a python `dict` which holds a numpy array per column, keyed by column name:
```python
>>> np_arrs
{'timestamp': array(['2016-01-01T00:00:00.000000000', '2016-01-01T00:00:10.000000000',
       '2016-01-01T00:00:20.000000000', '2016-01-01T00:00:30.000000000',
       '2016-01-01T00:00:40.000000000', '2016-01-01T00:00:50.000000000',
       '2016-01-01T00:01:00.000000000', '2016-01-01T00:01:10.000000000',
       '2016-01-01T00:01:20.000000000', '2016-01-01T00:01:30.000000000'],
      dtype='datetime64[ns]'), 'hostname': array(['host_0', 'host_1', 'host_2', 'host_3', 'host_4', 'host_5',
       'host_6', 'host_7', 'host_8', 'host_9'], dtype=object), 'datacenter': array(['ap-southeast-2b', 'eu-west-1b', 'us-west-1b', 'us-west-2c',
       'us-west-2b', 'eu-west-1b', 'eu-west-1b', 'us-west-1a',
       'ap-southeast-2a', 'us-east-1a'], dtype=object), 'usage_user': array([1.39169048, 0.33846369, 0.        , 1.81511203, 0.84273104,
       0.        , 0.        , 0.28085548, 0.        , 1.37192634]), 'usage_nice': array([0.30603088, 1.21496673, 0.        , 0.16688796, 0.        ,
       2.77319521, 0.40332488, 1.81585253, 1.92844804, 2.12841919])}
```

If we wanted to calculate a (rather non-sensical) weighted average of `usage_user` and `usage_nice` we can
do this by accessing the `numpy` columns:

```python
>>> np_arrs['usage_user'].dot(np_arrs['usage_nice'].T)
4.5700692045031985
```

## Querying a remote database

If your database is running on a remote host, specify an endpoint:

```python
from questdb_query import numpy_query, Endpoint

endpoint = Endpoint(host='your.hostname.com', https=True, username='user', password='pass')

np_arrs = numpy_query('select * from cpu limit 10', endpoint)
```

Note how the example above enables HTTPS and specifies a username and password for authentication.


## Chunks: Query Parallelism

You can sometimes improve performance by splitting up a large query into smaller ones, running them in parallel,
and joining the results together. This is especially useful if you have multiple CPUs available.

The `numpy_query` function can do this automatically for you, by specifying the `chunks` parameter.

The example below, splits up the query into 6 parallel chunks.

```python
from questdb_query import numpy_query

np_arrs = numpy_query('select * from cpu', chunks=6)
```

The speed-up of splitting up a query into smaller ones is highly query-dependent and we recommend you experiment and
benchmark. Mostly due to Python library limitations, not all parts of the query can be parallelized, so whilst you may
see great benefits in going from 1 chunk (the default) to 8, the improvement going from 8 to 16 might be marginal. 

_Read on for more details on benchmarking: This is covered later in this README page._

> :warning: The `chunks > 1` parameter parallelizes queries. If the table(s) queried contain fast-moving data the
> results may be inconsistent as each chunk's query would be started at slightly different times.
>
> To avoid consistency issues formulate the query so that it only queries data that is not changing.
> You can do this, for example, by specifying a `timestamp` range in the `WHERE` clause.

## Querying into Pandas

You can also query into Pandas:

```python
from questdb_query import pandas_query, Endpoint

endpoint = Endpoint(host='your.hostname.com', https=True, username='user', password='pass')

df = pandas_query('select * from cpu limit 1000', endpoint)
```

This allows you, for example, to pre-aggregate results:

```python
>>> df = df[['region', 'usage_user', 'usage_nice']].groupby('region').mean()
>>> df
                usage_user  usage_nice
region                                
ap-northeast-1    8.163766    6.492334
ap-southeast-1    6.511215    7.341863
ap-southeast-2    6.788770    6.257839
eu-central-1      7.392642    6.416479
eu-west-1         7.213417    7.185956
sa-east-1         7.143568    5.925026
us-east-1         7.620643    7.243553
us-west-1         6.286770    6.531977
us-west-2         6.228692    6.439672
```

You can then switch over to numpy with a simple and fast conversion:

```python
>>> from questdb_query import pandas_to_numpy
>>> np_arrs = pandas_to_numpy(df)
>>> np_arrs
{'usage_user': array([8.16376556, 6.51121543, 6.78876964, 7.3926419 , 7.21341716,
       7.14356839, 7.62064304, 6.28677006, 6.22869169]), 'usage_nice': array([6.49233392, 7.34186348, 6.25783903, 6.41647863, 7.18595643,
       5.92502642, 7.24355328, 6.53197733, 6.43967247]), 'region': array(['ap-northeast-1', 'ap-southeast-1', 'ap-southeast-2',
       'eu-central-1', 'eu-west-1', 'sa-east-1', 'us-east-1', 'us-west-1',
       'us-west-2'], dtype=object)}
```

## Benchmarking

### From code

Each query result also contains a `Stats` object with the performance summary which you can print.

```python
>>> from questdb_query import numpy_query
>>> np_arrs = numpy_query('select * from cpu', chunks=8)
>>> print(np_arrs.query_stats)
Duration: 2.631s
Millions of lines: 5.000
Millions of lines/s: 1.901
MiB: 1332.144
MiB/s: 506.381
```

You can also extract individual fields:

```python
>>> np_arrs.query_stats
Stats(duration_s=2.630711865, line_count=5000000, byte_count=1396853875, throughput_mbs=506.3814407360216, throughput_mlps=1.900626239810569)
>>> np_arrs.query_stats.throughput_mlps
1.900626239810569
```

### From the command line

To get the best performance it may be useful to try queries with different hardware setups, chunk counts etc.

You can run the benchmarking tool from the command line:

```bash
$ python3 -m questdb_query.tool --chunks 8 "select * from cpu"
```
```
         hostname          region       datacenter  rack              os arch team  service  service_version service_environment  usage_user  usage_system  usage_idle  usage_nice  usage_iowait  usage_irq  usage_softirq  usage_steal  usage_guest  usage_guest_nice           timestamp
0          host_0  ap-southeast-2  ap-southeast-2b    96     Ubuntu16.10  x86  CHI       11                0                test    1.391690      0.000000    2.644812    0.306031      1.194629   0.000000       0.000000     0.726996     0.000000          0.000000 2016-01-01 00:00:00
1          host_1       eu-west-1       eu-west-1b    52  Ubuntu16.04LTS  x64  NYC        7                0          production    0.338464      1.951409    2.455378    1.214967      2.037935   0.000000       1.136997     1.022753     1.711183          0.000000 2016-01-01 00:00:10
2          host_2       us-west-1       us-west-1b    69  Ubuntu16.04LTS  x64  LON        8                1          production    0.000000      2.800873    2.296324    0.000000      1.754139   1.531160       0.662572     0.000000     0.472402          0.312164 2016-01-01 00:00:20
3          host_3       us-west-2       us-west-2c     8  Ubuntu16.04LTS  x86  LON       11                0                test    1.815112      4.412385    2.056344    0.166888      3.507148   3.276577       0.000000     0.000000     0.000000          1.496152 2016-01-01 00:00:30
4          host_4       us-west-2       us-west-2b    83  Ubuntu16.04LTS  x64  NYC        6                0                test    0.842731      3.141248    2.199520    0.000000      2.943054   5.032342       0.391105     1.375450     0.000000          1.236811 2016-01-01 00:00:40
...           ...             ...              ...   ...             ...  ...  ...      ...              ...                 ...         ...           ...         ...         ...           ...        ...            ...          ...          ...               ...                 ...
624995  host_3995  ap-southeast-2  ap-southeast-2a    30  Ubuntu16.04LTS  x86  CHI       19                1             staging   33.238309     82.647341   17.272531   52.707720     71.718564  45.605728     100.000000    22.907723    78.130846         15.652954 2017-08-01 16:52:30
624996  host_3996       us-west-2       us-west-2a    67     Ubuntu15.10  x64  CHI        9                0          production   33.344070     81.922739   16.653731   52.107537     71.844945  45.880606      99.835977    23.045458    76.468930         17.091646 2017-08-01 16:52:40
624997  host_3997       us-west-2       us-west-2b    63     Ubuntu15.10  x86   SF        8                0          production   32.932095     80.662915   14.708377   53.354277     72.265215  44.803275      99.013038    20.375169    78.043473         17.870002 2017-08-01 16:52:50
624998  host_3998       eu-west-1       eu-west-1b    53  Ubuntu16.04LTS  x86  CHI       11                1             staging   31.199818     80.994859   15.051577   51.923123     74.169828  46.453950      99.107213    21.004499    78.341154         18.880808 2017-08-01 16:53:00
624999  host_3999       us-east-1       us-east-1c    87     Ubuntu16.10  x64   SF        8                1          production   30.310735     81.727637   15.413537   51.417897     74.973555  44.882255      98.821672    19.055040    78.094993         19.263652 2017-08-01 16:53:10

[5000000 rows x 21 columns]

Duration: 2.547s
Millions of lines: 5.000
Millions of lines/s: 1.963
MiB: 1332.144
MiB/s: 522.962
```

These are the complete command line arguments:

```bash
$ python3 -m questdb_query.tool --help
```
```
usage: tool.py [-h] [--host HOST] [--port PORT] [--https] [--username USERNAME] [--password PASSWORD] [--chunks CHUNKS] query

positional arguments:
  query

optional arguments:
  -h, --help           show this help message and exit
  --host HOST
  --port PORT
  --https
  --username USERNAME
  --password PASSWORD
  --chunks CHUNKS
```


## Async operation

The `numpy_query` and `pandas_query` functions are actually wrappers around `async` variants.

If your application is already using `async`, then call those directly as it allows other parts of your application to
perform work in parallel during the data download.

The functions take identical arguments as their synchronous counterparts.

```python
import asyncio
from questdb_query.asynchronous import numpy_query


def main():
    endpoint = Endpoint(host='your.hostname.com', https=True, username='user', password='pass')
    np_arrs = await numpy_query('select * from cpu limit 10', endpoint)
    print(np_arrs)


if __name__ == '__main__':
    asyncio.run(main())

```

