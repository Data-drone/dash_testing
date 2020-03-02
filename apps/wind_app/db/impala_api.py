import pandas as pd
import ibis
import hdfs

# Testing impala connection

# should replace with env var?
webhdfs_host = 'ec2-54-66-248-84.ap-southeast-2.compute.amazonaws.com'
webhdfs_port = 9870
impala_host = 'ec2-54-66-248-84.ap-southeast-2.compute.amazonaws.com'
impala_port = 21050


hdfs = ibis.hdfs_connect(host=webhdfs_host, port=webhdfs_port)
client = ibis.impala.connect(
    host=impala_host, port=impala_port, hdfs_client=hdfs
)
db = client.database('default')


def get_wind_data(start: int, end: int) -> pd.DataFrame:

    table = db.wind
    filtered = table.filter([table.rowid > start,
                            table.rowid < end])
    df = filtered['speed', 'speederror', 'direction']


    return df.execute()


def get_wind_data_by_id(id: int) -> pd.DataFrame:

    table = db.wind
    df = table[table.rowid == id]

    return df.execute()