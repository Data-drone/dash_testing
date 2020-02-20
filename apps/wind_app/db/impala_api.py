import pandas as pd
import ibis

# Testing impala connection

# should replace with env var?
webhdfs_host = ''
webhdfs_port = ''
impala_host = ''
impala_port = ''


hdfs = ibis.hdfs_connect(host=webhdfs_host, port=webhdfs_port)
client = ibis.impala.connect(
    host=impala_host, port=impala_port, hdfs_client=hdfs
)
db = client.database('wind')


def get_wind_data(start, end):

    table = db.wind
    filtered = table.filter([table.rowid > start,
                            table.rowid < end])
    df = filtered['Speed', 'SpeedError', 'Direction']


    return df


def get_wind_data_by_id(id):

    table = db.wind
    df = table[table.rowid = id]

    return df