import pathlib
import sqlite3
import pandas as pd
from impala.dbapi import connect
import sys
import os

#impala_server = os.environ['IMPALA_SRV']
# refactor to use hdfs?

# TODO - redo to generate data load to hdfs then create table and load
# webhdfs_host = 'ec2-54-66-248-84.ap-southeast-2.compute.amazonaws.com'
# webhdfs_port = 9870
# hdfs = ibis.hdfs_connect(host=webhdfs_host, port=webhdfs_port)
# hdfs.put(hdfs_path='/tmp/', resource='wind_dataset_table.csv')   

impala_server = 'ec2-54-66-248-84.ap-southeast-2.compute.amazonaws.com'

# connect to impala instance
conn = connect(host=impala_server, port=21050)
cursor = conn.cursor()

print("create table")


cursor.execute(""" CREATE TABLE wind (rowid INT, Speed FLOAT, SpeedError FLOAT, Direction FLOAT,
                    PRIMARY KEY(rowid)) PARTITION BY HASH PARTITIONS 16
                    STORED AS KUDU TBLPROPERTIES ('kudu.num_tablet_replicas' = '1')""")


### read in the data table
### temp -we have the table in csv
print("load df")
df = pd.read_csv('wind_dataset_table.csv')


# works but is really really slow and inefficient
print("load data")
for index, row in df.iterrows():

    sql = 'INSERT INTO wind ( rowid, Speed, SpeedError, Direction ) VALUES ( {0}, {1}, {2}, {3} )'\
                .format(int(row['index']), row['Speed'], row['SpeedError'], row['Direction'])

    print(">>>>>> " + sql, file=sys.stderr, flush=True)

    cursor.execute(sql)






