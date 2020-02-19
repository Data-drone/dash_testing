import pathlib
import sqlite3
import pandas as pd
from impala.dbapi import connect

impala_server = 'ec2-54-252-190-231.ap-southeast-2.compute.amazonaws.com'

# connect to impala instance
conn = connect(host=host, port=21050)
cursor = conn.cursor()

cur.execute(""" CREATE TABLE Wind (rowid, Speed, SpeedError, Direction)""")


