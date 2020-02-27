### Build out dataset
### Load in dataset
### create table structure

### build dataset
import math
import numpy as np
import pandas as pd
import ibis
import hdfs

# import os
# impala_host = os.environ['IMPALA_HOST']
# impala_port = os.environ['IMPALA_PORT']
# webhdfs_host = os.environ['WEBHDFS_HOST']
# webhdfs_port = os.environ['WEBHDFS_PORT']

# dev
impala_host = 'ec2-54-66-248-84.ap-southeast-2.compute.amazonaws.com'
impala_port = 21050
webhdfs_host = 'ec2-54-66-248-84.ap-southeast-2.compute.amazonaws.com'
webhdfs_port = 9870


hdfs = ibis.hdfs_connect(host=webhdfs_host, port=webhdfs_port)
client = ibis.impala.connect(
    host=impala_host, port=impala_port, hdfs_client=hdfs
)


def build_wind_frame() -> pd.DataFrame:

    # builds out a wind sensor dataset
    # returns as frame

    windVal = []
    windError = []
    windOrientation = []
    prevVal = 20

    prevOrientation = np.random.uniform(0, 360)
    for i in range(0, 86400):
        windVal.append(abs(np.random.normal(prevVal, 2, 1)[0]))
        windError.append(abs(np.random.normal(round(prevVal/10), 1)))
        if(i % 100 == 0):
            windOrientation.append(np.random.uniform(prevOrientation-50,
                                                    prevOrientation+50))
        else:
            windOrientation.append(np.random.uniform(prevOrientation-5,
                                                    prevOrientation+5))
        if(round(windVal[-1]) > 45):
            prevVal = int(math.floor(windVal[-1]))
        elif(round(windVal[-1]) < 10):
            prevVal = int(math.ceil(windVal[-1]))
        else:
            prevVal = int(round(windVal[-1]))
        prevOrientation = windOrientation[-1]

    df = pd.DataFrame.from_dict({
                             'Speed': windVal,
                             'SpeedError': windError,
                             'Direction': windOrientation
                            })

    return df



if __name__ == '__main__':

    wind_frame = build_wind_frame()

    wind_frame.to_parquet('wind_dataset.parq')

    hdfs.put(hdfs_path='/tmp/', resource='wind_dataset.parq')   
