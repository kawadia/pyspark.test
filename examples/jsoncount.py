""" wordcount example using the rdd api, we'll write a test for this """
from __future__ import print_function

import sys
from operator import add

from pyspark import SparkContext
from pyspark import HiveContext


def do_json_counts(df, target_name):
    """ count of records where name=target_name in a dataframe with column 'name' """

    return df.filter(df.name == target_name).count()


if __name__ == "__main__":
    
    if len(sys.argv) != 2:
        sys.exit("Usage: json file}")
    
    sc = SparkContext(appName="PythonJsonCount")
    hc = HiveContext.getOrCreate(sc)
    df = hc.read.json(sys.argv[1], 1)
    
    print("Name vikas found %d times" % do_json_counts(df, 'vikas'))
    

