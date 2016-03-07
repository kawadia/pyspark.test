""" wordcount example using the rdd api, we'll write a test for this """
from __future__ import print_function

import sys
from operator import add

from pyspark import SparkContext


def do_word_counts(lines):
    """ count of words in an rdd of lines """

    counts = (lines.flatMap(lambda x: x.split())
                  .map(lambda x: (x, 1))
                  .reduceByKey(add)
             ) 
    results = {word: count for word, count in counts.collect()}
    return results


if __name__ == "__main__":
    
    if len(sys.argv) != 2:
        sys.exit("Usage: wordcount file}")
    
    sc = SparkContext(appName="PythonWordCount")
    lines = sc.textFile(sys.argv[1], 1)
    
    print(do_word_counts(lines))
    

