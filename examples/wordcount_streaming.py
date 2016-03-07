""" wordcount example using the rdd api, we'll write a test for this """
from __future__ import print_function

from operator import add

def do_streaming_word_counts(lines):
    """ count of words in a dstream of lines """

    counts_stream = (lines.flatMap(lambda x: x.split())
                  .map(lambda x: (x, 1))
                  .reduceByKey(add)
             ) 
    return counts_stream



