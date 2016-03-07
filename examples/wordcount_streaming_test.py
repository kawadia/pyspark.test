import pytest
import time

from . import wordcount_streaming

# this allows using the fixture in all tests in this module
pytestmark = pytest.mark.usefixtures("spark_context", "streaming_context")


# Can also use a decorator such as this to use specific fixtures in specific functions
# @pytest.mark.usefixtures("spark_context", "hive_context")


def make_dstream_helper(sc, ssc, test_input):
    """ make dstream from input
    Args:
        test_input: list of lists of input rdd data

    Returns: a dstream

    """
    input_rdds = [sc.parallelize(d, 1) for d in test_input]
    input_stream = ssc.queueStream(input_rdds)
    return input_stream


def collect_helper(ssc, dstream, expected_length, block=True):
    """
    Collect each RDDs into the returned list.

    :return: list with the collected items.
    """
    result = []

    def get_output(_, rdd):
        if rdd and len(result) < expected_length:
            r = rdd.collect()
            if r:
                result.append(r)

    dstream.foreachRDD(get_output)

    if not block:
        return result

    ssc.start()

    timeout = 2
    start_time = time.time()
    while len(result) < expected_length and time.time() - start_time < timeout:
        time.sleep(0.01)
    if len(result) < expected_length:
        print("timeout after", timeout)

    return result


def test_streaming_word_counts(spark_context, streaming_context):
    """ test that a single event is parsed correctly
    Args:
        spark_context: test fixture SparkContext
        streaming__context: test fixture SparkStreamingContext
    """

    test_input = [
        [
            ' hello spark ',
            ' hello again spark spark'
        ],
        [
            ' hello there again spark spark'
        ],

    ]

    input_stream = make_dstream_helper(spark_context, streaming_context, test_input)

    tally = wordcount_streaming.do_streaming_word_counts(input_stream)
    results = collect_helper(streaming_context, tally, 2)
    
    expected_results = [
        [('again', 1), ('hello', 2), ('spark', 3)],
        [('again', 1), ('there', 1), ('hello', 1), ('spark', 2)]
    ]


    assert results == expected_results

