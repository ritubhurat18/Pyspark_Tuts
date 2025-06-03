import pytest
from pyspark.sql import SparkSession
from unittest.mock import MagicMock, patch
from src.code.word_count import clean_text

# This fixture automatically mocks SparkSession for all tests in this module
@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local").appName("test").getOrCreate()

def python_word_count(s):
    s= s.lower()
    words = {i: s.count(i) for i in set(s.split())}
    return words

def test_python_word_count():
    s = "Hello Reetu Reetu Hello Hi hi"
    assert python_word_count(s) == {"hello": 2, "hi": 2, "reetu": 2}

def test_clean_text_real(spark):
    df = spark.createDataFrame(["Hello Reetu @, How're you?"], "string").toDF("value")
    cleaned_df = clean_text(df)
    result = cleaned_df.collect()[0]["cleaned"]
    assert result == "Hello Reetu  How're you"

def test_word_count():
    from src.code.word_count import word_count
    result = word_count("hello world hello")
    assert result == {"hello": 2, "world": 1}
