from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, explode, udf, concat_ws, size, slice
from pyspark.sql.types import StringType, IntegerType
from pyspark.ml import Pipeline
from pyspark.ml.feature import Tokenizer, NGram
import string

def main():
    spark = SparkSession.builder.appName("data-engineering-laboratory-work-1").getOrCreate()
    data = spark.read.json("./data/10K.github.json")
    filteredData = data.where(data["type"] == "PushEvent")

    to_no_punctuation = udf(
        lambda line: line.translate(str.maketrans('', '', string.punctuation)),
        StringType()
    )

    trim_all = udf(
        lambda line: " ".join(line.split()),
        StringType()
    )

    commits = filteredData.select("id", explode("payload.commits").alias("commit"))
    commits = commits.select("id", col("commit.message"), col("commit.author.name"))
    commits = commits.withColumn("message", trim_all(to_no_punctuation(lower(col("message")))))

    message_tokenizer = Tokenizer(inputCol="message", outputCol="ngram_tokenized_message")
    ngram = NGram(n=3, inputCol=message_tokenizer.getOutputCol(), outputCol="message_as_ngrams")
    pipeline = Pipeline(stages=[message_tokenizer, ngram])
    commits = pipeline.fit(commits).transform(commits)

    size_five_or_less = udf(
        lambda actual_size: actual_size if actual_size < 5 else 5,
        IntegerType()
    )

    commits = commits.withColumn(
        "message_as_ngrams",
        slice(col("message_as_ngrams"), 1, size_five_or_less(size(col("message_as_ngrams"))))
    )

    commits = commits.withColumn(
        "message_as_ngrams",
        concat_ws(", ", col("message_as_ngrams"))
    )

    commits = commits.select("name", "message_as_ngrams").coalesce(1)
    commits.write.mode("overwrite").csv("./formatted-data/10K-formatted")


if __name__ == "__main__":
    main()
