from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, explode, udf, concat_ws, size, slice
from pyspark.sql.types import StringType
import string

def main():
    spark = SparkSession.builder.appName("data-engineering-laboratory-work-1").getOrCreate()
    data = spark.read.json("./data/10K.github.json")
    pushEvents = data.where(data["type"] == "PushEvent")

    to_no_punctuation = udf(
        lambda line: line.translate(str.maketrans('', '', string.punctuation)),
        StringType()
    )

    trim_all = udf(
        lambda line: " ".join(line.split()),
        StringType()
    )

    commits = pushEvents.select("id", explode("payload.commits").alias("commit"))
    commits = commits.select("id", col("commit.message"), col("commit.author.name"))
    commits = commits.withColumn("message", trim_all(to_no_punctuation(lower(col("message")))))
    print("wip")


if __name__ == "__main__":
    main()
