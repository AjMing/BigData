# ✅ Step 1: Set Up Spark Session
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("TweetStreamLab").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType

file_schema = StructType() \
    .add("timestamp", TimestampType()) \
    .add("username", StringType()) \
    .add("tweet", StringType())

stream_dir = "gs://mingmanas123/tweet_stream"

# ✅ Step 2: Read Tweet Stream with Spark
df = spark.readStream.option("header", "true").schema(file_schema).csv(stream_dir)

query = df.writeStream.outputMode("append").format("console").start()
query.awaitTermination(30)


# ✅ Step 3: Extract and Count Hashtags
from pyspark.sql.functions import explode, split, lower, regexp_extract, col



# Split tweet text into words
words = df.select(explode(split(col("tweet"), " ")).alias("word"))

# Extract hashtags using regex
hashtags = words.filter(col("word").startswith("#")) \
                .withColumn("hashtag", lower(regexp_extract("word", "(#\\w+)", 1)))\
# Count hashtags
hashtag_counts = hashtags.groupBy( "hashtag") \
                    .count()

# Stream output
query = hashtag_counts.writeStream \
      .outputMode("complete") \
      .format("console") \
      .start()

query.awaitTermination(30)
