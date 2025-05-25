from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as sum_
from pyspark.sql.functions import coalesce, lit

def compute_running_total(numbers_df):
    # Thay NULL bằng 0 khi không có dữ liệu
    return numbers_df.groupBy().agg(coalesce(sum_("number"), lit(0)).alias("total"))

def create_spark_session(app_name: str) -> SparkSession:
    return SparkSession.builder.appName(app_name).getOrCreate()

def read_stream_from_socket(spark: SparkSession, host: str, port: int):
    return (
        spark.readStream
        .format("socket")
        .option("host", host)
        .option("port", port)
        .load()
    )

def parse_numbers_from_stream(lines):
    return (
        lines.selectExpr("CAST(value AS INT) AS number")
        .na.drop(subset=["number"])  # drop rows where 'number' is null
    )

def compute_running_total(numbers_df):
    return numbers_df.groupBy().agg(sum_("number").alias("total"))

def write_to_console(stream_df, checkpoint_path: str):
    return (
        stream_df.writeStream
        .outputMode("complete")
        .format("console")
        .option("checkpointLocation", checkpoint_path)
        .start()
    )

def main():
    spark = create_spark_session("StreamSum")
    spark.sparkContext.setLogLevel("ERROR")

    lines = read_stream_from_socket(spark, host="nc-server", port=9999)
    numbers = parse_numbers_from_stream(lines)
    total = compute_running_total(numbers)

    query = write_to_console(total, checkpoint_path="/tmp/checkpoints/stream_sum")
    query.awaitTermination()

if __name__ == "__main__":
    main()
