from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, avg

def create_spark_session(app_name: str = "ProductAnalysis") -> SparkSession:
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark

def read_csv_data(spark: SparkSession, path: str):
    return spark.read.csv(path, header=True, inferSchema=True)

def show_sample_rows(products_df, n: int = 5):
    print(f"=== Hiển thị {n} dòng đầu tiên ===")
    products_df.show(n)

def filter_expensive_products(products_df, min_price: float):
    return products_df.filter(col("price") > min_price).orderBy(col("price").desc())

def calculate_price_stats(filtered_df):
    aggregated = filtered_df.agg(
        spark_sum("price").alias("total_price"),
        avg("price").alias("avg_price")
    ).collect()[0]
    return aggregated["total_price"], aggregated["avg_price"]

def add_total_value_column(products_df):
    return products_df.withColumn("total_value", col("price") * col("quantity"))

def group_by_category(products_df):
    return products_df.groupBy("category").agg(spark_sum("quantity").alias("total_quantity"))

def main():
    spark = create_spark_session()
    products_df = read_csv_data(spark, "/scripts/products.csv")

    show_sample_rows(products_df)

    expensive_products = filter_expensive_products(products_df, min_price=100)
    print("=== Sản phẩm giá > 100, sắp xếp giảm dần ===")
    expensive_products.show()

    total_price, avg_price = calculate_price_stats(expensive_products)
    print(f"Tổng giá: {total_price}, Trung bình giá: {avg_price}")

    products_with_total = add_total_value_column(products_df)
    print("=== Thêm cột total_value ===")
    products_with_total.show()

    category_summary = group_by_category(products_df)
    print("=== Tổng số lượng theo danh mục ===")
    category_summary.show()

    spark.stop()

if __name__ == "__main__":
    main()
