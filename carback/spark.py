from extensions import spark
# 创建 SparkSession
#spark = SparkSession.builder \
#    .appName("PyCharmSparkExample") \
#    .master("local[*]").getOrCreate()

# 示例数据
data = [("Alice", 29), ("Bob", 35), ("Cathy", 32)]
columns = ["Name", "Age"]

# 创建 DataFrame
df = spark.createDataFrame(data, columns)

# 展示数据
print("DataFrame 内容：")
df.show()

# 执行简单操作
filtered_df = df.filter(df["Age"] > 30)
print("过滤后的数据：")
filtered_df.show()

# 停止 SparkSession
spark.stop()
