from pyspark.sql import SparkSession
import random
import datetime

spark = SparkSession.builder \
    .appName("Synthetic Data Generation") \
    .getOrCreate()

num_rows = 1000
products = ["Товар A", "Товар B", "Товар C", "Товар D", "Товар E"]
max_quantity = 10
max_price = 100.0

data = []
start_date = datetime.datetime.now() - datetime.timedelta(days=365)

for _ in range(num_rows):
    date = start_date + datetime.timedelta(days=random.randint(0, 365))
    user_id = random.randint(1, 1000)
    product = random.choice(products)
    quantity = random.randint(1, max_quantity)
    price = round(random.uniform(1, max_price), 2)
    data.append((date.strftime('%Y-%m-%d %H:%M:%S'), user_id, product, quantity, price))

columns = ["Date", "UserID", "Product", "Quantity", "Price"]
df = spark.createDataFrame(data, columns)

output_path = "synthetic_data.csv"
df.write.csv(output_path, header=True, mode="overwrite")

spark.stop()