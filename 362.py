from pyspark.sql import SparkSession
from faker import Faker
import random
import csv

fake = Faker()
csv_file_path = "synthetic_data.csv"
num_records = 1000
products = ['Laptop', 'Mouse', 'Keyboard', 'Monitor', 'Printer']

spark = SparkSession.builder \
    .appName("csv") \
    .getOrCreate()

with open(csv_file_path, mode="w", newline="") as file:
    writer = csv.writer(file)
    writer.writerow(["Date", "UserID", "Product", "Quantity", "Price"])

    for i in range(num_records):
        writer.writerow([fake.date_between(start_date='-1y', end_date='today'),
        fake.random_int(min=1, max=1000),
        random.choice(products),
        fake.random_int(min=1, max=5),
        round(random.uniform(10, 1000), 2)])

df = spark.read.csv(csv_file_path, header=True, inferSchema=True)
df.show()
spark.stop()