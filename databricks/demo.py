# Databricks notebook source
for i in dbutils.fs.ls("/FileStore/tables/demo"):
    if i.name.endswith(".csv"):
        df = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/tables/demo/SampleSuperstore_in_.csv")
        tempSQL=df.createOrReplaceTempView("SampleSuperstore")
        spark.sql(f"select sum(profit) as totalProfit from SampleSuperstore group by shipDate ").show()
    else:
        exit()
