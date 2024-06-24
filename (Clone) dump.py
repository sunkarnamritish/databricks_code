# Databricks notebook source
filepath1='dbfs:/FileStore/sample/employees.csv'
df=spark.read.csv(filepath1,header=True,inferSchema=True)
df.display()


