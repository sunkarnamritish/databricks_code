# Databricks notebook source
dbutils.widgets.dropdown("dataset", "dataset1", ["dataset1", "dataset2", "dataset3"], "Select Dataset")
selected_dropdown = dbutils.widgets.get("dataset")
data1 = [("Alice", 34), ("Bob", 45)]
data2 = [("Charlie", 29), ("David", 41)]
data3 = [("Eve", 22), ("Frank", 33)]

datasets = {
    "dataset1": data1,
    "dataset2": data2,
    "dataset3": data3
}

print(f"Selected dropdown: {selected_dropdown}")
# # Load the selected dataset
data = spark.createDataFrame(datasets[selected_dropdown], ["Name", "Age"])
#data.display()

dbutils.widgets.text("filter_value", "", "Enter Filter Value")
filter_value = dbutils.widgets.get("filter_value")
print(f"Filter value: {filter_value}")


# # Filter the DataFrame based on the widget value
if filter_value:
        filtered_data = data.filter(data.Age == int(filter_value))
else:
     filtered_data = data

filtered_data.show()

filepath1='dbfs:/FileStore/sample/employees.csv'
df=spark.read.csv(filepath1,header=True,inferSchema=True)
sorted_df = df.orderBy("First_Name")
uniqueval=[row[0] for row in sorted_df.select("First_name").distinct().collect() ]
dbutils.widgets.combobox('combobox',uniqueval[2],uniqueval,'comboboxlabel')
selected_combobox=dbutils.widgets.get('combobox')
print('selected combobox option is:' +selected_combobox)



