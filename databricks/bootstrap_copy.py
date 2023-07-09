# Databricks notebook source
dbutils.widgets.text("source", "", "Source from which to copy file from")
dbutils.widgets.text("dest", "", "Destination where to copy file to")
dbutils.widgets.text("py-files", "", "Additional comma separated list of files normally submitted via --py-files")

# COMMAND ----------

import os

# COMMAND ----------

source = dbutils.widgets.get("source")
dest = dbutils.widgets.get("dest")
py_files = dbutils.widgets.get("py-files").split(",")
if dest.startswith("/") or dest.startswith("."):
  dest = f"file://{dest}"
py_files = [f"{'file://' if f.startswith('/') or f.startswith('.') else ''}{f}" for f in py_files]

# COMMAND ----------

path_splits = os.path.split(source)
filename = path_splits[1]

# COMMAND ----------

dbutils.fs.mkdirs(dest)

# COMMAND ----------

dbutils.fs.cp(source, f"{dest}/{filename}", True)

# COMMAND ----------

os.system(f"unzip -o {dest}/{filename} -d {dest}")

# COMMAND ----------

for f in py_files:
  spark.sparkContext.addPyFile(f)
