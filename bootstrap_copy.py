# Databricks notebook source
dbutils.widgets.text("source", "", "Source from which to copy file from")
dbutils.widgets.text("dest", "", "Destination where to copy file to")

# COMMAND ----------

import os

# COMMAND ----------

source = dbutils.widgets.get("source")
dest = dbutils.widgets.get("dest")
if dest.startswith("/") or dest.startswith("."):
  dest = f"file://{dest}"

# COMMAND ----------

path_splits = os.path.split(source)
filename = path_splits[1]

# COMMAND ----------

dbutils.fs.mkdirs(dest)

# COMMAND ----------

dbutils.fs.cp(source, f"{dest}/{filename}", True)

# COMMAND ----------

os.system(f"unzip -o {dest}/{filename} -d {dest}")
