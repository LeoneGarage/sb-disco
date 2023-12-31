# Databricks notebook source
dbutils.widgets.text("source", "", "Source from which to copy file from")
dbutils.widgets.text("dest", "", "Destination where to copy file to")
dbutils.widgets.text("py-files", "", "Additional comma separated list of files normally submitted via --py-files")

# COMMAND ----------

import os
import uuid
import shutil

# COMMAND ----------

class OperationFailed(RuntimeError):
  pass

# COMMAND ----------

source = dbutils.widgets.get("source")
dest = dbutils.widgets.get("dest")
prefixed_dest = dest
py_files = dbutils.widgets.get("py-files").split(",")
if prefixed_dest.startswith("/") or prefixed_dest.startswith(".") or prefixed_dest.startswith("~"):
  prefixed_dest = f"file://{prefixed_dest}"
py_files = [f"{'file://' if f.startswith('/') or f.startswith('.') or f.startswith('~') else ''}{f}" for f in py_files]

# COMMAND ----------

home_dir = os.path.expanduser('~')
print(f"Home directory is {home_dir}")
dest = dest.replace("~", home_dir)
prefixed_dest = prefixed_dest.replace("~", home_dir)
py_files = [f.replace("~", home_dir) for f in py_files]

# COMMAND ----------

path_splits = os.path.split(source)
filename = path_splits[1]

# COMMAND ----------

os.makedirs(dest, 0o770, True)

# COMMAND ----------

dbutils.fs.cp(source, f"{prefixed_dest}/{filename}", True)

# COMMAND ----------

os.chmod(f'{dest}', 0o770)

# COMMAND ----------

import zipfile

with zipfile.ZipFile(f'{dest}/{filename}', 'r') as zip:
  zip.extractall(f'{dest}')

# COMMAND ----------

for f in py_files:
  dbutils.fs.ls(f) # check if file exists because addPyFile() does nothing if it doesn't or if it can't be accessed
  spark.sparkContext.addPyFile(f)
