# Databricks notebook source
dbutils.widgets.text("source", "", "Source from which to copy file from")
dbutils.widgets.text("dest", "", "Destination where to copy file to")
dbutils.widgets.text("py-files", "", "Additional comma separated list of files normally submitted via --py-files")

# COMMAND ----------

import os

# COMMAND ----------

class OperationFailed(RuntimeError):
  pass

# COMMAND ----------

source = dbutils.widgets.get("source")
dest = dbutils.widgets.get("dest")
prefixed_dest = dest
py_files = dbutils.widgets.get("py-files").split(",")
if prefixed_dest.startswith("/") or prefixed_dest.startswith("."):
  prefixed_dest = f"file://{prefixed_dest}"
py_files = [f"{'file://' if f.startswith('/') or f.startswith('.') else ''}{f}" for f in py_files]

# COMMAND ----------

path_splits = os.path.split(source)
filename = path_splits[1]

# COMMAND ----------

dbutils.fs.mkdirs(prefixed_dest)

# COMMAND ----------

dbutils.fs.cp(source, f"{prefixed_dest}/{filename}", True)

# COMMAND ----------

cmd = f"unzip -o {dest}/{filename} -d {dest}"
result = os.system(cmd)
if result != 0:
  raise OperationFailed(f"Command \"{cmd}\" exited with {result}")

# COMMAND ----------

for f in py_files:
  spark.sparkContext.addPyFile(f)
