# Databricks notebook source
# MAGIC %sh
# MAGIC
# MAGIC cat > ~/.databrickscfg << EOF
# MAGIC [DEFAULT]
# MAGIC host = https://e2-demo-field-eng.cloud.databricks.com/
# MAGIC token = dapib08811095e1049241f77ca7e10e08dbf
# MAGIC EOF

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
w.current_user.me()

# COMMAND ----------

import submit

# COMMAND ----------

# MAGIC %sh
# MAGIC
# MAGIC python3 submit.py
