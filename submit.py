#!/usr/bin/env python3

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
print(w.current_user.me())