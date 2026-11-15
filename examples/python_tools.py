"""Expose GlassMoon's Python evaluator as a shared tool."""

import os
from pathlib import Path

from glassmoon.workspace_tools import TOOLS as WORKSPACE_TOOLS
from glassmoon.workspace_tools import Workspace

workspace = Workspace(Path.cwd(), python=True, socket=os.environ.get("GLASSMOON_SOCKET"))
description = WORKSPACE_TOOLS["python_eval"]

TOOLS = {
    "python_eval": {
        "description": description["description"],
        "parameters": description["params_schema"],
        "handler": workspace.evaluate,
    },
}
