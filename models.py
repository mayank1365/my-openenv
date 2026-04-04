from pydantic import BaseModel
from typing import Any, Optional

class PipelinePatch(BaseModel):
    step_index: int
    field: str          # dot-notation path, e.g. "params.null_handling"
    old_value: Any = None
    new_value: Any

class Action(BaseModel):
    diagnosis: str = ""
    patch: Optional[PipelinePatch] = None
    validate_only: bool = False   # if True, run pipeline without patching

class Observation(BaseModel):
    pipeline_name: str
    pipeline_config: dict
    error_log: str
    input_schema: dict             # {"columns": [...], "types": [...]}
    expected_output_schema: dict   # {"columns": [...], "types": [...]}
    sample_input_rows: list[dict]
    current_output_rows: list[dict]
    comparison: dict               # output of compare_output()
    fix_attempts: list[dict]       # history of (patch, result) pairs
    step_number: int

class Reward(BaseModel):
    value: float
    breakdown: dict    # keys: diagnosis, schema, rows, bonus, penalty
