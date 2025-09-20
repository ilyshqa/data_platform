from __future__ import annotations
from pydantic import BaseModel, Field, field_validator
from typing import List, Dict, Literal, Optional, Any

class SourceLocalCSV(BaseModel):
    type: Literal["local_csv"]
    path: str
    options: Optional[Dict[str, Any]] = None

class TransformSelect(BaseModel):
    type: Literal["select"]
    columns: List[str]

class TransformCast(BaseModel):
    type: Literal["cast"]
    columns: Dict[str, Literal["int", "float", "timestamp", "string"]]

class TransformFilter(BaseModel):
    type: Literal["filter"]
    expr: str  # pandas .query() expression

Transform = TransformSelect | TransformCast | TransformFilter

class QualityNotNull(BaseModel):
    type: Literal["not_null"]
    column: str

class QualityUnique(BaseModel):
    type: Literal["unique"]
    columns: List[str]

class QualityRowCountMin(BaseModel):
    type: Literal["row_count_min"]
    min: int

QualityCheck = QualityNotNull | QualityUnique | QualityRowCountMin

class TargetPostgres(BaseModel):
    type: Literal["postgres"]
    conn_id: str
    table: str
    mode: Literal["append", "replace"] = "append"
    batch_size: int | None = 10000

class Pipeline(BaseModel):
    pipeline_id: str = Field(pattern=r"^[a-zA-Z0-9_]+$")
    schedule: str
    retries: int = 0
    tags: List[str] = []
    source: SourceLocalCSV | SourceS3
    transforms: List[Transform] = []
    target: TargetPostgres
    quality_checks: List[QualityCheck] = []

    @field_validator("schedule")
    @classmethod
    def validate_cron(cls, v: str):
        # very light check; Airflow will validate further
        if not v or not isinstance(v, str):
            raise ValueError("schedule must be a cron string")
        return v
