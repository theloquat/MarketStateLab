from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, timedelta, timezone
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

POLICY = "date-plus-2-calendar-days-utc-v1"
FEATURE_VERSION = "observed-changes-20-v1"


def canonical(value) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
        default=str,
    )


def identity(value) -> str:
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def available_on(day: str) -> str:
    """Disclosed reconstruction policy, NOT a verified historic release timestamp."""
    return (date.fromisoformat(day) + timedelta(days=2)).isoformat()


class Observation(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)
    dataset_id: str
    series_id: str
    observation_date: date
    value: float
    quote_kind: Literal["close", "yield"]
    unit: str
    source_file: str
    source_hash: str
    source_row: int = Field(ge=2)
    calendar: str = "unknown"
    availability_policy: str = POLICY
    quality_flags: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def units(self):
        if self.quote_kind == "yield" and self.unit != "percentage_point":
            raise ValueError("yield observations require percentage_point units")
        return self

    @property
    def event_id(self):
        return identity(self.model_dump(mode="json"))


class ReplayRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)
    model_id: str = Field(min_length=1, max_length=64, pattern=r"^[a-f0-9]+$")
    start: date
    end: date
    warning_threshold: float = Field(default=0.75, ge=0.5, le=1)
    cooldown_days: int = Field(default=5, ge=0, le=90)
    rate_per_second: int = Field(default=0, ge=0, le=1000000, strict=True)

    @model_validator(mode="after")
    def interval(self):
        if self.start > self.end:
            raise ValueError("start must not be after end")
        if 0 < self.rate_per_second < 500:
            raise ValueError("rate_per_second must be 0 (unpaced) or at least 500")
        return self
