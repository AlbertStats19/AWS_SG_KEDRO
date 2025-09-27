# src/processing/pipeline_registry.py
"""Project pipelines."""

from __future__ import annotations
from kedro.pipeline import Pipeline

# 👉 Reexporta desde TU paquete real (no desde processing)
from data_bbog_integration_fabrica_personas.pipelines.backtesting import (
    create_pipeline as backtesting_pipeline,
)

def register_pipelines() -> dict[str, Pipeline]:
    pipelines = {
        "backtesting": backtesting_pipeline(),
    }
    pipelines["__default__"] = pipelines["backtesting"]
    return pipelines
