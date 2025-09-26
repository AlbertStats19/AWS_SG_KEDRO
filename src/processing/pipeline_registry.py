# src/processing/pipeline_registry.py
"""Project pipelines."""

from __future__ import annotations
from kedro.pipeline import Pipeline
from processing.pipelines.backtesting import create_pipeline as backtesting_pipeline


def register_pipelines() -> dict[str, Pipeline]:
    """Register the project's pipelines.

    Returns:
        A mapping from pipeline names to ``Pipeline`` objects.
    """
    pipelines = {
        "backtesting": backtesting_pipeline()
    }
    pipelines["__default__"] = pipelines["backtesting"]
    return pipelines
