import os
import sagemaker
from sagemaker.sklearn.processing import SKLearnProcessor
from sagemaker.workflow.parameters import ParameterString
from sagemaker.processing import ProcessingInput, ProcessingOutput
from sagemaker.workflow.steps import ProcessingStep
from sagemaker.workflow.pipeline import Pipeline


def get_pipeline(
    region=None,
    role=None,
    default_bucket=None,
    base_job_prefix="iris-mlops",
):
    region = region or os.environ.get("AWS_REGION", "us-east-1")
    sagemaker_session = sagemaker.Session()
    role = role or os.environ["SAGEMAKER_EXECUTION_ROLE_ARN"]
    default_bucket = default_bucket or os.environ.get("S3_ARTIFACT_BUCKET", "iris-mlops-artifacts")

    # === Parámetros de entrada para replicar tu CLI ===
    param_product = ParameterString(name="Product", default_value="CDT")
    param_fecha   = ParameterString(name="FechaEjecucion", default_value="2025-07-10")
    param_var     = ParameterString(name="VariableApertura", default_value="cdt_cant_aper_mes")
    param_target  = ParameterString(name="Target", default_value="cdt_cant_ap_group3")

    # === Processor con imagen sklearn preconstruida ===
    processor = SKLearnProcessor(
        framework_version="1.2-1",
        role=role,
        instance_type="ml.t3.medium",
        instance_count=1,
        sagemaker_session=sagemaker_session,
        base_job_name=f"{base_job_prefix}-tradeoff",
    )

    step = ProcessingStep(
        name="TradeOffBiasVariance",
        processor=processor,
        inputs=[
            ProcessingInput(
                source=".",
                destination="/opt/ml/processing/code",
                input_name="source",
            ),
        ],
        outputs=[
            ProcessingOutput(
                source="/opt/ml/processing/code",
                destination=f"s3://{default_bucket}/tradeoff/artifacts/",
                output_name="artifacts",
            )
        ],
        code="/opt/ml/processing/code/src/processing/run_kedro.py",
        job_arguments=[
            f"--PARAM_PRODUCT={param_product}",
            f"--PARAM_FECHA_EJECUCION={param_fecha}",
            f"--PARAM_VARIABLE_APERTURA={param_var}",
            f"--PARAM_TARGET={param_target}",
        ],
    )

    pipeline = Pipeline(
        name="iris-mlops-pipeline-TradeOffBiasVariance",
        parameters=[param_product, param_fecha, param_var, param_target],
        steps=[step],
        sagemaker_session=sagemaker_session,
    )
    return pipeline


if __name__ == "__main__":
    region = os.environ.get("AWS_REGION")
    role = os.environ.get("SAGEMAKER_EXECUTION_ROLE_ARN")
    default_bucket = os.environ.get("S3_ARTIFACT_BUCKET")
    base_job_prefix = os.environ.get("SAGEMAKER_BASE_JOB_PREFIX", "iris-mlops")

    if not all([region, role, default_bucket]):
        print("ERROR: faltan variables de entorno necesarias para registrar el pipeline.")
        import sys
        sys.exit(1)

    pipeline = get_pipeline(region=region, role=role, default_bucket=default_bucket, base_job_prefix=base_job_prefix)

    print(f"\nBucket en uso por este pipeline: {default_bucket}\n")
    print(f"Upserting SageMaker Pipeline: {pipeline.name}")
    pipeline.upsert(role_arn=role)

    print("Pipeline registrado correctamente en SageMaker. ✅")
    print("Recuerda: el start del pipeline lo lanzas manualmente desde la consola o CLI.")
