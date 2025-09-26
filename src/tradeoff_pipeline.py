import os
import sagemaker
from sagemaker.processing import ScriptProcessor, ProcessingInput, ProcessingOutput
from sagemaker.workflow.steps import ProcessingStep
from sagemaker.workflow.pipeline import Pipeline

def get_pipeline(
    region=None,
    role=None,
    default_bucket=None,
    base_job_prefix="iris-mlops",
):
    region = region or os.environ.get("AWS_REGION", "us-east-1")
    sagemaker_session = sagemaker.session.Session()
    role = role or os.environ["SAGEMAKER_EXECUTION_ROLE_ARN"]
    default_bucket = default_bucket or os.environ.get("S3_ARTIFACT_BUCKET", "iris-mlops-artifacts")

    account_id = os.environ.get("ACCOUNT_ID", "503427799533")
    ecr_image_uri = f"{account_id}.dkr.ecr.{region}.amazonaws.com/iris-mlops:latest"

    # Procesador SageMaker (no pasamos parámetros porque ya están quemados en run_kedro.py)
    kedro_processor = ScriptProcessor(
        image_uri=ecr_image_uri,
        role=role,
        instance_type="ml.t3.medium",
        instance_count=1,
        base_job_name=f"{base_job_prefix}-tradeoff",
        sagemaker_session=sagemaker_session,
        command=["python3"],
    )

    kedro_step = ProcessingStep(
        name="TradeOffBiasVariance",
        processor=kedro_processor,
        code="src/processing/run_kedro.py",
        inputs=[
            ProcessingInput(
                source="conf_mlops",
                destination="/opt/ml/processing/conf_mlops",
                input_name="conf_mlops",
            )
        ],
        outputs=[
            ProcessingOutput(
                source="/opt/ml/processing/output",
                destination=f"s3://{default_bucket}/09-backtesting",
                output_name="backtesting_output",
            )
        ],
    )

    pipeline = Pipeline(
        name="iris-mlops-pipeline-TradeOffBiasVariance",
        steps=[kedro_step],
        sagemaker_session=sagemaker_session,
    )

    return pipeline


if __name__ == "__main__":
    region = os.environ.get("AWS_REGION", "us-east-1")
    role = os.environ["SAGEMAKER_EXECUTION_ROLE_ARN"]
    default_bucket = os.environ.get("S3_ARTIFACT_BUCKET", "iris-mlops-artifacts")
    base_job_prefix = os.environ.get("SAGEMAKER_BASE_JOB_PREFIX", "iris-mlops")

    pipeline = get_pipeline(
        region=region,
        role=role,
        default_bucket=default_bucket,
        base_job_prefix=base_job_prefix,
    )

    print(f"[INFO] Upserting SageMaker Pipeline: {pipeline.name}")
    pipeline.upsert(role_arn=role)
    print(f"[INFO] Pipeline {pipeline.name} creado/actualizado correctamente ✅")
