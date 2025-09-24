import os
import sagemaker
from sagemaker.sklearn.processing import SKLearnProcessor
from sagemaker.processing import ProcessingInput, ProcessingOutput
from sagemaker.workflow.parameters import ParameterString
from sagemaker.workflow.steps import ProcessingStep
from sagemaker.workflow.pipeline import Pipeline


def get_pipeline(
    region=None,
    role=None,
    default_bucket=None,
    base_job_prefix="iris-mlops",
):
    # Sesión de SageMaker
    region = region or os.environ.get("AWS_REGION", "us-east-1")
    sagemaker_session = sagemaker.session.Session()
    role = role or os.environ["SAGEMAKER_EXECUTION_ROLE_ARN"]
    default_bucket = default_bucket or os.environ.get("S3_ARTIFACT_BUCKET", "iris-mlops-artifacts")

    # === Parámetros del pipeline (simulan tus CLI params) ===
    param_product = ParameterString(name="Product", default_value="CDT")
    param_fecha = ParameterString(name="FechaEjecucion", default_value="2025-07-10")
    param_var = ParameterString(name="VariableApertura", default_value="cdt_cant_aper_mes")
    param_target = ParameterString(name="Target", default_value="cdt_cant_ap_group3")

    # === Processor con imagen preconstruida ===
    kedro_processor = SKLearnProcessor(
        framework_version="1.2-1",
        role=role,
        instance_type="ml.t3.medium",
        instance_count=1,
        base_job_name=f"{base_job_prefix}-tradeoff",
        sagemaker_session=sagemaker_session,
    )

    # === Paso único: ejecutar Kedro Backtesting ===
    kedro_step = ProcessingStep(
        name="TradeOffBiasVariance",
        processor=kedro_processor,
        code="src/processing/run_kedro.py",
        inputs=[
            ProcessingInput(
                source=".",  # El repo traído por CodePipeline
                destination="/opt/ml/processing/code",
                input_name="source",
            )
        ],
        outputs=[
            ProcessingOutput(
                source="/opt/ml/processing/output",
                destination=f"s3://{default_bucket}/09-backtesting",
                output_name="backtesting_output",
            )
        ],
        job_arguments=[
            "--product", param_product,
            "--fecha_ejecucion", param_fecha,
            "--variable_apertura", param_var,
            "--target", param_target,
        ],
    )

    # === Pipeline principal ===
    pipeline = Pipeline(
        name="iris-mlops-pipeline-TradeOffBiasVariance",
        parameters=[param_product, param_fecha, param_var, param_target],
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

    print(f"Upserting SageMaker Pipeline: {pipeline.name}")
    pipeline.upsert(role_arn=role)
    print(f"Pipeline {pipeline.name} creado/actualizado correctamente ✅")
