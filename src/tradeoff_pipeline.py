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
    """Define el pipeline TradeOffBiasVariance (basado en Kedro)."""

    # Sesión de SageMaker
    region = region or os.environ.get("AWS_REGION", "us-east-1")
    role = role or os.environ["SAGEMAKER_EXECUTION_ROLE_ARN"]
    default_bucket = default_bucket or os.environ.get("S3_ARTIFACT_BUCKET", "iris-mlops-artifacts")

    sagemaker_session = sagemaker.session.Session()

    # Parámetros dinámicos
    param_product = ParameterString(name="Product", default_value="CDT")
    param_fecha = ParameterString(name="FechaEjecucion", default_value="2025-07-10")
    param_var = ParameterString(name="VariableApertura", default_value="cdt_cant_aper_mes")
    param_target = ParameterString(name="Target", default_value="cdt_cant_ap_group3")

    # Processor SKLearn (sin ECR, solo imagen pública)
    processor = SKLearnProcessor(
        framework_version="1.2-1",
        role=role,
        instance_type="ml.t3.medium",
        instance_count=1,
        sagemaker_session=sagemaker_session,
        base_job_name=f"{base_job_prefix}-tradeoff",
    )

    # Paso único del pipeline (ejecuta Kedro)
    step = ProcessingStep(
        name="TradeOffBiasVariance",
        processor=processor,
        inputs=[
            ProcessingInput(
                source=".",  # El código viene del artifact de CodePipeline
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
            "--product", param_product.to_string(),
            "--fecha_ejecucion", param_fecha.to_string(),
            "--variable_apertura", param_var.to_string(),
            "--target", param_target.to_string(),
        ],
    )

    # Definir pipeline
    pipeline = Pipeline(
        name="iris-mlops-pipeline-TradeOffBiasVariance",
        parameters=[param_product, param_fecha, param_var, param_target],
        steps=[step],
        sagemaker_session=sagemaker_session,
    )

    return pipeline


if __name__ == "__main__":
    """Entrypoint para CodeBuild -> registra el pipeline en SageMaker."""

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

    print(f"\n[INFO] Registrando pipeline en SageMaker: {pipeline.name}")
    pipeline.upsert(role_arn=role)
    print(f"[INFO] Pipeline registrado correctamente: {pipeline.name}")
    print("[INFO] Ahora puedes lanzarlo con:")
    print(f"aws sagemaker start-pipeline-execution "
          f"--pipeline-name {pipeline.name} --region {region}")
