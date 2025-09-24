import os
from sagemaker.sklearn.processing import SKLearnProcessor
from sagemaker.workflow.parameters import ParameterString
from sagemaker.processing import ProcessingInput, ProcessingOutput
from sagemaker.workflow.steps import ProcessingStep
from sagemaker.workflow.pipeline import Pipeline
from sagemaker.session import Session
import sagemaker

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

    # === Parámetros de entrada para replicar tu CLI ===
    param_product = ParameterString(name="Product", default_value="CDT")
    param_fecha   = ParameterString(name="FechaEjecucion", default_value="2025-07-10")
    param_var     = ParameterString(name="VariableApertura", default_value="cdt_cant_aper_mes")
    param_target  = ParameterString(name="Target", default_value="cdt_cant_ap_group3")

    # === Processor con imagen preconstruida (sin ECR) ===
    processor = SKLearnProcessor(
        framework_version="1.2-1",   # versión estable (ajústala si la imagen cambia)
        role=role,
        instance_type="ml.t3.medium",
        instance_count=1,
        sagemaker_session=sagemaker_session,
        base_job_name=f"{base_job_prefix}-tradeoff",
    )

    # === Inputs/Outputs del job (el código y config van desde CodePipeline/Source) ===
    step = ProcessingStep(
        name="TradeOffBiasVariance",
        processor=processor,
        inputs=[
            ProcessingInput(
                source=sagemaker_session.default_bucket() if default_bucket is None else default_bucket,
                destination="/opt/ml/processing/input",   # opcional si quieres inyectar algo
                input_name="empty",                        # placeholder si no usas inputs
            ),
            ProcessingInput(
                source=".",                                # el repo descargado por CodeBuild -> CodePipeline artifact
                destination="/opt/ml/processing/code",
                input_name="source",
            ),
        ],
        outputs=[
            ProcessingOutput(
                source="/opt/ml/processing/code",          # si tu pipeline deja artefactos ahí
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
