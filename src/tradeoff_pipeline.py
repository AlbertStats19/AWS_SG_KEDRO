import os
import sagemaker
from sagemaker.processing import ScriptProcessor, ProcessingInput, ProcessingOutput
from sagemaker.workflow.parameters import ParameterString
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

    # Parámetros
    param_product = ParameterString(name="Product", default_value="CDT")
    param_fecha = ParameterString(name="FechaEjecucion", default_value="2025-07-10")
    param_var = ParameterString(name="VariableApertura", default_value="cdt_cant_aper_mes")
    param_target = ParameterString(name="Target", default_value="cdt_cant_ap_group3")

    kedro_processor = ScriptProcessor(
        image_uri=ecr_image_uri,
        role=role,
        instance_type="ml.t3.medium",
        instance_count=1,
        base_job_name=f"{base_job_prefix}-tradeoff",
        sagemaker_session=sagemaker_session,
        command=["python3"],
        env={
            "KEDRO_CONF_DIR": "/opt/ml/processing/conf_mlops",
            "KEDRO_ENV": "base",
            "PARAM_PRODUCT": param_product,
            "PARAM_FECHA_EJECUCION": param_fecha,
            "PARAM_VARIABLE_APERTURA": param_var,
            "PARAM_TARGET": param_target,
        },
    )

    # 👇 Aquí la corrección: el parámetro "code" debe apuntar a un archivo .py, no a un directorio
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

    print(f"[INFO] Upserting SageMaker Pipeline: {pipeline.name}")
    pipeline.upsert(role_arn=role)
    print(f"[INFO] Pipeline {pipeline.name} creado/actualizado correctamente ✅")
