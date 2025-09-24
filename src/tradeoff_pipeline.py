import os
import sagemaker
from sagemaker.workflow.pipeline import Pipeline
from sagemaker.processing import ScriptProcessor, ProcessingInput
from sagemaker.workflow.parameters import ParameterString

# Parámetros del pipeline
param_product = ParameterString(name="product", default_value="default_product")
param_fecha = ParameterString(name="fecha_ejecucion", default_value="2025-01-01")
param_var = ParameterString(name="variable_apertura", default_value="var")
param_target = ParameterString(name="target", default_value="target")


def get_pipeline(region, role, default_bucket, base_job_prefix):
    sagemaker_session = sagemaker.Session(default_bucket=default_bucket)

    # Processor para ejecutar Kedro
    kedro_processor = ScriptProcessor(
        image_uri=sagemaker.image_uris.retrieve(
            framework="sklearn", region=region, version="1.0-1"
        ),
        command=["python3"],
        role=role,
        instance_type="ml.t3.medium",
        instance_count=1,
        base_job_name=f"{base_job_prefix}-kedro",
        sagemaker_session=sagemaker_session,
    )

    # Paso: ejecutar Kedro
    kedro_step = kedro_processor.run(
        code="src/processing/run_kedro.py",
        inputs=[  # ✅ SOLO UN INPUT
            ProcessingInput(
                source=".",  # todo el repo
                destination="/opt/ml/processing/code",  
                input_name="source",
            ),
        ],
        job_arguments=[
            "--product", param_product,
            "--fecha_ejecucion", param_fecha,
            "--variable_apertura", param_var,
            "--target", param_target,
        ],
    )

    # Definir pipeline
    pipeline = Pipeline(
        name="iris-mlops-pipeline-TradeOffBiasVariance",
        parameters=[param_product, param_fecha, param_var, param_target],
        steps=[kedro_step],
        sagemaker_session=sagemaker_session,
    )

    return pipeline


if __name__ == "__main__":
    region = os.environ.get("AWS_REGION", "us-east-1")
    role = os.environ.get("SAGEMAKER_EXECUTION_ROLE_ARN")
    default_bucket = os.environ.get("S3_ARTIFACT_BUCKET")
    base_job_prefix = os.environ.get("SAGEMAKER_BASE_JOB_PREFIX", "iris-mlops")

    pipeline = get_pipeline(region, role, default_bucket, base_job_prefix)

    print(f"[INFO] Registrando pipeline en SageMaker: {pipeline.name}")
    pipeline.upsert(role_arn=role)
    print(f"[INFO] Pipeline registrado correctamente: {pipeline.name}")
    print("[INFO] Ahora puedes lanzarlo con:")
    print(
        f"aws sagemaker start-pipeline-execution --pipeline-name {pipeline.name} --region {region}"
    )
