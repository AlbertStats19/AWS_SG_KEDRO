from src.tradeoff_pipeline import get_pipeline
import os
import sagemaker

def main():
    sess = sagemaker.session.Session()
    pipeline = get_pipeline(
        region=os.environ.get("AWS_REGION"),
        role=os.environ["SAGEMAKER_EXECUTION_ROLE_ARN"],
        default_bucket=os.environ.get("S3_ARTIFACT_BUCKET"),
        base_job_prefix=os.environ.get("SAGEMAKER_BASE_JOB_PREFIX", "iris-mlops")
    )
    pipeline.upsert(role_arn=os.environ["SAGEMAKER_EXECUTION_ROLE_ARN"])
    print("Pipeline upserted: iris-mlops-pipeline-TradeOffBiasVariance")

if __name__ == "__main__":
    main()
