import json
import boto3


def lambda_handler(event, context):
    glue = boto3.client('glue')
    job_name = 'bovespa_glue_job'

    try:
        response = glue.start_job_run(JobName=job_name)

        job_run_id = response['JobRunId']
        print(
            f"Job {job_name} iniciado com sucesso: {response} - JobRunId: {job_run_id}")

        return {
            'statusCode': 200,
            'body': json.dumps(f"Job {job_name} iniciado com sucesso: {response} - JobRunId: {job_run_id}")
        }

    except Exception as e:
        print(f"Erro ao iniciar o job {job_name}: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"Erro ao iniciar o job {job_name}: {e}")
        }
