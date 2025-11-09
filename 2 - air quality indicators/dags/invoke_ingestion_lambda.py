"""
DAG de ejemplo para invocar la Lambda de ingestión desde MWAA.
Requisitos:
- El provider 'apache-airflow-providers-amazon' debe estar instalado en el entorno MWAA.
  (añádelo a requirements.txt del entorno si es necesario)
- MWAA debe tener permiso `lambda:InvokeFunction` sobre la función.
- La Lambda debe permitir invocaciones desde MWAA (aws_lambda_permission con source_arn o principal adecuado).

El DAG usa AwsLambdaInvokeFunctionOperator. Si tu MWAA no tiene el provider instalado, usa la alternativa comentada con PythonOperator.
"""
from datetime import datetime
from airflow import DAG
from airflow.utils.dates import days_ago

# Intentamos importar el operador dedicado; si no está instalado, hay una alternativa abajo.
try:
    from airflow.providers.amazon.aws.operators.lambda_function import AwsLambdaInvokeFunctionOperator
    has_lambda_operator = True
except Exception:
    has_lambda_operator = False

DEFAULT_ARGS = {
    'owner': 'air-quality',
    'depends_on_past': False,
}

with DAG(
    dag_id='invoke_ingestion_lambda',
    default_args=DEFAULT_ARGS,
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=['integration','lambda']
) as dag:

    if has_lambda_operator:
        invoke = AwsLambdaInvokeFunctionOperator(
            task_id='invoke_ingestion',
            function_name='air-quality-indicators-dev-ingestion',  # cambiar por ARN si prefieres
            payload={},  # puedes pasar parámetros aquí
            invocation_type='RequestResponse',
            log_type='Tail'
        )

    else:
        # Alternativa si el provider amazon no está instalado en MWAA:
        from airflow.operators.python import PythonOperator
        import boto3

        def _invoke_lambda_py(**context):
            client = boto3.client('lambda', region_name='us-east-1')
            resp = client.invoke(FunctionName='air-quality-indicators-dev-ingestion', InvocationType='RequestResponse')
            return resp.get('StatusCode')

        invoke = PythonOperator(
            task_id='invoke_ingestion_python',
            python_callable=_invoke_lambda_py
        )

    invoke
