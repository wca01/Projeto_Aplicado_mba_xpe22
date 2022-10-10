from airflow import DAG

from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
import boto3

aws_access_key_id = Variable.get('aws_access_key_id')
aws_secret_access_key = Variable.get('aws_secret_access_key')
glue = boto3.client('glue', region_name='us-east-2',
                    aws_access_key_id=aws_access_key_id, 
                    aws_secret_access_key=aws_secret_access_key)

from airflow.utils.dates import days_ago

def trigger_crawler_inscricao_func():
        glue.start_crawler(Name='') ### COLOCAR NOME DO CRAWLER

def trigger_crawler_final_func():
        glue.start_crawler(Name='') ### COLOCAR NOME DO CRAWLER



with DAG(
    'olist_batch_spark_k8s',
    default_args={
        'owner': 'WCdeAlmeida',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'max_active_runs': 1,
    },
    description='submit spark-pi as sparkApplication on kubernetes',
    schedule_interval="0 */2 * * *",
    start_date=days_ago(1),
    catchup=False,
    tags=['spark', 'kubernetes', 'batch', 'olist'],
) as dag:
    converte_parquet = SparkKubernetesOperator(
        task_id='converte_parquet',
        namespace="airflow",
        application_file="olist_converte_parquet.yaml",
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=True,
    )

    converte_parquet_monitor = SparkKubernetesSensor(
        task_id='converte_parquet_monitor',
        namespace="airflow",
        application_name="{{ task_instance.xcom_pull(task_ids='converte_parquet')['metadata']['name'] }}",
        kubernetes_conn_id="kubernetes_default",
    )

    entregadores_vendedores = SparkKubernetesOperator(
        task_id='entregadores_vendedores',
        namespace="airflow",
        application_file="olist_entregadores_vendedores.yaml",
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=True,
    )

    entregadores_vendedores_monitor = SparkKubernetesSensor(
        task_id='entregadores_vendedores_monitor',
        namespace="airflow",
        application_name="{{ task_instance.xcom_pull(task_ids='entregadores_vendedores')['metadata']['name'] }}",
        kubernetes_conn_id="kubernetes_default",
    )

    trigger_crawler_inscricao = PythonOperator(
        task_id='trigger_crawler_', ### prenncher crawler
        python_callable=trigger_crawler_?_func,
    )

    olist_join = SparkKubernetesOperator(
        task_id='olist_join',
        namespace="airflow",
        application_file="olist_join.yaml",
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=True,
    )

    olist_join_monitor = SparkKubernetesSensor(
        task_id='olist_join_monitor',
        namespace="airflow",
        application_name="{{ task_instance.xcom_pull(task_ids='olist_join')['metadata']['name'] }}",
        kubernetes_conn_id="kubernetes_default",
    )

    olist_tratamento_dados = SparkKubernetesOperator(
        task_id='olist_tratamento_dados',
        namespace="airflow",
        application_file="olist_tratamento_dados.yaml",
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=True,
    )

    olist_tratamento_dados_monitor = SparkKubernetesSensor(
        task_id='olist_tratamento_dados_monitor',
        namespace="airflow",
        application_name="{{ task_instance.xcom_pull(task_ids='olist_tratamento_dados')['metadata']['name'] }}",
        kubernetes_conn_id="kubernetes_default",
    )


    trigger_crawler_final = PythonOperator(
        task_id='trigger_crawler_final', ### Editar CRAWLER
        python_callable=trigger_crawler_final_func,
    )

converte_parquet >> converte_parquet_monitor >> olist_tratamento_dados >> olist_tratamento_dados_monitor
trigger_crawler_inscricao
olist_tratamento_dados_monitor >> olist_join >> olist_join_monitor
olist_join_monitor >> trigger_crawler_final
[olist_join_monitor, olist_tratamento_dados_monitor] >> entregadores_vendedores >> entregadores_vendedores_monitor


""" converte_parquet >> converte_parquet_monitor >> anonimiza_inscricao >> anonimiza_inscricao_monitor
anonimiza_inscricao_monitor >> trigger_crawler_inscricao
converte_parquet_monitor >> agrega_idade >> agrega_idade_monitor
converte_parquet_monitor >> agrega_sexo >> agrega_sexo_monitor
converte_parquet_monitor >> agrega_notas >> agrega_notas_monitor
[agrega_idade_monitor, agrega_sexo_monitor, agrega_notas_monitor] >> join_final >> join_final_monitor
join_final_monitor >> trigger_crawler_final
[agrega_idade_monitor, agrega_notas_monitor] >> agrega_sexo
[agrega_idade_monitor, agrega_notas_monitor] >> anonimiza_inscricao """
