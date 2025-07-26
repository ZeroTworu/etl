import pendulum

from airflow import DAG
from parser_dag.parser import CategoryProcessor

class ETLPipeline:

    _categories = [
        'malchiki',
        'devochki',
    ]

    def __init__(self, config):
        self.dag_id = config['dag_id']
        self._destination = config['destination']
        self._aws_conn_id = config['aws_conn_id']

        self.dag = self.build_dag()


    def build_dag(self):
        with DAG(
                dag_id=self.dag_id,
                schedule_interval='@daily',
                start_date=pendulum.datetime(2023, 1, 1),
                catchup=False,
                tags=['example']
        ) as dag:
            for category in self._categories:
                processor = CategoryProcessor(
                    category=category,
                    aws_conn_id=self._aws_conn_id,
                    destination=self._destination,
                )
                processor.build()
        return dag



config = {
    'dag_id': 'parse_kerry.su',
    'destination': 's3-mybucket',
    'aws_conn_id': 'S3_ETL_CONN',
}
etl_dag = ETLPipeline(config).dag
