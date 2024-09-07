from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tables=[],
                 checks=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables
        self.checks = checks

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        for table in self.tables:
            # Check if the table exists and has rows
            records = redshift.get_records(f"SELECT COUNT(*) FROM {table}")
            if len(records) < 1 or len(records[0]) < 1:
                self.log.error(f"Data quality check failed for {table}: No results returned.")
                raise ValueError(f"Data quality check failed for {table}.")
            num_records = records[0][0]
            if num_records < 1:
                self.log.error(f"Data quality check failed for {table}: Table contains no rows.")
                raise ValueError(f"Data quality check failed for {table}: Table contains no rows.")
            self.log.info(f"Data quality check passed for {table}: {num_records} records found.")

        # Additional checks for custom queries
        for check in self.checks:
            sql_query = check.get('check_sql')
            expected_result = check.get('expected_result')

            self.log.info(f"Running data quality check: {sql_query}")
            query_result = redshift.get_records(sql_query)

            if query_result[0][0] != expected_result:
                self.log.error(f"Data quality check failed: {sql_query}. Expected: {expected_result}, Got: {query_result[0][0]}")
                raise ValueError(f"Data quality check failed: {sql_query}. Expected: {expected_result}, Got: {query_result[0][0]}")
            self.log.info(f"Data quality check passed: {sql_query}")