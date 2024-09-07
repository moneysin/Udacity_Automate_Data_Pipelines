from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from udacity.common import final_project_sql_statements

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 create_table_sql=None,
                 sql_query="",
                 append_data=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.create_table_sql = create_table_sql
        self.sql_query = sql_query
        self.append_data = append_data

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.create_table_sql:
            self.log.info(f"Creating table {self.table} if it doesn't exist")
            redshift_hook.run(self.create_table_sql)

        # Insert data into the table
        self.log.info(f"Inserting data into dimension table {self.table}")
        redshift_hook.run(self.sql_query)
        self.log.info(f"Fact table {self.table} loaded successfully.")