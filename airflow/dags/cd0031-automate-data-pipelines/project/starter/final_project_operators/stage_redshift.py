from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.hooks.base_hook import BaseHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from udacity.common import final_project_sql_statements


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)
    copy_sql = """
        COPY {schema}.{table}
        FROM '{s3_path}'
        ACCESS_KEY_ID '{access_key}'
        SECRET_ACCESS_KEY '{secret_key}'
        REGION '{region}'
        FORMAT AS {file_format} {copy_options};
    """

    @apply_defaults
    def __init__(self,
                 schema="",
                 table="",
                 create_table_sql=None,
                 s3_bucket="",
                 s3_key="",
                 file_format="",
                 redshift_conn_id="",
                 aws_credential_id="",
                 copy_options=(),
                 region="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.schema = schema
        self.table = table
        self.create_table_sql = create_table_sql
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.file_format = file_format
        self.redshift_conn_id = redshift_conn_id
        self.aws_credential_id = aws_credential_id
        self.copy_options = copy_options
        self.region = region

    def execute(self, context):
        ## Get AWS credentials
        aws_hook = AwsBaseHook(aws_conn_id=self.aws_credential_id, client_type='s3')
        credentials = aws_hook.get_credentials()
        access_key = credentials.access_key
        secret_key = credentials.secret_key

        # Construct the S3 path
        s3_path = f"s3://{self.s3_bucket}/{self.s3_key}"

        #Create the table if create_table_sql is provided
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.create_table_sql:
            self.log.info(f"Creating table {self.schema}.{self.table} if it doesn't exist")
            redshift_hook.run(self.create_table_sql)
        
        # Construct the COPY SQL command dynamically
        copy_options_str = ' '.join(self.copy_options)
        copy_sql = f"""
        COPY {self.schema}.{self.table}
        FROM '{s3_path}'
        ACCESS_KEY_ID '{access_key}'
        SECRET_ACCESS_KEY '{secret_key}'
        REGION '{self.region}'
        FORMAT AS {self.file_format} {copy_options_str};
        """

        # Log the copy command for debugging
        self.log.info(f"Executing COPY command: {copy_sql}")

        # Execute the command on Redshift
        #redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        redshift_hook.run(copy_sql)

        self.log.info(f"Table {self.table} successfully loaded from {s3_path}")
      
    





