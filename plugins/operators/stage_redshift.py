from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
"""
    Custom Operator to load from AWS S3 to Redshift staging table
    
    :redshift_conn_id  Redshift Connection Id Object
    :aws_iam_role:     AWS IAM Role having access on S3 bucket
    :table:            Redshift staging table.
    :s3_bucket:        S3 bucket
    :s3_key:           Files Directory.
    :json:             Schema
"""    
    ui_color = '#358140'
    template_fields = ("s3_key",)
    copy_sql    = """
     COPY {}
        FROM '{}'
        CREDENTIALS 'aws_iam_role={}'
        JSON '{}'
        MAXERROR AS 100
    """
   
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_iam_role="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 json="auto",
                 *args, **kwargs):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_iam_role = aws_iam_role
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json = json

    def execute(self, context):

        #aws_hook = AwsHook(self.aws_credentials_id)
        #credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))

        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            #credentials.access_key,
            #credentials.secret_key,
            self.aws_iam_role,
            self.json
        )
        redshift.run(formatted_sql)



