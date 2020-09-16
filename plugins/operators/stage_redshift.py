from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

"""
This custom operator stages data in Redshift from S3
"""

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)
    copy_sql = """
        copy {}
        from '{}'
        access_key_id '{}'
        secret_access_key '{}'
        {} ;
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table_list="",
                 s3_bucket="",
                 s3_key="",
                 file_format="",
                 file_list="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table_list = table_list
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.aws_credentials_id = aws_credentials_id
        self.file_format=file_format
        self.file_list = file_list

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        form = ""
        
        if self.file_format == "csv":
            form = "region 'us-east-1' compupdate off csv ignoreheader 1 delimiter ','"
        elif self.file_format == "parquet":
            form = "format as parquet"

        self.log.info("Clearing data from destination Redshift tables")
        
        for i in range(len(self.table_list)):
            redshift.run("DELETE FROM {}".format(self.table_list[i]))

            self.log.info("Copying data from S3 to Redshift")
            s3_path = "s3://{}/{}/{}".format(self.s3_bucket, self.s3_key, self.file_list[i])
            formatted_sql = StageToRedshiftOperator.copy_sql.format(
                self.table_list[i],
                s3_path,
                credentials.access_key,
                credentials.secret_key,
                form
            )
            self.log.info(formatted_sql)
            redshift.run(formatted_sql)





