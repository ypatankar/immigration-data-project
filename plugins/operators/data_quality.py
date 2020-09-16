from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

import logging

"""
This custom operator runs quality checks on a list of tables
"""

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 qa_check_list=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.qa_check_list = qa_check_list
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        error_count = 0
        failing_tests = []
        
        redshift_hook = PostgresHook(self.redshift_conn_id)
        for check in self.qa_check_list:
            sql = check.get('check_sql')
            exp_result = check.get('expected_result')
            records = redshift_hook.get_records(sql)[0]
            
            if exp_result != records[0]:
                error_count += 1
                failing_tests.append(sql)
                
        if error_count > 0:
            self.log.info('Tests failed')
            self.log.info(failing_tests)
            raise ValueError('Data quality check failed') 
        else:
            self.log.info('Tests passed')
