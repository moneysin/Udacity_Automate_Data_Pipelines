## Project Introduction

A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow. They have decided to bring you into the project and expect us to create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. They have also noted that the data quality plays a big part when analyses are executed on top the data warehouse and want to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.
The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

## Project Overview

One need to create their own custom operators to perform tasks such as staging the data, filling the data warehouse, and running checks on the data as the final step.
A project template that takes care of all the imports and provides four empty operators that need to be implemented into functional pieces of a data pipeline. The template also contains a set of tasks that need to be linked to achieve a coherent and sensible data flow within the pipeline.
The project has provided  a helpers class that contains all the SQL transformations. Thus, one won't need to write the ETL ourselves, but need to execute it with their custom operators.

![image](https://github.com/user-attachments/assets/40c5e3ce-83df-4258-826b-77057b062b24)

The project files are inside below folders as follows:
1) SQL statements- https://github.com/moneysin/Udacity_Automate_Data_Pipelines/blob/main/final_project_files/udacity/common/final_project_sql_statements.py
2) Stage to Redshift Operator- https://github.com/moneysin/Udacity_Automate_Data_Pipelines/blob/main/final_project_files/final_project_operators/stage_redshift.py
3) load Fact Operator- https://github.com/moneysin/Udacity_Automate_Data_Pipelines/blob/main/final_project_files/final_project_operators/load_fact.py
4) Load Dimension Operator- https://github.com/moneysin/Udacity_Automate_Data_Pipelines/blob/main/final_project_files/final_project_operators/load_dimension.py
5) Data Quality Operator- https://github.com/moneysin/Udacity_Automate_Data_Pipelines/blob/main/final_project_files/final_project_operators/data_quality.py
6) Final project python file- https://github.com/moneysin/Udacity_Automate_Data_Pipelines/blob/main/final_project_files/final_project.py
