import psycopg2
from os import environ as env
from dotenv import load_dotenv

load_dotenv()

db_host = env["db_host"]
db_port = env["db_port"]
db_database = env["db_database"]
db_user = env["db_user"]
db_password = env["db_password"]

# CREATE_SCHEMA = f"CREATE SCHEMA IF NOT EXISTS {schema};"
# CREATE_TABLE1 = f"CREATE TABLE IF NOT EXISTS {schema}.table1 (...);"
# CREATE_TABLE2 = f"CREATE TABLE IF NOT EXISTS {schema}.table2 (...);"

connection = psycopg2.connect(host=db_host,
                              port=db_port,
                              database=db_database,
                              user=db_user,
                              password=db_password
                              )

# with connection:
#     with connection.cursor() as cursor:
#         cursor.execute(CREATE_SCHEMA)
#         cursor.execute(CREATE_TABLE1)
#         cursor.execute(CREATE_TABLE2)
