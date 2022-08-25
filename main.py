import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import itertools
import time


def list_tables(dataset_id):
    tables = client.list_tables(dataset_id)

    print("Tables contained in '{}':".format(dataset_id))
    for table in tables:
        print("{}.{}.{}".format(table.project, table.dataset_id, table.table_id))


if __name__ == '__main__':

    start_time = time.time()
    #TODO: Document  Evernote

    credentials = service_account.Credentials.from_service_account_file('c:\\\GoogleKeys\\perceptive-net-341815-a874424537f2.json')

    project_id = 'perceptive-net-341815'
    client = bigquery.Client(credentials=credentials, project=project_id)

    query_job = client.query("""
       SELECT *
       FROM Dataset1.abc
       LIMIT 1000 """)
    results = query_job.result()  # Wait for the job to complete.

    sqlquery = """
       SELECT *
       FROM Dataset1.abc LIMIT 10
       """

    rdf = client.query(sqlquery).to_dataframe()
    print(rdf)

    rowdict = {'a': 6, 'b': 'x', 'c': 'y'}

    rowDf = pd.DataFrame(rowdict, index=[0])

    loopnum = 100000
    # for _ in itertools.repeat(None, loopnum):
    #     rdf = pd.concat([rdf, rowDf])

        #rdf = rdf.append(rowdict, ignore_index = True)

    table_id = "perceptive-net-341815.Dataset1.abc"

    job = client.load_table_from_dataframe(
        rdf, table_id
    )  # Make an API request.
    # Note there is a job_config option that would help loading dataframes as well.
    job.result()  # Wait for the job to complete.

    list_tables(dataset_id="Dataset1")



    table = client.get_table(table_id)  # Make an API request.
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), table_id
         )
    )

    # sql = """
    #    SELECT *
    #    FROM Dataset1.abc
    #    LIMIT 1000 """

    # sql = """
    #     SELECT * FROM `bigquery-public-data.austin_crime.crime` LIMIT 10000
    #     """
    #
    # df = client.query(sql).to_dataframe()
    # print(df)

    print('Success')
    print("--- %s seconds ---" % (time.time() - start_time))

