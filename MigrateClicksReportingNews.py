#!/usr/bin/python

import json
import math
import multiprocessing
import os
import pathlib
import time
import traceback
import pandas as pd
import sys
import mysql.connector
from datetime import date
from datetime import timedelta
from datetime import datetime
import json
from io import StringIO # python3; python2: BytesIO
import boto3
import http.client
import html


dynamodb = boto3.resource("dynamodb",  aws_access_key_id="AKIAQKP5J77Z55HMRSU5",aws_secret_access_key="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",  region_name='us-west-2')
dynamodbClient = boto3.client("dynamodb", aws_access_key_id="AKIAQKP5J77Z55HMRSU5",aws_secret_access_key="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", region_name='us-west-2')

#aws s3 ls s3://lv2-trivia-prod-reporting --recursive --profile lv2-trivia | sort

with open('def.json') as f:
  confData = json.load(f)

db_host = confData['db_host']
db_user = confData['db_user']
db_pass = confData['db_pass']
db_name = confData['db_name']

session = boto3.Session(
    aws_access_key_id= confData['s3_key'],
    aws_secret_access_key= confData['s3_secret'],
)
bucket = confData['bucket_name']


def slack_updates(message):
    conn = http.client.HTTPSConnection("hooks.slack.com")
    payload = json.dumps({
        "text": html.escape(str(message))
    })
    headers = {
        'Content-Type': 'application/json'
    }
    conn.request("POST", "/services/T0L4M9K43/B06H4E1M1FY/L5Nzrre5YDck5FCehfJtoJSv", payload, headers)
    res = conn.getresponse()
    data = res.read()
    print("slack response: ", data.decode("utf-8"))


def GetValue(query):
    """
    Universal f to get records
    :param query:
    :return: array of results (or empty)
    """
    try:
        dbo = mysql.connector.connect(
            host=db_host,
            user=db_user,
            password=db_pass,
            database=db_name
        )

        c = dbo.cursor()

        c.execute(query)
        row = c.fetchone()
        if row == None:
            print("There are no results for this query")
            return False

        dbo.close()

        return row[0]

    except Exception:
        slack_updates("MigrateReportingDataNews.py exception: " + str(Exception))
        print(Exception)
        return False


def GetRecordsIntoDF(query):
    """
    Universal f to get records
    :param query:
    :return: array of results (or empty)
    """

    try:

        dbo = mysql.connector.connect(
            host=db_host,
            user=db_user,
            password=db_pass,
            database=db_name
        )

        c = dbo.cursor()

        result_dataFrame = pd.read_sql(query, dbo)
        #result_dataFrame.to_csv('Test.csv')
        dbo.close()

        return result_dataFrame
    except Exception as e:
        slack_updates("MigrateReportingDataNews.py exception: " + str(e))
        dbo.close()
        print(str(e))


def GetJobLogDate(table):
    dbo = mysql.connector.connect(
        host=db_host,
        user=db_user,
        password=db_pass,
        database=db_name
    )

    c = dbo.cursor()

    query = f"SELECT `to` FROM {db_name}.ExportJobs WHERE success = 1 and complete = 1 and `table` = '{table}' ORDER BY id DESC LIMIT 1"
    c.execute(query)
    row = c.fetchone()
    if not c.rowcount or c.rowcount < 1:
        print("No results found in clicks log")
        return ''

    # disconnecting from server
    dbo.close()

    return ValidateDate(row[0])


def ValidateDate(date_text):
    try:
        res = bool(datetime.strptime(str(date_text), '%Y-%m-%d'))
        return date_text if res else ''
    except ValueError:
        #raise ValueError("Incorrect data format, should be YYYY-MM-DD")
        print("Incorrect data format, should be YYYY-MM-DD")
        return ''


def LogJobCompletion(from_date, to_date, success, count, table, iteration = 0, complete = 1):
    dbo = mysql.connector.connect(
        host=db_host,
        user=db_user,
        password=db_pass,
        database=db_name
    )

    c = dbo.cursor()

    sql = f"INSERT INTO {db_name}.ExportJobs (`from`, `to`, `success`, `count`, `table`, `iteration`, `complete`) VALUES ('{from_date}', '{to_date}', {success}, {count}, '{table}', {iteration}, {complete})"

    try:
        c.execute(sql)
        dbo.commit()
        print(c.rowcount, "records inserted.")
    except ValueError:
        dbo.rollback()
        slack_updates("MigrateReportingDataNews.py exception: " + str(ValueError))
        print("error, rolled back")
        print(ValueError)

    dbo.close()


def GetDatesDifference(date1, date2):
    d1 = datetime.strptime(date1.strftime('%Y-%m-%d'), "%Y-%m-%d")
    d2 = datetime.strptime(date2.strftime('%Y-%m-%d'), "%Y-%m-%d")
    return abs((d2 - d1).days)


def ShowS3Files():
    # print contents to console
    try:
        s3_resource = session.resource('s3')
        bucketContent = s3_resource.Bucket(bucket)

        print("bucket contents:")
        for bo in bucketContent.objects.all():
            print(bo.key)
    except Exception as e:
        print(e)
        slack_updates("MigrateReportingDataNews.py exception: " + str(e))


def WriteToS3(df, name, fileSize):

    if df.empty: return

    currentPath = str(pathlib.Path(__file__).parent.resolve())

    # result_dataFrame.to_csv('Test.csv')

    if fileSize > 1000000:
        s3_resource = session.client('s3')
        tempFileName = name.split('/')[-1]
        tempFilePath = currentPath + '/' + tempFileName
        df.to_csv(tempFilePath)
        s3_resource.upload_file(tempFilePath, bucket, name)
        try:
            os.remove(tempFilePath)
        except OSError:
            pass
    else:
        s3_resource = session.resource('s3')
        csv_buffer = StringIO()
        df.to_csv(csv_buffer)
        s3_resource.Object(bucket, name).put(Body=csv_buffer.getvalue())
        #s3_resource.upload_file(name, bucket)


def Driver():

    global processesCount

    #table: where clause
    q = [
        ["hp_clicks", " WHERE created_at >= '{date} 00:00:00' AND created_at < '{today}'"]
    ]

    processesCount = len(q)

    pool_obj.map(PullData, q)  # ONLY check unsuccessful words


def ConvertStringToDatetime(date_time, format):
    try:
        datetime_str = datetime.strptime(date_time, format)

        return datetime_str
    except Exception as e:
        print(e)
        return None


def PullData(pullConfig):

    try:
        table = pullConfig[0]
        query = pullConfig[1]

        queryMaxLimit = 1000000

        last_pull = GetJobLogDate(table)

        today = date.today()
        yesterday = today - timedelta(days=1)
        # yesterday = yesterday.strftime('%Y-%m-%d')

        if (last_pull == ''): last_pull = yesterday

        delta = GetDatesDifference(today, last_pull)
        print("delta is " + str(delta))
        if (delta < 1):
            print("already pulled for yesterday, canceling...")
            # sys.exit()
            return

        pull_from = today - timedelta(days=delta)

        selectBlock = f"SELECT * FROM {db_name}.{table} "
        countBlock = f"SELECT COUNT(*) FROM {db_name}.{table} "
        limitBlock = " LIMIT {skip}, {pageSize} ;"

        whereClause = query.replace('{date}', str(pull_from)).replace('{today}', str(today))

        rowCount = GetValue(countBlock + whereClause)

        #dynamo here

        filePath = f"reporting_export/News/{str(today).replace('-', '')}/{table}_{pull_from}_{today}_{str(rowCount)}.csv"

        if rowCount > queryMaxLimit:
            pages = math.ceil(rowCount / queryMaxLimit)
            print("starting " + str(pages) + " iterations:")
            skip = 0
            for x in range(0, pages):
                tQuery = selectBlock + whereClause + limitBlock.replace('{skip}', str(skip)).replace('{pageSize}',
                                                                                                     str(queryMaxLimit))
                print('working on ', table, tQuery)
                data = GetRecordsIntoDF(tQuery)
                fileSize = data.shape[0]
                print('got records for ', table, 'record count: ', str(fileSize))
                WriteToS3(data, filePath.replace('.csv', '_iteration_' + str(x) + '.csv'), fileSize)
                print('added file to s3', filePath)
                LogJobCompletion(pull_from, today, 1, fileSize, table, str(x), (1 if pages - 1 == x else 0))
                skip = skip + queryMaxLimit

        else:
            fQuery = selectBlock + whereClause
            print('working on ', table, fQuery)
            data = GetRecordsIntoDF(fQuery)
            fileSize = data.shape[0]
            print('got records for ', table)
            WriteToS3(data, filePath, fileSize)
            print('added file to s3', filePath)
            LogJobCompletion(pull_from, today, 1, fileSize, table, 0, 1)

        print("Finished exporting / uploading files for ", table)
        # ShowS3Files()
    except Exception as e:
        print("error found")
        slack_updates("MigrateReportingDataNews.py exception: " + str(e))
        print(e)
        traceback.print_exc()


if __name__ == '__main__' or '_RunProcess.py' in sys.argv[0]:
    print(f"hp_clicks Job started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    currentPath = str(pathlib.Path(__file__).parent.resolve())

    runFlag = currentPath + "/runFlag.txt"

    if os.path.isfile(runFlag):
        today = datetime.today()
        modified_date = datetime.fromtimestamp(os.path.getmtime(runFlag))
        duration = today - modified_date
        if duration.days < 1:
            sys.exit()

    f = open(runFlag, "a")
    f.write("running")
    f.close()

    print("running clicks")
    import NewsClickActivity

    processesCount = 10
    #
    pool_obj = multiprocessing.Pool(processes=processesCount)
    #
    Driver()
    #
    pool_obj.close()

    print(f"hp_clicks Job finished at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    os.remove(runFlag)

