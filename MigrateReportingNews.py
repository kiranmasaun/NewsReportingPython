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
        # ["hp_clicks", " WHERE created_at >= '{date} 00:00:00' AND created_at < '{today}'"], # Note: It will be uploaded to S3 in MigrateClicksReportingNews.py script
        ["preferred_categories", " WHERE modified_at >= '{date} 00:00:00' and modified_at < '{today}'"],
        ["priority_rank", " WHERE modified_at >= '{date} 00:00:00' "], #removed """ and modified_at < '{today}' """ it was causing issue because calculation happens today.
        ["priority_rank_isp_flags", ""],
        ["web_users_NC", ""],
        ["web_users_TN", ""],
        ["web_users_NB", ""],
        ["blogs", ""],
        ["categories", ""],
        ["blog_headlines", ""],
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

def DeleteDynamoDocs(items, table, primaryKey, secondaryKey = None):
    for i in items:
        response = None
        if isinstance(i, str):
            response = dynamodbClient.delete_item(
                TableName=table,
                Key={
                    primaryKey: {
                        'S': i
                    }
                }
            )
        else:
            response = dynamodbClient.delete_item(
                TableName=table,
                Key={
                    primaryKey: {
                        'S': i[0]
                    },
                    secondaryKey: {
                        'S': i[1]
                    }
                }
            )

        status_code = response['ResponseMetadata']['HTTPStatusCode']
        if status_code != 200:
            print(status_code)
            print(f'error deleting {i} from {table}')
            print(response)
        else:
            print(f'deleted {i} from {table}')

def PullDynamoRequestLogData(pull_from, pull_to, tableName):

    settings = {
            "format": "%d/%b/%Y:%H:%M:%S",
            "split_point": ' +',
            "useCases": ['log', 'get']
        }

    table = dynamodb.Table(tableName)

    response = table.scan()
    data = response['Items']

    i = 0
    while 'LastEvaluatedKey' in response:
        i = i + 1
        if i % 7 == 0:
            print('sleeping')
            time.sleep(3)
        print(f"scan {i} on dynamo")
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        data.extend(response['Items'])

    deleteItems = []
    malformattedItems = []
    preDF = {}
    for item in data:

        # make sure the use case exists in the preDF dict
        if item['useCase'] not in preDF:
            preDF[item['useCase']] = {}

            # load keys into obj
            keysList = list(item.keys())
            for key in keysList:
                if key not in preDF[item['useCase']]:
                    preDF[item['useCase']][key] = []
        else: # ensure same number of columns will exist for the data frame
            if len(item.keys()) != len(preDF[item['useCase']]):
                malformattedItems.append(item)
                continue

        # get date
        d = item['activity_date'].split(settings['split_point'])[0]
        dt = ConvertStringToDatetime(d, settings['format'])

        # add to delete list if older than x days
        if dt is not None and dt < datetime.now()-timedelta(days=60):
            deleteItems.append(item['requestId'])

        # pretty format it for downstream
        item['activity_date'] = dt.strftime("%Y-%m-%d_%H:%M:%S")

        # load data into dataframe after allowing manipulation
        for key, value in item.items():
            preDF[item['useCase']][key].append(value)

        print(dt)
        print(dt.strftime("%Y-%m-%d_%H:%M:%S"))
        print(item)
        pass

    for key, value in preDF.items():
        df = pd.DataFrame(data=value)

        today = date.today()
        rowCount = df.shape[0]
        filePath = f"reporting_export/dynamoDB/{str(today).replace('-', '')}/{tableName}_{key}_{pull_from}_{pull_to}_{str(rowCount)}.csv"
        print('got records for ', tableName)
        WriteToS3(df, filePath, rowCount)
        print('added file to s3', filePath)
        LogJobCompletion(pull_from, today, 1, rowCount, tableName, 0, 1)

    print('malformatted items:')
    for i in malformattedItems:
        print(i)

    DeleteDynamoDocs(deleteItems, tableName, 'requestId')


def PullDynamoUserActivityData(pull_from, pull_to, tableName):

    settings = {
        "format": "%Y-%m-%d"
    }

    table = dynamodb.Table(tableName)

    response = table.scan()
    data = response['Items']

    i = 0
    while 'LastEvaluatedKey' in response:
        i = i + 1
        if i % 7 == 0:
            print('sleeping')
            time.sleep(3)
        print(f"scan {i} on dynamo")
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        data.extend(response['Items'])

    deleteItems = []
    malformattedItems = []
    preDF = {}

    # because there is a high chance of different column / json object count we need to always account for all columns in the df
    allColumns = []
    for item in data:
        keysList = list(item.keys())
        for key in keysList:
            if key not in allColumns:
                allColumns.append(key)
                preDF[key] = []

    for item in data:

        if len(item.keys()) < 3:
            malformattedItems.append(item)
            continue


        # get date
        d = item['activity_date']
        dt = ConvertStringToDatetime(d, settings['format'])

        # add to delete list if older than x days
        if dt is not None and dt < datetime.now()-timedelta(days=60):
            deleteItems.append([item['email_date'], item['activity_date']])

        # pretty format it for downstream
        item['activity_date'] = dt.strftime("%Y-%m-%d")

        # add value to preDF or add '' if doesnt exist
        for c in allColumns:
            if c not in item:
                preDF[c].append('')
            else:
                if '[' in item[c] and ']' in item[c]:
                    preDF[c].append(item[c].replace('[', '').replace(']', '').replace('"','').replace(',', '||'))
                else:
                    preDF[c].append(item[c])

        print(dt)
        print(dt.strftime("%Y-%m-%d"))
        print(item)
        pass

    output = pd.DataFrame(data=preDF)

    today = date.today()
    fileSize = output.shape[0]
    filePath = f"reporting_export/dynamoDB/{str(today).replace('-', '')}/{tableName}_{pull_from}_{pull_to}_{str(fileSize)}.csv"

    print('got records for ', tableName)
    WriteToS3(output, filePath, fileSize)
    print('added file to s3', filePath)
    LogJobCompletion(pull_from, today, 1, fileSize, tableName, 0, 1)

    print('malformatted items:')
    for i in malformattedItems:
        print(i)

    DeleteDynamoDocs(deleteItems, tableName, 'email_date', 'activity_date')

    return output

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

        # year-month-day/table_name
        # yyyymmdd/table_fromDate_toDate_row_count_iteration_n.csv

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
    print(f"Other Jobs Job started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    currentPath = str(pathlib.Path(__file__).parent.resolve())

    # limit = sys.getrecursionlimit()
    # print('Before changing, limit of stack =', limit)
    # Newlimit = 10000
    # sys.setrecursionlimit(Newlimit)
    # limit = sys.getrecursionlimit()
    # print('After changing, limit of stack =', limit)

    runFlag = currentPath + "/runFlag.txt"

    # three_months_ago = datetime.now() - relativedelta(months=3)
    # file_time = datetime.fromtimestamp(os.path.getmtime(runFlag))
    # if file_time < three_months_ago:

    #seperate these:
    # engagement_request_log
    # engagement_user_activity
    #PullDynamoRequestLogData('2024-04-13', '2024-04-17', 'engagement_request_log')
    #PullDynamoUserActivityData('2024-04-13', '2024-04-17', 'engagement_user_activity')

    #sys.exit()

    if os.path.isfile(runFlag):
        today = datetime.today()
        modified_date = datetime.fromtimestamp(os.path.getmtime(runFlag))
        duration = today - modified_date
        if duration.days < 1:
            sys.exit()

    f = open(runFlag, "a")
    f.write("running")
    f.close()

    print("running preferred cat")
    import preferredCat

    print("running headline tagging")
    import HeadlineTagging

    print("running dynamic priority")
    import DynamicPriority

    processesCount = 10
    #
    pool_obj = multiprocessing.Pool(processes=processesCount)
    #
    Driver()
    #
    pool_obj.close()

    print(f"Other Jobs finished at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    os.remove(runFlag)
