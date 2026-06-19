#!/usr/bin/python

import os
import pandas as pd
import sys
import mysql.connector
from datetime import date
from datetime import timedelta
from datetime import datetime
import json
import http.client
import html
import pytz

__location__ = os.path.realpath(
    os.path.join(os.getcwd(), os.path.dirname(__file__)))

with open(os.path.join(__location__, 'def.json')) as f:
  confData = json.load(f)

db_host = confData['db_host']
db_user = confData['db_user']
db_pass = confData['db_pass']
db_name = confData['db_name']

hp_configs = confData['hp_configs']

sys_args = []

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

def ValidateDate(date_text):
    try:
        res = bool(datetime.strptime(str(date_text), '%Y-%m-%d'))
        return date_text if res else ''
    except ValueError:
        #raise ValueError("Incorrect data format, should be YYYY-MM-DD")
        print("Incorrect data format, should be YYYY-MM-DD")
        return ''


def GetClicksLog():
    dbo = mysql.connector.connect(
        host=db_host,
        user=db_user,
        password=db_pass,
        database=db_name
    )

    c = dbo.cursor()

    query = f"SELECT `to` FROM {db_name}.hp_clicks_jobs WHERE success = 1 AND hp_campaign_id = {confData['hp_campaign_id']} ORDER BY id DESC LIMIT 1"
    #print(query)
    c.execute(query)
    row = c.fetchone()
    print(c.rowcount)
    if (not c.rowcount) or (c.rowcount < 1):
        print("No results found in clicks log")
        return ''




    # disconnecting from server
    dbo.close()

    return ValidateDate(row[0])


def LogClicksJob(from_date, to_date, success, count):
    dbo = mysql.connector.connect(
        host=db_host,
        user=db_user,
        password=db_pass,
        database=db_name
    )

    c = dbo.cursor()
   
   # 2025-12-10 changed to_date to from_date
    sql = f"INSERT INTO {db_name}.hp_clicks_jobs (`from`, `to`, `success`, `count`, `hp_campaign_id`) VALUES ('{from_date}', '{to_date}', {success}, {count}, {confData['hp_campaign_id']})"

    try:
        c.execute(sql)
        dbo.commit()
        print(c.rowcount, "records inserted.")
    except ValueError as e:
        slack_updates("NewsClickActivity.py exception: " + str(e))
        dbo.rollback()
        print("error, rolled back")
        print(ValueError)

    dbo.close()


def GetDatesDifference(date1, date2):
    d1 = datetime.strptime(date1.strftime('%Y-%m-%d'), "%Y-%m-%d")
    d2 = datetime.strptime(date2.strftime('%Y-%m-%d'), "%Y-%m-%d")
    return abs((d2 - d1).days)

def GetCSVDataFromUrl(pull_from, pull_to):
    data = None
    success = False
    frames = []
    url = ''
    for x in range(0, 3):
        try:
            if success == True: break
            print("HP csv grab. attempt: " + str(x + 1) + "/10")
            url = f'http://reporting.wallatrax.com/api.php?type=Clicks&key=81122047e3cc53bf900bd97cfd945aa0&start={pull_from}&end={pull_to}&agents={confData["hp_agents"]}'

            data = pd.read_csv(
                url,
                dtype={'campaignid': int, 'date': str, 'c1': str, 'c2': str, 'c3': str, 'ip': str, 'amount': float,
                       'hitid': int, 'agentid': int}
            )
            success = True
        
        # below I need to add an exception to the pd - sometimes the csv returns empty with only a row of headers, and a row of empty values. this causes pandas to error out. I need to catch if it comes back as that empty, and not retry, and fill the first row after the headers with dummy data that suits the dtypes.
        except (pd.errors.EmptyDataError, pd.errors.ParserError, ValueError, TypeError):
            print("HP csv grab returned empty data. filling with dummy row.")
            data = pd.DataFrame([{'campaignid': 0, 'date': '', 'c1': '', 'c2': '', 'c3': '', 'ip': '', 'amount': 0.0, 'hitid': 0, 'agentid': 0}])
            success = True

        except Exception as e:
            print(url)
            print("HP csv grab failed. trying again: " + str(x + 1) + "/4")
            print(e)
            slack_updates("NewsClickActivity.py exception: " + str(e))
            data = None

    if data is None:
        # try recursive
        print("tried getting large CSV, but HP failed. trying again by chunks")
        cont = True


        c_from = pull_from
        c_to = pull_from + timedelta(days=1)
        while cont:
            try:
                success = False
                for x in range(0, 3):
                    if success == True: break
                    print(f"HP csv grab. trying dates {c_from} and {c_to}")
                    data = pd.read_csv(
                        f'http://reporting.wallatrax.com/api.php?type=Clicks&key=81122047e3cc53bf900bd97cfd945aa0&start={c_from}&end={c_to}&agents={confData["hp_agents"]}',
                        dtype={'campaignid': int, 'date': str, 'c1': str, 'c2': str, 'c3': str, 'ip': str, 'amount': float, 'hitid': int, 'agentid': int}
                    )
                    frames.append(data)
                    success = True
                    print(f"HP csv grab. success for dates {c_from} and {c_to}")

                if success is False:
                    print(f"HP csv grab. failed for dates {c_from} and {c_to}")
                    return False

                # exit loop
                if c_to == pull_to:
                    cont = False
                #mark dates for next iteration
                c_from = c_to
                c_to = c_to + timedelta(days=1)
            except Exception as e:
                print(e)
                print("HP CSV Grab failed. quitting")
                return False

        data = pd.concat(frames)
        return data
    else:
        return data

# return: [data array (q_id, choice, email hash), array length, pull from, pull until]
def GetClicks():

    last_pull_value = GetClicksLog()

    today = date.today()
    yesterday = today - timedelta(days=1)
    #yesterday = yesterday.strftime('%Y-%m-%d')

    last_pull = None
    if(last_pull_value == ''):
        last_pull = yesterday
    else:
        last_pull = last_pull_value

    delta = GetDatesDifference(today, last_pull)
    print("delta is " + str(delta))
    if(delta < 2 and 'allow_same_day_pull' not in sys_args and last_pull_value != ''):
        print("already pulled for yesterday, canceling...")
        return []

    pull_from = today - timedelta(days=delta)

    # overwriteEndDate = '2023-12-30'
    # yesterday = overwriteEndDate
   
   # 2025-12-10 changed to fetch only 1 day click from hitpath
   #  data = GetCSVDataFromUrl(pull_from, yesterday)
    pull_from = pull_from + timedelta(days=1)

    data = GetCSVDataFromUrl(pull_from, yesterday)
    # data = GetCSVDataFromUrl(pull_from, pull_from)

    if data is False or data is None:
        print("HP CSV Grab failed. quitting")
        return


    print("downloaded CSV")
    print(f'http://reporting.wallatrax.com/api.php?type=Clicks&key=81122047e3cc53bf900bd97cfd945aa0&start={pull_from}&end={yesterday}&agents={confData["hp_agents"]}')

    count = 0
    data_a = []

    for i, row in data.iterrows():
        count = i

        #print(row['c3'])

        if row['campaignid'] != confData['hp_campaign_id']:
            continue
        elif (pd.isna(row['c3'])):
            #print('empty value')
            continue
        elif ('choice' not in str(row['c3']) or pd.isna(row['c2'])):
            #print('no proper format')
            continue
        else:
            e_hash = qualifyEmailHashFromC2(row['c2'])
            if(e_hash == False): continue
            # validate the right value is there
            # NEW FORMAT: q_id-q_choiceX-q_position -> posX
            # 1K62E5d1A2nkjBgGDuox-choice1-position1
            a = row['c3'].replace('choice', '').replace('position', '').replace('pos', '').split('-')

            # if less than 3 arguments it means pos wasnt there, add 0 for backwards compatibility
            if(len(a) < 3): a.append(0)
            a.append(e_hash)
            #q_id, choice_selected, q_position, e_hash
            # add c1, c2, c3, hitid
            a.append(row['c1'])
            a.append(row['c2'])
            a.append(row['c3'])
            a.append(row['hitid'])
            a.append(row['date'])
            data_a.append(a)
            # print(a)

    # sys.exit()
    #print(data_a)

    l = len(data_a)

    # log if there is nothing as success
    if(l < 1): LogClicksJob(pull_from, yesterday, 1, l)

    return [data_a, l, pull_from, yesterday]

def qualifyEmailHashFromC2(s):
    if('-' not in s or 'email_hash' in s): return False
    e = s.split('-')[1]
    if(e == ''): return False
    return e



def insertClicks(data):
    if (data[1] == 0): return

    dbo = mysql.connector.connect(
        host=db_host,
        user=db_user,
        password=db_pass,
        database=db_name
    )
    current_timestamp = datetime.now(pytz.timezone('America/Los_Angeles')).strftime('%Y-%m-%d %H:%M:%S')
    sql = f"INSERT INTO {db_name}.hp_clicks (q_id, choice_selected, q_position, e_hash, c1, c2, c3, hitid, created_at, cid) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, {confData['hp_campaign_id']})"
    mysql_limit_int = 990

    if(data[1] < mysql_limit_int): #mysql has a limit
        c = dbo.cursor()

        try:
            c.executemany(sql, data[0])
            dbo.commit()
            print(c.rowcount, "records inserted.")
            LogClicksJob(data[2], data[3], 1, data[1])
        except Exception as e:
            dbo.rollback()
            slack_updates("NewsClickActivity.py exception: " + str(e))
            print(c.rowcount, "records not inserted, rolling back.")
            LogClicksJob(data[2], data[3], 0, data[1])

    else:

        temp_a = []
        i = 0 # record count
        x = 0 # iteration count
        for r in data[0]:
            i = i+1
            temp_a.append(r)
            if(i % mysql_limit_int == 0 or i == data[1]):
                x = x+1
                c = dbo.cursor()
                try:
                    c.executemany(sql, temp_a)
                    dbo.commit()
                    print(c.rowcount, "records inserted. Iteration: " + str(x))
                    temp_a = []
                except TypeError as e:
                    slack_updates("NewsClickActivity.py exception: " + str(e))
                    dbo.rollback()
                    print(c.rowcount, "incremental issue, rolling back. Iteration: " + str(x))
                    LogClicksJob(data[2], data[3], 0, data[1])
                    break

        LogClicksJob(data[2], data[3], 1, data[1])

    dbo.close()

def ExtractArgs():
    """
    Extracts the arguments from the system arguments param
    Accepted arguments:
    ['allow_same_day_pull']
    :return:
    """
    for arg in sys.argv:
        sys_args.append(arg)

print("Starting News Click Activity")
ExtractArgs()
# c = [data array (q_id, choice, email hash), array length, pull from, pull until]
print(hp_configs)
for x in hp_configs:
    print(f"working on campaign {x}")

    confData['hp_campaign_id'] = hp_configs[x]['hp_campaign_id']
    confData['hp_agents'] = hp_configs[x]['hp_agents']

    c = GetClicks()
    if c is not None and len(c) > 0:
        insertClicks(c)


# c = GetClicks()
# if len(c) > 0:
#     insertClicks(c)

print("Ending News Click Activity")

