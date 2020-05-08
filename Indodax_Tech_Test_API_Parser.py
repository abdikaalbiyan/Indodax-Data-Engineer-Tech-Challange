import json
import pandas as pd
import requests
import warnings
import datetime
import pytz
import psycopg2 as ps
import pandas.io.sql as psql
import schedule
import time
from datetime import datetime


def store_data(date, price, amount, tid, types):

    db = ps.connect(database="indodax_tech_test",
                    user="postgres",
                    password="smanivda",
                    host="127.0.0.1",
                    port="5433")
    cursor = db.cursor()

    insert_query = "INSERT INTO api(date, price, amount, tid, types) VALUES (%s, %s, %s, %s, %s)"

    # tid_exits = cursor.execute("SELECT tid FROM api WHERE tid=%s", (tid))

#    if tid_exits == 0:
    cursor.execute(insert_query, (date, price, amount, tid, types))

    db.commit()
    cursor.close()
    db.close()
    return


def run_store_data():

    url = 'https://indodax.com/api/trades'

    jsondata = requests.get(url).json()

    for i in range(len(jsondata)):
        date = jsondata[i]["date"]
        price = jsondata[i]["price"]
        amount = jsondata[i]["amount"]
        tid = jsondata[i]["tid"]
        types = jsondata[i]["type"]

        return store_data(date, price, amount, tid, types), print('Update: ', datetime.now())


schedule.every(0).minutes.do(run_store_data)

while True:
    # Checks whether a scheduled task
    # is pending to run or not
    try:
        schedule.run_pending()
        time.sleep(1)
    except requests.exceptions.ConnectionError:
        print("HEI! Connection refused by the server..")
        print("Lemme sleep for 10 seconds")
        print("ZZzzzz...")
        time.sleep(10)
        print("Was a nice sleep, now let me continue...")
        continue
    except KeyboardInterrupt:
        print("Someone Touch My PC!!")
        break
    except requests.Timeout:
        print("OOPS!! Timeout Error")
        continue
    except requests.RequestException:
        print("OOPS!! General Error")
        continue
