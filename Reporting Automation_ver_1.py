import telegram
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import io
import pandas as pd
import requests
import pandahouse

from datetime import date, timedelta, datetime
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

sns.set()

default_args = {
    'owner': 'a.burlakov-9',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 2, 12)
    }

schedule_interval = '0 11 * * *'

def my_report(chat=None):
    chat_id = chat or -6771XXXXXXXXXXX
    
    my_token = '6023168328:AAE1WuD5RUDLNDyOgGdJkPBmKXXXXXXXXXXXXXX'
    my_bot = telegram.Bot(token=my_token)
    
    connection = {'host': 'https://clickhouse.lab.karpov.courses',
                  'database':'simulator_20230120',
                  'user':'student',
                  'password':'XXXXXXXXX'}

    query_dau = """
                SELECT
                    COUNT(DISTINCT user_id)
                FROM
                    simulator_20230120.feed_actions
                WHERE
                    toDate(time) = yesterday()
                """

    query_views = """
                SELECT
                    COUNTIf(action = 'view')
                FROM 
                    simulator_20230120.feed_actions
                WHERE
                    toDate(time) = yesterday()
                  """

    query_likes = """
                SELECT
                    COUNTIf(action = 'like')
                FROM
                    simulator_20230120.feed_actions
                WHERE
                    toDate(time) = yesterday()
                  """

    query_ctr = """
                SELECT
                    COUNTIf(action = 'like') / countIf(action = 'view')
                FROM
                    simulator_20230120.feed_actions
                WHERE
                    toDate(time) = yesterday()
                """

    query_metrics = """
                SELECT
                    toDate(time) AS day,
                    COUNT(DISTINCT user_id) AS DAU,
                    COUNTIf(user_id, action = 'view') AS views,
                    COUNTIf(user_id, action = 'like') AS likes,
                    COUNTIf(user_id, action = 'like') / COUNTIf(user_id, action = 'view') AS CTR
                FROM
                    simulator_20230120.feed_actions
                WHERE
                    toDate(time) BETWEEN today()-7 AND today()-1
                GROUP BY
                    day
                    """
    dau = pandahouse.read_clickhouse(query_dau, connection=connection)
    views = pandahouse.read_clickhouse(query_views, connection=connection)
    likes = pandahouse.read_clickhouse(query_likes, connection=connection)
    ctr = pandahouse.read_clickhouse(query_ctr, connection=connection)

    msg = "Metrics for the previous day " + str(date.today() - timedelta(days=1)) + '\n' \
              + 'DAU: ' + str(dau.iloc[0, 0]) + '\n' \
              + 'Views: ' + str(views.iloc[0, 0]) + '\n' \
              + 'Likes: ' + str(likes.iloc[0, 0]) + '\n' \
              + 'CTR: ' + str(round(ctr.iloc[0, 0], 3))

    my_bot.sendMessage(chat_id=chat_id, text=msg)
    
    metrics = pandahouse.read_clickhouse(query_metrics, connection=connection)

    #########################################        
    fig, axes = plt.subplots(2,2, figsize=(10, 10))
    fig.suptitle("Metrics for the previous 7 days")
    fig.subplots_adjust(hspace=0.7)
    fig.subplots_adjust(wspace=0.4)

    sns.lineplot(data=metrics, x='day', y='DAU', ax=axes[0, 0])
    axes[0, 0].set_title("DAU")
    axes[0, 0].tick_params(axis="x", rotation=45)

    sns.lineplot(data=metrics, x='day', y='views', ax=axes[1, 1])
    axes[1, 1].set_title("Views")
    axes[1, 1].tick_params(axis="x", rotation=45)

    sns.lineplot(data=metrics, x='day', y='likes', ax=axes[0, 1])
    axes[0, 1].set_title("Likes")
    axes[0, 1].tick_params(axis="x", rotation=45)

    sns.lineplot(data=metrics, x='day', y='CTR', ax=axes[1, 0])
    axes[1, 0].set_title("CTR")
    axes[1, 0].tick_params(axis="x", rotation=45)

    plot_object = io.BytesIO()
    plt.savefig(plot_object)

    plot_object.seek(0)
    plot_object.name = ('metrics.png')
    plt.close()

    my_bot.sendPhoto(chat_id=chat_id, photo=plot_object)
    ##########################################################
@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def burlakov_dag_report_ver4():
    
    @task()
    def make_report():
        my_report()
    
    make_report()
    
burlakov_dag_report_ver4 = burlakov_dag_report_ver4()  