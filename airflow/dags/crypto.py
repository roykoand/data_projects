from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from typing import List, Tuple
from datetime import datetime, timedelta
import json
import os
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

START_TIME = datetime(2022, 1, 12, 14, 30) # CHANGE IT
END_TIME = START_TIME + timedelta(minutes=30)

dag = DAG(
    dag_id="Bitcoin_daily_rate",
    start_date=START_TIME,
    end_date=END_TIME,
    schedule_interval=timedelta(minutes=1),
)

download_rate = BashOperator(
    task_id="download_bitcoine_rate",
    bash_command='mkdir -p /tmp/bitcoin & curl -o /tmp/bitcoin/$(date +"%Y_%m_%d_%H_%M_%S" -u).json -L "https://api.coindesk.com/v1/bpi/currentprice.json"',
    dag=dag,
)


def create_or_update_graph():
    plt.figure(figsize=(16, 6))
    time_value_list: List[Tuple[str, float]] = []
    for root, dirs, files in os.walk(os.path.join("/tmp", "bitcoin")):
        for file in files:
            if file.endswith("json"):
                with open(os.path.join(root, file)) as f:
                    json_file = json.load(f)
                    time_value_list.append(
                        (
                            json_file["time"]["updated"],
                            json_file["bpi"]["USD"]["rate_float"],
                        )
                    )
    df = pd.DataFrame(time_value_list, columns=["Time", "Rate"])
    df["Time"] = df["Time"].apply(
        lambda time: datetime.strptime(time, "%b %d, %Y %H:%M:%S UTC")
    )
    df.sort_values(by="Time", inplace=True)
    plt.title(f"Bitcoin rate from {START_TIME} till {END_TIME}")
    df["Time"] = df["Time"].apply(lambda time: time.strftime("%H:%M"))
    plt.xticks(rotation=45)
    sns.lineplot(y=df["Rate"], x=df["Time"], color="blue", linewidth=3)
    plt.savefig(os.path.join("/tmp", "bitcoin", f"{START_TIME}-{END_TIME}.png"))


graph = PythonOperator(
    task_id="plot_graph",
    python_callable=create_or_update_graph,
    dag=dag,
)

download_rate.set_downstream(graph)
