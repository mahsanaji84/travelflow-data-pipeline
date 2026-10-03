from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime
import pandas as pd
from cassandra.cluster import Cluster


CSV_PATH = "/opt/airflow/data/reservations.csv"


def process_batch():

    print("Lecture du fichier CSV...")

    df = pd.read_csv(CSV_PATH)

    print(f"Nombre total de réservations : {len(df)}")

    stats = []

    for destination, group in df.groupby("destination"):

        total_reservations = len(group)

        completed_reservations = (
            group["status"] == "completed"
        ).sum()

        abandoned_reservations = (
            group["status"] == "abandoned"
        ).sum()

        total_revenue = group.loc[
            group["status"] == "completed",
            "amount"
        ].sum()

        stats.append({
            "destination": destination,
            "total_reservations": int(total_reservations),
            "completed_reservations": int(completed_reservations),
            "abandoned_reservations": int(abandoned_reservations),
            "total_revenue": float(total_revenue),
        })

    print("Statistiques calculées :")

    for row in stats:
        print(row)

    cluster = Cluster(["cassandra"])
    session = cluster.connect("travelflow")

    insert_query = """
        INSERT INTO batch_booking_stats (
            destination,
            total_reservations,
            completed_reservations,
            abandoned_reservations,
            total_revenue
        )
        VALUES (%s, %s, %s, %s, %s)
    """

    for row in stats:
        session.execute(
            insert_query,
            (
                row["destination"],
                row["total_reservations"],
                row["completed_reservations"],
                row["abandoned_reservations"],
                row["total_revenue"],
            ),
        )

    print("Écriture dans Cassandra terminée avec succès.")

    cluster.shutdown()


with DAG(
    dag_id="travelflow_batch_analysis",
    description="Analyse batch des réservations TravelFlow",
    start_date=datetime(2026, 10, 1),
    schedule="@daily",
    catchup=False,
    tags=["travelflow", "batch"],
) as dag:

    analyse_reservations = PythonOperator(
        task_id="analyse_reservations",
        python_callable=process_batch,
    )

    analyse_reservations