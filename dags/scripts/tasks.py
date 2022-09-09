import findspark
findspark.init()


import datetime
import os
import random
from datetime import date, timedelta
import datetime

import numpy as np
import pandas as pd
from airflow.models import Variable
import pyspark.sql.functions as F
from pyspark.ml.feature import StandardScaler
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.window import Window


def save_table(table, filename, output_prefix):
    table.to_parquet(filename, index=False)
    os.system(f"hdfs dfs -rm -r -skipTrash {output_prefix}/{filename}")
    os.system(f"hdfs dfs -put {filename} {output_prefix}/{filename}")


def read_table(output_prefix, filename):
    os.system(f"rm {filename}")
    os.system(f"hdfs dfs -copyToLocal {output_prefix}/{filename} ./")
    return pd.read_parquet(filename)


def check_max_date():
    yesterday = str(date.today() - timedelta(days=1))
    current_value = Variable.get("max_available_date", default_var=yesterday)
    Variable.set("max_available_date", current_value)


def generate_terminals(config):
    terminal_profiles_table = generate_terminal_profiles_table(config.n_terminals, random_state=1)
    save_table(terminal_profiles_table, "terminals_table.parquet", config.output_prefix)


def generate_customers(config):
    customers_profiles_table = generate_customer_profiles_table(config.n_customers, random_state=1)
    save_table(customers_profiles_table, "customers_table.parquet", config.output_prefix)


def gen_transactions(config):
    customer_profiles_table = read_table(config.output_prefix, "customers_table.parquet")
    terminal_profiles_table = read_table(config.output_prefix, "terminals_table.parquet")

    x_y_terminals = terminal_profiles_table[['x_terminal_id', 'y_terminal_id']].values.astype(float)
    customer_profiles_table['available_terminals'] = customer_profiles_table.apply(
        lambda x: get_list_terminals_within_radius(x, x_y_terminals=x_y_terminals, r=config.radius), axis=1)

    customer_profiles_table['nb_terminals'] = customer_profiles_table.available_terminals.apply(len)

    last_date = Variable.get("max_available_date")
    last_date = datetime.datetime.strptime(last_date, "%Y-%m-%d").date()
    nb_days = (date.today() - last_date).days
    start_date = str(last_date + timedelta(1))

    transactions_df = customer_profiles_table.groupby('CUSTOMER_ID').apply(
        lambda x: generate_transactions_table(x.iloc[0], start_date=start_date, nb_days=nb_days)).reset_index(drop=True)

    transactions_df = transactions_df.sort_values('TX_DATETIME')
    transactions_df.reset_index(inplace=True, drop=True)
    transactions_df.reset_index(inplace=True)
    transactions_df.rename(columns={'index': 'TRANSACTION_ID'}, inplace=True)

    for filename, table in [
        ("terminals_table_final.parquet", terminal_profiles_table),
        ("customers_table_final.parquet", customer_profiles_table),
        ("transactions_table.parquet", transactions_df)
    ]:
        save_table(table, filename, config.output_prefix)


def add_fraud(config):
    customer_profiles_table = read_table(config.output_prefix, "customers_table_final.parquet")
    terminal_profiles_table = read_table(config.output_prefix, "terminals_table_final.parquet")
    transactions_df = read_table(config.output_prefix, "transactions_table.parquet")

    transactions_df = add_frauds(customer_profiles_table, terminal_profiles_table, transactions_df)
    max_date = pd.to_datetime(transactions_df["TX_DATETIME"]).dt.date.astype(str).max()
    Variable.set("max_available_date", max_date)

    transactions_df["date"] = pd.to_datetime(transactions_df["TX_DATETIME"]).dt.date.astype(str)
    save_table(transactions_df, "transactions_table_final.parquet", config.output_prefix)


def generate_customer_profiles_table(n_customers, random_state=0):
    np.random.seed(random_state)

    customer_id_properties = []

    # Generate customer properties from random distributions
    for customer_id in range(n_customers):
        x_customer_id = np.random.uniform(0, 100)
        y_customer_id = np.random.uniform(0, 100)

        mean_amount = np.random.uniform(5, 100)  # Arbitrary (but sensible) value
        std_amount = mean_amount / 2  # Arbitrary (but sensible) value

        mean_nb_tx_per_day = np.random.uniform(0, 4)  # Arbitrary (but sensible) value

        customer_id_properties.append([customer_id,
                                       x_customer_id, y_customer_id,
                                       mean_amount, std_amount,
                                       mean_nb_tx_per_day])

    customer_profiles_table = pd.DataFrame(customer_id_properties, columns=['CUSTOMER_ID',
                                                                            'x_customer_id', 'y_customer_id',
                                                                            'mean_amount', 'std_amount',
                                                                            'mean_nb_tx_per_day'])

    return customer_profiles_table


def generate_terminal_profiles_table(n_terminals, random_state=0):
    np.random.seed(random_state)

    terminal_id_properties = []

    # Generate terminal properties from random distributions
    for terminal_id in range(n_terminals):
        x_terminal_id = np.random.uniform(0, 100)
        y_terminal_id = np.random.uniform(0, 100)

        terminal_id_properties.append([terminal_id,
                                       x_terminal_id, y_terminal_id])

    terminal_profiles_table = pd.DataFrame(terminal_id_properties, columns=['TERMINAL_ID',
                                                                            'x_terminal_id', 'y_terminal_id'])

    return terminal_profiles_table


def get_list_terminals_within_radius(customer_profile, x_y_terminals, r):
    # Use numpy arrays in the following to speed up computations

    # Location (x,y) of customer as numpy array
    x_y_customer = customer_profile[['x_customer_id', 'y_customer_id']].values.astype(float)

    # Squared difference in coordinates between customer and terminal locations
    squared_diff_x_y = np.square(x_y_customer - x_y_terminals)

    # Sum along rows and compute suared root to get distance
    dist_x_y = np.sqrt(np.sum(squared_diff_x_y, axis=1))

    # Get the indices of terminals which are at a distance less than r
    available_terminals = list(np.where(dist_x_y < r)[0])

    # Return the list of terminal IDs
    return available_terminals


def generate_transactions_table(customer_profile, start_date, nb_days):
    customer_transactions = []

    random.seed(int(customer_profile.CUSTOMER_ID))
    np.random.seed(int(customer_profile.CUSTOMER_ID))

    # For all days
    for day in range(nb_days):

        # Random number of transactions for that day
        nb_tx = np.random.poisson(customer_profile.mean_nb_tx_per_day)

        # If nb_tx positive, let us generate transactions
        if nb_tx > 0:

            for tx in range(nb_tx):

                # Time of transaction: Around noon, std 20000 seconds. This choice aims at simulating the fact that
                # most transactions occur during the day.
                time_tx = int(np.random.normal(86400 / 2, 20000))

                # If transaction time between 0 and 86400, let us keep it, otherwise, let us discard it
                if (time_tx > 0) and (time_tx < 86400):

                    # Amount is drawn from a normal distribution
                    amount = np.random.normal(customer_profile.mean_amount, customer_profile.std_amount)

                    # If amount negative, draw from a uniform distribution
                    if amount < 0:
                        amount = np.random.uniform(0, customer_profile.mean_amount * 2)

                    amount = np.round(amount, decimals=2)

                    if len(customer_profile.available_terminals) > 0:
                        terminal_id = random.choice(customer_profile.available_terminals)

                        customer_transactions.append([time_tx + day * 86400, day,
                                                      customer_profile.CUSTOMER_ID,
                                                      terminal_id, amount])

    customer_transactions = pd.DataFrame(customer_transactions,
                                         columns=['TX_TIME_SECONDS', 'TX_TIME_DAYS', 'CUSTOMER_ID', 'TERMINAL_ID',
                                                  'TX_AMOUNT'])

    if len(customer_transactions) > 0:
        customer_transactions['TX_DATETIME'] = pd.to_datetime(customer_transactions["TX_TIME_SECONDS"], unit='s',
                                                              origin=start_date)
        customer_transactions = customer_transactions[
            ['TX_DATETIME', 'CUSTOMER_ID', 'TERMINAL_ID', 'TX_AMOUNT', 'TX_TIME_SECONDS', 'TX_TIME_DAYS']]

    return customer_transactions


def add_frauds(customer_profiles_table, terminal_profiles_table, transactions_df):
    # By default, all transactions are genuine
    transactions_df['TX_FRAUD'] = 0
    transactions_df['TX_FRAUD_SCENARIO'] = 0

    # Scenario 1
    transactions_df.loc[transactions_df.TX_AMOUNT > 220, 'TX_FRAUD'] = 1
    transactions_df.loc[transactions_df.TX_AMOUNT > 220, 'TX_FRAUD_SCENARIO'] = 1
    nb_frauds_scenario_1 = transactions_df.TX_FRAUD.sum()
    print("Number of frauds from scenario 1: " + str(nb_frauds_scenario_1))

    # Scenario 2
    for day in range(transactions_df.TX_TIME_DAYS.max()):
        compromised_terminals = terminal_profiles_table.TERMINAL_ID.sample(n=2, random_state=day)

        compromised_transactions = transactions_df[(transactions_df.TX_TIME_DAYS >= day) &
                                                   (transactions_df.TX_TIME_DAYS < day + 28) &
                                                   (transactions_df.TERMINAL_ID.isin(compromised_terminals))]

        transactions_df.loc[compromised_transactions.index, 'TX_FRAUD'] = 1
        transactions_df.loc[compromised_transactions.index, 'TX_FRAUD_SCENARIO'] = 2

    nb_frauds_scenario_2 = transactions_df.TX_FRAUD.sum() - nb_frauds_scenario_1
    print("Number of frauds from scenario 2: " + str(nb_frauds_scenario_2))

    # Scenario 3
    for day in range(transactions_df.TX_TIME_DAYS.max()):
        compromised_customers = customer_profiles_table.CUSTOMER_ID.sample(n=3, random_state=day).values

        compromised_transactions = transactions_df[(transactions_df.TX_TIME_DAYS >= day) &
                                                   (transactions_df.TX_TIME_DAYS < day + 14) &
                                                   (transactions_df.CUSTOMER_ID.isin(compromised_customers))]

        nb_compromised_transactions = len(compromised_transactions)

        random.seed(day)
        index_fauds = random.sample(list(compromised_transactions.index.values), k=int(nb_compromised_transactions / 3))

        transactions_df.loc[index_fauds, 'TX_AMOUNT'] = transactions_df.loc[index_fauds, 'TX_AMOUNT'] * 5
        transactions_df.loc[index_fauds, 'TX_FRAUD'] = 1
        transactions_df.loc[index_fauds, 'TX_FRAUD_SCENARIO'] = 3

    nb_frauds_scenario_3 = transactions_df.TX_FRAUD.sum() - nb_frauds_scenario_2 - nb_frauds_scenario_1
    print("Number of frauds from scenario 3: " + str(nb_frauds_scenario_3))

    return transactions_df

def get_spark():
    import findspark
    findspark.init()

    import pyspark
    from pyspark import SparkConf

    conf = SparkConf()
    conf.setAppName('practice_4')
    conf.set("spark.dynamicAllocation.enabled", "true")
    conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    conf.set("spark.driver.maxResultSize", "4G")
    conf.set("spark.driver.memory", "4G")
    conf.set("spark.executor.memory", "4G")
    conf.set("spark.driver.allowMultipleContexts", "true")

    spark = pyspark.sql.SparkSession.builder.config(conf=conf).getOrCreate()
    return spark


def get_max_data_date(spark, config):
    try:
        df = (
            spark
                .read
                .parquet(
                f"{config.output_prefix}features.parquet"
            )
                .select(
                F.max("date")
            )
                .toPandas()
        )
        return df.iloc[:, 0].astype(str).max()
    except:
        return None


def prepare_features(config):
    spark = get_spark()
    max_date = get_max_data_date(spark, config)

    customer_window = Window.partitionBy("CUSTOMER_ID")
    terminal_window = Window.partitionBy("TERMINAL_ID")

    table = spark.read.parquet("./all_transactions_table.parquet")
    if max_date is not None:
        table = table.filter(F.col("date") > max_date)

    prepared_table = (
        table
            .withColumn("f_mean_customer_TX_AMOUNT", F.mean(F.col("TX_AMOUNT")).over(customer_window))
            .withColumn("f_mean_terminal_TX_AMOUNT", F.mean(F.col("TX_AMOUNT")).over(terminal_window))
            .withColumn("f_unq_terminals_per_customer",
                        F.approx_count_distinct(F.col("TERMINAL_ID")).over(terminal_window))
            .withColumn(
            "f_customer_amount_ratio",
            F.col("TX_AMOUNT") / F.col("f_mean_customer_TX_AMOUNT")
        )
            .withColumn(
            "f_terminal_amount_ratio",
            F.col("TX_AMOUNT") / F.col("f_mean_terminal_TX_AMOUNT")
        )
            .withColumnRenamed("TX_AMOUNT", "f_TX_AMOUNT")
            .withColumnRenamed("TX_TIME_SECONDS", "f_TX_TIME_SECONDS")
    )

    features = list(filter(lambda x: x.startswith("f_"), prepared_table.columns))
    vec_assembler = VectorAssembler(inputCols=features, outputCol="features")
    prepared_table = vec_assembler.transform(prepared_table)

    scaler = StandardScaler(
        inputCol="features",
        outputCol="scaledFeatures",
        withStd=True,
        withMean=True
    )

    scalerModel = scaler.fit(prepared_table)
    prepared_table = scalerModel.transform(prepared_table)
    prepared_dataset = prepared_table.select("scaledFeatures", "TX_FRAUD", "date")

    prepared_dataset.write.mode("OVERWRITE").partitionBy("date").parquet(f"{config.output_prefix}features.parquet")
