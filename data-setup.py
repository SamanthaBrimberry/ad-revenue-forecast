# Databricks notebook source
# DBTITLE 1,Install
# MAGIC %pip install faker

# COMMAND ----------

# DBTITLE 1,Imports
from faker import Faker
import pandas as pd
import random
from datetime import datetime, timedelta

# COMMAND ----------

def generate_ad_logs(num_records=1000):
    data = []
    for _ in range(num_records):
        timestamp = fake.date_time_between(start_date='-30d', end_date='now')
        data.append({
            'ad_impression_id': fake.uuid4(),
            'user_id': fake.uuid4(),
            'ad_id': fake.uuid4(),
            'NetValue': round(random.uniform(1.0, 100.0), 2),
            'timestamp': timestamp,
            'duration': random.randint(5, 60),
            'ad_type': random.choice(['pre-roll', 'mid-roll', 'post-roll', 'banner']),
            'completion_rate': round(random.uniform(0, 1), 2)
        })
    return pd.DataFrame(data)

ad_logs_df = generate_ad_logs()


# COMMAND ----------

def generate_identity_data(num_records=200):
    data = []
    for _ in range(num_records):
        data.append({
            'user_id': fake.uuid4(),
            'household_id': fake.uuid4(),
            'age': random.randint(18, 80),
            'gender': random.choice(['Male', 'Female', 'Other']),
            'location': fake.city(),
            'subscription_type': random.choice(['free', 'basic', 'premium']),
            'registration_date': fake.date_between(start_date='-5y', end_date='today')
        })
    return pd.DataFrame(data)

identity_df = generate_identity_data()


# COMMAND ----------

def generate_qoe_data(num_records=500):
    data = []
    for _ in range(num_records):
        start_time = fake.date_time_between(start_date='-30d', end_date='now')
        end_time = start_time + timedelta(minutes=random.randint(10, 120))
        data.append({
            'session_id': fake.uuid4(),
            'user_id': fake.uuid4(),
            'start_time': start_time,
            'end_time': end_time,
            'avg_bitrate': random.randint(1000, 10000),
            'buffering_count': random.randint(0, 10),
            'total_watch_time': (end_time - start_time).total_seconds(),
            'device_type': random.choice(['mobile', 'desktop', 'tablet', 'smart_tv']),
            'connection_type': random.choice(['wifi', '4G', '5G', 'broadband'])
        })
    return pd.DataFrame(data)

qoe_df = generate_qoe_data()

# COMMAND ----------

def ensure_joinability(ad_logs_df, qoe_df, identity_df):
    # Get a subset of user_ids from the identity dataset
    shared_user_ids = identity_df['user_id'].sample(n=min(len(ad_logs_df), len(qoe_df), len(identity_df))).tolist()
    
    # Update ad_logs and qoe datasets with these shared user_ids
    ad_logs_df.loc[:len(shared_user_ids)-1, 'user_id'] = shared_user_ids
    qoe_df.loc[:len(shared_user_ids)-1, 'user_id'] = shared_user_ids
    
    return ad_logs_df, qoe_df, identity_df

ad_logs_df, qoe_df, identity_df = ensure_joinability(ad_logs_df, qoe_df, identity_df)


# COMMAND ----------

ad_log = spark.createDataFrame(ad_logs_df)
qoe_df = spark.createDataFrame(qoe_df)
ir_df = spark.createDataFrame(identity_df)

# COMMAND ----------

ad_log.write.mode('overwrite').saveAsTable(f'{catalog}.{schema}.ad_log')
qoe_df.write.mode('overwrite').saveAsTable(f'{catalog}.{schema}.qoe_df')
ir_df.write.mode('overwrite').saveAsTable(f'{catalog}.{schema}.ir_df')