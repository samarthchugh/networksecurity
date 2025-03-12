import os
import sys
import numpy as np
import pandas as pd

'''
Defining common constants variables for training pipeline
'''
TARGET_COLUMN :str = 'Result'
PIPELINE_NAME :str = 'NetworkSecurity'
ARTIFACTS_DIR :str = 'Artifacts'
FILE_NAME :str = 'phisingData.csv'

TRAIN_FILE_NAME :str = 'train.csv'
TEST_FILE_NAME :str = 'test.csv'


'''
Data Ingestion related constants start with Data Ingestion var name
'''

DATA_INGESTION_COLLECTION_NAME :str = 'NetworkData'
DATA_INGESTION_DATABASE_NAME :str = 'SamarthProject'
DATA_INGESTION_DIR_NAME :str = 'data_ingestion'
DATA_INGESTION_FEATURE_STORE_DIR :str = 'feature_store'
DATA_INGESTION_INGESTED_DIR :str = 'ingested'
DATA_INGESTION_TRAIN_TEST_SPLIT_RATION :float = 0.2