from networksecurity.exception.exception import NetworkSecurityException
from networksecurity.logging.logger import logging

# cofigurations of the Data Ingestion Config
from networksecurity.entity.config_entity import DataIngestionConfig
from networksecurity.entity.artifact_entity import DataIngestionArtifact

import os
import sys
from sklearn.model_selection import train_test_split
import pandas as pd
import numpy as np
import pymongo
from typing import List

from dotenv import load_dotenv
load_dotenv()

MONGO_DB_URL = os.getenv('MONGO_DB_URL')

class DataIngestion:
    def __init__(self,config:DataIngestionConfig):
        try:
            self.config = config
        except Exception as e:
            logging.error(f"Error in Data Ingestion __init__ : {str(e)}")
            raise NetworkSecurityException(f"Error in Data Ingestion __init__ : {str(e)}")
        
    def export_collection_as_dataframe(self):
        """
        Read data from mongodb
        """
        try:
            database_name=self.config.database_name
            collection_name=self.config.collection_name
            self.mongo_client=pymongo.MongoClient(MONGO_DB_URL)
            collection=self.mongo_client[database_name][collection_name]
            
            df=pd.DataFrame(list(collection.find()))
            if "_id" in df.columns.to_list():
                df=df.drop(columns=["_id"],axis=1)#drop the _id column
                
            df.replace({"na":np.nan},inplace=True)
            return df
        except Exception as e:
            logging.error(f"Error in Data Ingestion export_collection_as_dataframe : {str(e)}")
            raise NetworkSecurityException(f"Error in Data Ingestion export_collection_as_dataframe : {str(e)}")
        
    def export_data_into_feature_store(self,dataframe: pd.DataFrame):
        try:
            feature_store_file_path=self.config.feature_store_file_path
            #creating folder
            dir_path=os.path.dirname(feature_store_file_path)
            os.makedirs(dir_path,exist_ok=True)
            dataframe.to_csv(feature_store_file_path,index=False,header=True)
            return dataframe
        
        except Exception as e:
            logging.error(f"Error in Data Ingestion export_data_into_feature_store : {str(e)}")
            raise NetworkSecurityException(f"Error in Data Ingestion export_data_into_feature_store : {str(e)}")
        
    def split_data_as_train_test(self,dataframe: pd.DataFrame):
        try:
            train_set,test_set=train_test_split(
                dataframe,
                test_size=self.config.train_test_split_ratio,
                random_state=42
            )
            logging.info("Performed train test split on the dataframe")
            
            logging.info("Exited split_data_as_train_test method of Data_Ingestion class")
            
            dir_path=os.path.dirname(self.config.training_file_path)
            os.makedirs(dir_path,exist_ok=True)
            logging.info(f"Exporting train and test file path.")
            
            train_set.to_csv(self.config.training_file_path,index=False,header=True)
            test_set.to_csv(self.config.testing_file_path,index=False,header=True)
            logging.info(f"Exported train and test file path.")
            
            
        except Exception as e:
            logging.error(f"Error in Data Ingestion split_data_as_train_test : {str(e)}")
            raise NetworkSecurityException(f"Error in Data Ingestion split_data_as_train_test : {str(e)}")
        
    def initiate_data_ingestion(self):
        try:
            dataframe = self.export_collection_as_dataframe()
            dataframe=self.export_data_into_feature_store(dataframe) #exporting data into feature store
            self.split_data_as_train_test(dataframe) #splitting data into train and test
            
            dataingestionartifact = DataIngestionArtifact(trained_file_path=self.config.training_file_path,
                                                          test_file_path=self.config.testing_file_path)#creating artifact object
            return dataingestionartifact
            
        except Exception as e:
            logging.error(f"Error in Data Ingestion initiate_data_ingestion : {str(e)}")
            raise NetworkSecurityException(f"Error in Data Ingestion initiate_data_ingestion : {str(e)}")
            