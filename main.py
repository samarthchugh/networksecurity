from networksecurity.components.data_ingestion import DataIngestion
from networksecurity.components.data_validation import DataValidation
from networksecurity.components.data_transformation import DataTransformation
from networksecurity.entity.config_entity import DataIngestionConfig,DataValidationConfig,DataTransformationConfig
from networksecurity.entity.config_entity import TrainingPipelineConfig
from networksecurity.components.model_trainer import ModelTrainer
from networksecurity.entity.config_entity import ModelTrainerConfig
from networksecurity.exception.exception import NetworkSecurityException
from networksecurity.logging.logger import logging
import sys

if __name__=='__main__':
    try:
        training_pipeline_config = TrainingPipelineConfig()
        data_ingestion_config = DataIngestionConfig(training_pipeline_config)
        data_ingestion = DataIngestion(data_ingestion_config)
        logging.info("Initiate the data ingestion process")
        dataingestionartifact=data_ingestion.initiate_data_ingestion()
        logging.info("Data initiation completed")
        print(dataingestionartifact)

        data_validation_config=DataValidationConfig(training_pipeline_config)
        data_validation=DataValidation(dataingestionartifact,data_validation_config)
        logging.info("Initiate the data validation process")
        data_validation_artifact=data_validation.initiate_data_validation()
        logging.info("Data validation completed")
        print(data_validation_artifact)
        
        data_transformation_config = DataTransformationConfig(training_pipeline_config)
        data_transformation = DataTransformation(data_validation_artifact,data_transformation_config)
        logging.info("Data transformation process initiated")
        data_transformation_artifact=data_transformation.initiate_data_transformation()
        logging.info("Data transformation completed")
        print(data_transformation_artifact)
        
        logging.info("Model Trainer started.")
        model_trainer_config=ModelTrainerConfig(training_pipeline_config)
        model_trainer=ModelTrainer(model_trainer_config=model_trainer_config,data_transformation_artifact=data_transformation_artifact)
        model_trainer_artifact=model_trainer.initiate_model_trainer()
        print(model_trainer_artifact)
        logging.info("Model Training artifact created")
        
    except NetworkSecurityException as e:
        logging.error(f"Error in main : {str(e)}")
        raise NetworkSecurityException(e,sys)