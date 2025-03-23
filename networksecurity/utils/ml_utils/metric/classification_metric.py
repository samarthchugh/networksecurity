from networksecurity.entity.artifact_entity import ClassificationMetricsArtifact
from networksecurity.exception.exception import NetworkSecurityException
from sklearn.metrics import f1_score,recall_score,precision_score

import sys

def get_classification_score(y_true,y_pred)->ClassificationMetricsArtifact:
    try:
        model_f1_score = f1_score(y_true=y_true,y_pred=y_pred)
        model_recall_score = recall_score(y_true=y_true,y_pred=y_pred)
        model_precision_score = precision_score(y_true=y_true,y_pred=y_pred)
        
        classification_metrics = ClassificationMetricsArtifact(f1_score=model_f1_score,
                                precission_score=model_precision_score,
                                recall_score=model_recall_score)
        return classification_metrics
    
    except Exception as e:
        raise NetworkSecurityException(e,sys)