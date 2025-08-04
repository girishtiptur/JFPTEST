"""
 * © Copyright 2025 Hewlett Packard Enterprise Development LP
 *
 * This work is proprietary to Hewlett Packard Enterprise Development LP
 * ("HPED LP"). It is specifically "closed source" code, confidential, and
 * held in copyright as an unpublished work by HPED LP. All rights reserved.
"""
import logging
import os
import typing
import re
import xgboost as xgb
import numpy as np
import os
#import datetime
from datetime import datetime
from jfp.failure_predictor.operators.operator import Operator

logger = logging.getLogger(__name__)


class XGBoostPredictorOps(Operator):

    def __init__(self, parameters: typing.Dict):
        super().__init__()

        self.model_path = None
        self.model_type = None
        self.model = None
        self.context = None
        self.set_parameters(**parameters)

        # statistics data points
        self.__stride = 1
        self.num_received = 0  # An integer which stores number of received events
        self.num_sent = 0  # An integer which stores number of sent events
        self.num_failures = 0  # An integer which stores number of failures events
        self.total_received = 0  # An integer which stores number of total received events
        self.total_sent = 0  # An integer which stores total number of sent events
        self.total_failures = 0  # An integer which stores total number of failures events

    @property
    def stride(self):
        return self.__stride

    @stride.setter
    def stride(self, value: int):
        self.__stride = value

        # load the latest model

    def load_model(self):

        """
        Load the latest model from the specified directory based on the
        naming convention. xgboost XGBClassifier
        """

        try:
            # Log the path of the model being loaded
            logger.info(f"Loading model from: {self.model_path}")
            # load using xgboost XGBClassifier
            self.model = xgb.XGBClassifier()
            self.model.load_model(self.model_path)
            # Log the success of the model loading
            logger.info(f"Model ubj loaded successfully of type: {self.model_type} and path : {self.model_path}")           
        except Exception as e:
            # Log any errors that occur during model loading
            logger.error(f"Error loading model: {e}")
            raise

    def __json__(self) -> dict:
        return {
            "__type__": "operators.xgboost_pred_ops.XGBoostPredictorOps",
            "parameters": {
                "model_path": self.model_path,
                "model_type": self.model_type
            }
        }

 
        
    def set_parameters(self, model_path: str, model_type: str):
        """
        Set the model's parameters.
    
        Args:
            model_path (str): The path to the model file.
            model_type (str): The type of the model (e.g., 'xgboost', 'random_forest').
        """
        self.model_path = model_path
        self.model_type = model_type
            



    def process(self, event):
        """
        Process an incoming event, run the model, and send the output.
    
        Args:
            event (dict): The input event containing feature data.
    
        Workflow:
            1. Ensure the model is loaded.
            2. Extract the required features from the event.
            3. Predict probabilities for the event using the model.
            4. Classify the event based on the predicted probabilities and a threshold.
            5. Attach additional metadata and send the processed event to the output stream.
        """
                   
        # Ensure the model is loaded
        if not self.model:
            self.load_model()
    
        # Increment counters for received events
        self.num_received += 1
        self.total_received += 1
    
        # Retrieve feature list & threshold from metadata
        features = self.context.metadata.get('train_features', [])
        threshold = self.context.metadata.get('threshold', 0.5)  # Default threshold to 0.5 if not provided

        values = []
        for feature in features:
            if feature in event:
                value = event[feature]
                if value is None or (isinstance(value, str) and value.lower() == "nan"):
                    value = np.nan  # Replace None or 'nan' (string) with NaN
                try:
                    values.append(float(value))  # Convert to float
                except ValueError:
                    values.append(np.nan)  # Handle unexpected non-numeric values safely
    
        values = np.array([values], dtype=np.float32)  # Shape (1, num_features)
    
        # Predict probabilities using the model
        probabilities = self.model.predict_proba(values)
        nominal_probability = float(probabilities[0][0])  # Probability of "nominal" (0)
        failure_probability = float(probabilities[0][1])  # Probability of "failure" (1)
    
        # Classify based on the failure probability and threshold
        if failure_probability >= threshold:
            event['label'] = 1  # Failure
            event['confidence'] = failure_probability
            self.num_failures += self.__stride
            self.total_failures += self.__stride
        else:
            event['label'] = 0  # Nominal
            event['confidence'] = nominal_probability
    
        # Add model metadata
        event["model"] = self.model_type
    
        # Send event to the output stream
        self.output_stream.push(event)        
       
        # Increment sent event counters
        self.num_sent += 1
        self.total_sent += 1


