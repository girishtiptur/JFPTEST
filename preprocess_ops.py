"""
 * © Copyright 2025 Hewlett Packard Enterprise Development LP
 *
 * This work is proprietary to Hewlett Packard Enterprise Development LP
 * ("HPED LP"). It is specifically "closed source" code, confidential, and
 * held in copyright as an unpublished work by HPED LP. All rights reserved.
"""

import time 
import logging
import math
import re
import numpy as np
import pandas as pd

from typing import Optional
from datetime import datetime
from cloudpickle import cloudpickle
from sklearn.preprocessing import LabelEncoder
from sklearn.preprocessing import OrdinalEncoder

from jfp.failure_predictor.event import Event
from jfp.failure_predictor.operators.operator import Operator

logger = logging.getLogger(__name__)


class PreprocessOps(Operator):

    def __init__(self, encoder_path: Optional[str] = None):
        super().__init__()

        self.encoder_path = encoder_path        
        self.context = None        
        self.ordinal_encoders = {}         
        self.load_ordinal_encoder()

    def __json__(self) -> dict:
        return {
            "__type__": "operators.preprocess_ops.PreprocessOps",
            "encoder_path": self.encoder_path
        }
        
    def load_ordinal_encoder(self):
        if self.encoder_path:
            try:
                with open(self.encoder_path, "rb") as f:
                    self.ordinal_encoders = cloudpickle.load(f)
            except FileNotFoundError:
                logger.error(f"Encoder file not found at {self.encoder_path}")
                self.ordinal_encoders = {}
            except Exception as e:
                logger.error(f"Error loading encoder: {str(e)}")
                self.ordinal_encoders = {}

              
 
    @staticmethod
    def _is_valid(event):
        """
        Validates and replaces invalid attributes in an event.
    
        - Numeric: Replaces `None`, `NaN`, or `Inf` with `np.nan`.
        - String:  `"none"`, `"nan"`, `"null"`, or empty values with `np.nan`. Skip the messages 
    
        Args:
            event: An object with an `attributes` attribute, which is expected to be a dictionary 
                   containing feature values (can include both numeric and non-numeric types).
    
        Returns:
            bool: Always returns `True` after replacing invalid values.
        """
        for attr, value in event.attributes.items():
            
            # Handle invalid numeric values (replace with np.nan)
            if isinstance(value, (int, float)) and (value is None or math.isnan(value) or math.isinf(value)):
                logger.info(f"Invalid numeric value for {attr}: {value}, skipping event: {event}")
                return False 
     
            # Handle invalid string values (skip the message)
            if isinstance(value, str) and value.strip().lower() in {"", "none", "nan", "null"}:
                logger.info(f"Invalid string value for {attr}: {value}, skipping event: {event}")
                return False  

        return True  # Always return True
        
             

    def _convert_categorical_features(self, event):
      """
      Optimized encoding of categorical features in an event using dictionary lookup 
      instead of OrdinalEncoder.transform().
      
      Args:
          event (dict): The event containing categorical features.
      
      Returns:
          dict: The updated event with encoded features.
      """
#      start_time = datetime.now()
#      print(f"START: {start_time} | FUNCTION: _convert_categorical_features") 
#      
      categorical_features_base = self.context.metadata.get('categorical_features', [])
    
      for feature in categorical_features_base:
          encoded_feature = f"{feature}_encoded"
    
          if feature in event:
              if feature in self.ordinal_encoders:
                  encoder = self.ordinal_encoders[feature]  # Fetch the encoder
    
                  # **Step 1:** Create a lookup dictionary from the encoder
                  if not hasattr(encoder, "categories_"):  # Ensure it's a valid OrdinalEncoder
                      logger.error(f"Encoder for '{feature}' is not properly trained.")
                      event[encoded_feature] = -1
                      continue
    
                  # **Step 2:** Build a category-to-index lookup dictionary
                  category_lookup = {cat: i for i, cat in enumerate(encoder.categories_[0])}
    
                  # **Step 3:** Lookup the encoded value, fallback to -1 if not found
                  event[encoded_feature] = category_lookup.get(event[feature], -1)
              else:
                  logger.warning(f"Encoder not found for feature '{feature}'. Assigning -1.")
                  event[encoded_feature] = -1  # Default -1 for missing encoder
          else:
              logger.warning(f"Feature '{feature}' not present in event. Assigning -1.")
              event[encoded_feature] = -1  # Default -1 for missing feature
    
#      end_time = datetime.now() 
#      print(f"END: {end_time} | FUNCTION: _convert_categorical_features | TOTAL TIME: {end_time - start_time}")        
#      
      return event
    
        

         
    def _convert_time_columns(self, event):
          """
          Convert specified time-related columns in an event to datetime format.
      
          Args:
              event (dict): Input event containing raw feature data, including time columns.
      
          Returns:
              dict: Updated event with time columns converted to datetime format.
          """
 
          # Retrieve time-related columns from metadata          
          time_columns = self.context.metadata.get('time_features', [])     
        
          for column in time_columns:
              if column not in event or event[column] is None:
                  logger.warning(f"Missing or None value for column '{column}' in event.")
                  continue
      
              try:
                  value = event[column]
      
                  # Check for numeric values (epoch times)
                  if isinstance(value, (int, float)):
                      if value < 1e10:  # Seconds
                          event[column] = pd.to_datetime(value, unit='s').tz_localize(None)
                      elif value < 1e13:  # Milliseconds
                          event[column] = pd.to_datetime(value, unit='ms').tz_localize(None)
                      else:  # Nanoseconds
                          event[column] = pd.to_datetime(value, unit='ns').tz_localize(None)
                  else:
                      # Convert string-formatted dates
                      event[column] = pd.to_datetime(value).tz_localize(None)
              except Exception as e:
                  logger.error(f"Error converting column '{column}' with value '{event[column]}': {e}")           
          return event


    @staticmethod
    def _interpolate_calculation(calculation, base_features, event):
        placeholders = re.findall(r'\{\{ (.*?) \}\}', calculation)
        for placeholder in placeholders:
            if placeholder in base_features:
                calculation = calculation.replace(f'{{{{ {placeholder} }}}}', f"event['{placeholder}']")
            else:
                logger.info(f"Placeholder '{placeholder}' not found in original features.")
        return calculation
        
        
    def _add_new_features(self, event):
        """
        Add new calculated features to the event based on metadata definitions.
    
        Args:
            event (dict): Input event containing raw feature data.
    
        Returns:
            dict: Updated event with additional features calculated and added.
    
        Steps:
            1. Retrieve additional features and base features metadata from context.
            2. For each additional feature:
               - Interpolate the calculation expression using base features and event values.
               - Evaluate the interpolated calculation and add the result as a new feature in the event.
            3. Log and raise an exception if an error occurs during feature calculation.
    
        Raises:
            Exception: If there is an error evaluating the calculation for any additional feature.
        """
        # Retrieve additional features and base features from metadata
        
        new_features = self.context.metadata.get('additional_features_test')
        base_features = self.context.metadata.get('base_features')

        # If there are new features to calculate
        if new_features:
            for feature in new_features:
                feature_name = feature['name']
                calculation = feature['calculation']
                interpolated_calculation = PreprocessOps._interpolate_calculation(calculation, base_features, event)
                try:
                    event[feature_name] = eval(interpolated_calculation)
                except Exception as e:
                    logger.error(
                        f"Error occurred while calculating additional feature '{feature_name}' "
                        f"with calculation '{calculation}': {str(e)}")
                    raise          
        return event
        
        
    def _filter_event_columns(self,event, features_event):
        """
        Removes columns from the event that are not present in the features list.
    
        Args:
            event (dict): The event dictionary containing various attributes.
            features (list): The list of features to retain in the event.
    
        Returns:
            dict: A filtered event dictionary with only the specified features.
            
        """
         # Extract the required positional arguments if needed
        timestamp = event.get("timestamp")  # Or set a default value if timestamp is mandatory        
        event_dict = {key: value for key, value in event.items() if key in features_event}    
    
        # Pass the filtered dictionary along with the required positional arguments
        return Event(timestamp=timestamp, **event_dict)

  
           
    
    def preprocess(self, event):
        """
        Preprocess the input event for further processing.
    
        Steps:
        1. Retrieve required features for preprocessing from metadata.
        2. Filter out unnecessary columns from the event.
        3. Validate the event for correctness.
        4. Apply preprocessing steps in sequence: 
            - Convert time columns.
            - Convert categorical features.
            - Add new calculated features.
    
        Args:
            event (dict): Input event data.
    
        Returns:
            dict: Preprocessed event data or None if invalid.
        """
 
        # Retrieve the required features for preprocessing
        features_event = self.context.metadata.get('test_features')
        
        # Filter the event to include only required columns
        event = self._filter_event_columns(event, features_event)       
        
        # Check if the event is valid for processing
        if not self._is_valid(event):
            # Log or handle invalid event scenario with specific fields
            logger.info(f"Invalid event. Skipping processing: {event}")                        
            return None
        
                # Define the preprocessing pipeline as a sequence of transformation steps
        pipeline = [
            self._convert_time_columns,        # Convert time-related fields
            self._convert_categorical_features, # Handle categorical data
            self._add_new_features             # Add calculated features
        ]         
    
        # Sequentially apply the preprocessing steps
        for step in pipeline:
            event = step(event)      
            
           
        return event
                
    
    def process(self, event):
        """
        Process an event by preprocessing it and sending it to the output stream.
    
        Args:
            event (dict): Input event data.
    
        Returns:
            None
        """
  
        # Preprocess the input event
        event = self.preprocess(event=event)                     
        # Skip further processing if the event is invalid or empty
        if not event:
            return        
        # Push the preprocessed event to the output stream
        self.output_stream.push(event)
         

