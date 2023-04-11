import os
from model_logging.logger import App_Logger
import pandas as pd

class dataTransform:
    """
    Performing transformation on all good RawData.
    """
    
    def __init__(self):
        self.goodDataPath = "Training_Raw_files_validated/Good_Raw"
        self.logger = App_Logger() 
    
    def replaceMissingWithNull(self):
        """
        Method Name: replaceMissingWithNull
        Description: This method replaces the missing values in columns with "NULL" to
                    store in the table. We are using substring in the first column to
                    keep only "Integer" data for ease up the loading.
                    This column is anyways going to be removed during training.
        """
        log = open("Training_Logs/dataTransformLog.txt", 'a+')
        try:
            for file in os.listdir(self.goodDataPath):
                data = pd.read_csv(os.path.join(self.goodDataPath, file))

                # Replace missing values with "NULL"
                #data.fillna("NULL", inplace=True)

                # Add quotes around string columns
                str_cols = ['Income', 'workclass', 'education', 'marital-status', 'occupation', 'relationship',
                            'race', 'sex', 'native-country']
                for col in str_cols:
                    data[col] = "'" + data[col].astype(str) + "'"

                data.to_csv(os.path.join(self.goodDataPath, file), index=False)

                self.logger.log(log, f"{file}: Quotes added successfully!!")

        except Exception as e:
            self.logger.log(log, f"Data Transformation failed because: {e}")
            self.logger.log(log, f"Error occured in file: {file}")
        finally:
            log.close()

                      