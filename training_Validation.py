from model_logging import logger
from Training_Raw_data_validation.raw_data_Validation import Raw_Data_Validation
from Training_Data_Transformation.data_transform import dataTransform
from DataTypeValidation_Insertion_Training.DataTypeValidation import DBOperation


class train_validation:
    def __init__(self, path):
        self.raw_data = Raw_Data_Validation(path)
        self.file_object = open("Training_Logs/Training_Main_Log.txt", 'a+')
        self.log_writer = logger.App_Logger()
        self.dataTransform = dataTransform()
        self.dbOperation = DBOperation()
        
    def train_validation_function(self):
        try:
            self.log_writer.log(self.file_object ,"Starting Validation of files for predection")
            
            # extracting values from prediction schema
            LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, noofcolumns = self.raw_data.valuesFromSchema()
            
            # creating regex object validate filename
            regex = self.raw_data.manualRegexCreation()
            
            # validating filename of prediction files
            self.raw_data.validationFileNameRaw(regex, LengthOfDateStampInFile, LengthOfTimeStampInFile)
            
             # replacing blanks in the csv file with "Null" values to insert in table
            self.dataTransform.replaceMissingWithNull()
            self.log_writer.log(self.file_object, "DataTransformation Completed!!!")
            
            
            self.log_writer.log(self.file_object,
                                "Creating Training_Database and tables on the basis of given schema!!!")
            # create database with given name, if present open the connection! Create table with columns given in schema.
            self.dbOperation.createTable('Training' ,column_names)
            self.log_writer.log(self.file_object, "Table creation Completed!!")
            self.log_writer.log(self.file_object, "Insertion of Data into Table started!!!!")
            
            # insert csv files in the table
            self.dbOperation.InsertIntoTable('Training')
            self.log_writer.log(self.file_object, "Insertion in Table completed!!!")
            self.log_writer.log(self.file_object, "Deleting Good Data Folder!!!")
            
            # Delete the good data folder after loading files in table
            self.raw_data.deleteExistingGoodDataTrainingFolder()
            self.log_writer.log(self.file_object, "Good_Data folder deleted!!!")
            self.log_writer.log(self.file_object, "Moving bad files to Archive and deleting Bad_Data folder!!!")
            
            # Move the bad files to archive folder
            self.raw_data.moveBadFilesToArchive()
            self.log_writer.log(self.file_object, "Bad files moved to archive!! Bad folder Deleted!!")
            self.log_writer.log(self.file_object, "Validation Operation completed!!")
            self.log_writer.log(self.file_object, "Extracting csv file from table")
              
            # export data in table to csvfile
            self.dbOperation.selectingDatafromtableintocsv('Training')
            self.file_object.close()
            
        except Exception as e:
            raise e    