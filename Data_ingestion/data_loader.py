import pandas as pd

class Get_Data:
    """
    Class is used to obtain the Data from the input file.
    """
    def __init__(self, file_object, logger_object):
        self.training_file='Training_FileFromDB/InputFile.csv'
        self.file_object=file_object
        self.logger_object=logger_object
    
    def get_data(self):
        """
        Method Name: get_data
        Description: This method reads the data from source.
        Output: A pandas DataFrame.
        On Failure: Raise Exception
        """ 
        self.logger_object.log(self.file_object,'Entered the get_data method of the Get Data class')   
        
        try:
            self.data= pd.read_csv(self.training_file) 
            self.logger_object.log(self.file_object,'Data Load Successful.Exited the get_data method of the Get_Data class')
            return self.data
        except Exception as e:
            self.logger_object.log(self.file_object,'Exception occured in get_data method of the Get_Data class. Exception message: '+str(e))
            self.logger_object.log(self.file_object,
                                   'Data Load Unsuccessful.Exited the get_data method of the Get_Data class')
            raise Exception()