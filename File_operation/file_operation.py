import os
import pickle
import shutil

class FileOperation:
    """
    The FileOperation class contains methods for saving and loading machine learning models to/from files, as well as finding the correct model file for a given cluster number.

    The class takes two parameters: a file_object for logging messages and a logger_object for writing logs to a file.
    """
    
    def __init__(self ,file_object ,logger_object):
        self.file_object = file_object
        self.logger_object = logger_object
        self.model_directory='models/'
    
    def save_model(self ,model ,filename):
        """
        Saves a machine learning model to a file.
        Args:
            model: The trained machine learning model object to be saved.
            filename: The name of the file to which the model should be saved.
        Returns:
            A string indicating the success or failure of the operation.
        Raises:
            Exception: If an error occurs while attempting to save the model file.
        Example:
            save_model(model, 'model.pkl')
        """
        
        self.logger_object.log(self.file_object, 'Entered the save_model method of the FileOperation class')
        try:
            #create seperate directory for each cluster
            path = os.path.join(self.model_directory,filename) 
            #remove previously existing models for each clusters
            if os.path.isdir(path): 
                shutil.rmtree(self.model_directory)
                os.makedirs(path)
            else:
                os.makedirs(path) 
            with open(path +'/' + filename+'.sav',
                      'wb') as f:
                # save the model to file
                pickle.dump(model, f) 
            self.logger_object.log(self.file_object,
                                   'Model File '+filename+' saved. Exited the save_model method of the Model_Finder class')

            return 'success'
        except Exception as e:
            self.logger_object.log(self.file_object,'Exception occured in save_model method of the Model_Finder class. Exception message:  ' + str(e))
            self.logger_object.log(self.file_object,
                                   'Model File '+filename+' could not be saved. Exited the save_model method of the Model_Finder class')
            raise Exception() 
    
       
        
    def load_model(self, filename):
        """
        Load the trained model from a file.

        Args:
        filename (str): Name of the file containing the saved model.

        Returns:
        model: Loaded model.

        Raises:
        Exception: If an error occurs while loading the model.
        """
        self.logger_object.log(self.file_object, 'Entered the load_model method of the File_Operation class')
        try:
            with open(os.path.join(self.model_directory, filename, filename + '.sav'), 'rb') as f:
                model = pickle.load(f) # Load the model from the file
                self.logger_object.log(self.file_object, 'Model File ' + filename + ' loaded. Exited the load_model method of the Model_Finder class')
                return model
        except Exception as e:
            self.logger_object.log(self.file_object, 'Exception occured in load_model method of the Model_Finder class. Exception message:  ' + str(e))
            self.logger_object.log(self.file_object, 'Model File ' + filename + ' could not be saved. Exited the load_model method of the Model_Finder class')
            raise Exception()
        
        
    def find_correct_model_file(self, cluster_number):
        """
        Finds the correct model file for the given cluster number in the model directory.
        
        Args:
        cluster_number (int): The cluster number for which the model file needs to be found.
        
        Returns:
        str: The name of the model file for the given cluster number.
        
        Raises:
        Exception: If the model file for the given cluster number is not found in the model directory.
        """
        self.logger_object.log(self.file_object, 'Entered the find_correct_model_file method of the File_Operation class')
        try:
            self.cluster_number = cluster_number
            self.folder_name = self.model_directory
            self.list_of_model_files = []
            self.list_of_files = os.listdir(self.folder_name)
            for self.file in self.list_of_files:
                try:
                    # converting cluster_number to string and checking if the number is present in the file name
                    if (self.file.index(str(self.cluster_number)) != -1):
                        self.model_name = self.file
                except:
                    continue
            self.model_name = self.model_name.split('.')[0]
            self.logger_object.log(self.file_object,
                                'Exited the find_correct_model_file method of the Model_Finder class.')
            return self.model_name
        except Exception as e:
            self.logger_object.log(self.file_object,
                                'Exception occurred in find_correct_model_file method of the Model_Finder class. Exception message:  ' + str(e))
            self.logger_object.log(self.file_object,
                                'Exited the find_correct_model_file method of the Model_Finder class with failure')
            raise Exception()    