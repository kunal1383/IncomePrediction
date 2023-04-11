import pandas as pd
from imblearn.over_sampling import RandomOverSampler
from sklearn.impute import KNNImputer
from sklearn.preprocessing import StandardScaler 
from sklearn.preprocessing import LabelEncoder
from sklearn.impute import SimpleImputer
import re

class Preprocesser:
    
    def __init__(self, file_object, logger_object):
        self.file_object = file_object
        self.logger_object = logger_object
        
    def impute_missing_values(self, data, cols_with_missing_values):
        """Method Name: impute_missing_values
        Description: This method replaces all the missing values in the DataFrame using Simple Imputer.
        Output: A DataFrame which has all the missing values imputed.
        On Failure: Raise Exception
        
        create a copy of the original data to avoid changing it"""
        imputed_data = data.copy()
        
        # create a SimpleImputer object for numeric missing values
        num_imputer = SimpleImputer(strategy='mean')
        num_cols_with_missing_values = [col for col in cols_with_missing_values if imputed_data[col].dtype != 'object']
        if len(num_cols_with_missing_values) > 0:
            imputed_data[num_cols_with_missing_values] = num_imputer.fit_transform(imputed_data[num_cols_with_missing_values])

        # create a SimpleImputer object for categorical missing values
        cat_imputer = SimpleImputer(strategy='most_frequent')
        cat_cols_with_missing_values = [col for col in cols_with_missing_values if imputed_data[col].dtype == 'object']
        if len(cat_cols_with_missing_values) > 0:
            imputed_data[cat_cols_with_missing_values] = cat_imputer.fit_transform(imputed_data[cat_cols_with_missing_values])

        return imputed_data        

    # def impute_missing_values(self, data, cols_with_missing_values):
    #     """
    #     Method Name: impute_missing_values
    #     Description: This method replaces all the missing values in the Dataframe using KNN Imputer.
    #     Output: A Dataframe which has all the missing values imputed.
    #     On Failure: Raise Exception
    #     """
    #     self.logger_object.log(self.file_object, 'Entered the impute_missing_values method of the Preprocessor class')
    #     self.data= data
    #     self.cols_with_missing_values=cols_with_missing_values
    #     try:
    #         # Convert categorical columns to numeric using LabelEncoder
    #         for col in self.cols_with_missing_values:
    #             if self.data[col].dtype == 'object':
    #                 label_encoder = LabelEncoder()
    #                 self.data[col] = label_encoder.fit_transform(self.data[col])

    #         imputer = KNNImputer()
    #         self.data[self.cols_with_missing_values] = imputer.fit_transform(self.data[self.cols_with_missing_values])
    #         self.logger_object.log(self.file_object, 'Imputing missing values Successful. Exited the impute_missing_values method of the Preprocessor class')
    #     except Exception as e:
    #         self.logger_object.log(self.file_object,f'Exception occurred in impute_missing_values method of the Preprocessor class. Exception message: {str(e)}')
    #         self.logger_object.log(self.file_object,'Imputing missing values failed. Exited the impute_missing_values method of the Preprocessor class')
    #         raise Exception("Missing value imputation failed.")
        
    #     return self.data    
    # def impute_missing_values(self, data, cols_with_missing_values):
    #     """
    #     Method Name: impute_missing_values
    #     Description: This method replaces all the missing values in the Dataframe using KNN Imputer.
    #     Output: A Dataframe which has all the missing values imputed.
    #     On Failure: Raise Exception
    #     """
    #     self.logger_object.log(self.file_object, 'Entered the impute_missing_values method of the Preprocessor class')
    #     self.data= data
    #     self.cols_with_missing_values=cols_with_missing_values
    #     try:
    #         imputer = KNNImputer()
    #         self.data[self.cols_with_missing_values] = imputer.fit_transform(self.data[self.cols_with_missing_values])
    #         self.logger_object.log(self.file_object, 'Imputing missing values Successful. Exited the impute_missing_values method of the Preprocessor class')
    #     except Exception as e:
    #         self.logger_object.log(self.file_object,f'Exception occurred in impute_missing_values method of the Preprocessor class. Exception message: {str(e)}')
    #         self.logger_object.log(self.file_object,'Imputing missing values failed. Exited the impute_missing_values method of the Preprocessor class')
    #         raise Exception("Missing value imputation failed.")
        
    #     return self.data
    
    

    def remove_unwanted_space(self, data):
        """
        Method Name: remove_unwanted_space
        Description: This method removes the unwanted spaces from a pandas dataframe.
        Output: A pandas DataFrame after removing the spaces.
        On Failure: Raise Exception
        """
        self.logger_object.log(self.file_object, 'Entered the remove_unwanted_space method of the Preprocessor class')
        self.data = data

        try:
            self.df_without_spaces = self.data.apply(lambda x: x.str.replace(' ', '') if x.dtype == "object" else x)
            self.logger_object.log(self.file_object, 'Unwanted spaces removed successfully. Exiting the remove_unwanted_spaces method of the Preprocessor class')
            return self.df_without_spaces
        except Exception as e:
            self.logger_object.log(self.file_object, 'Exception occurred in remove_unwanted_spaces method of the Preprocessor class. Exception message: ' + str(e))
            self.logger_object.log(self.file_object, 'Unwanted space removal unsuccessful. Exited the remove_unwanted_spaces method of the Preprocessor class')
            raise Exception()
    # def remove_unwanted_space(self,data):
    #     """
    #     Method Name: remove_unwanted_spaces
    #     Description: This method removes the unwanted spaces from a pandas dataframe.
    #     Output: A pandas DataFrame after removing the spaces.
    #     On Failure: Raise Exception
    #     """
    #     self.logger_object.log(self.file_object, 'Entered the remove_unwanted_spaces method of the Preprocessor class')
    #     self.data = data

    #     try:
    #         self.df_without_spaces = self.data.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
    #         self.logger_object.log(self.file_object, 'Unwanted spaces removed Successful. Exiting the remove_unwanted_spaces method of the Preprocessor class')
    #         return self.df_without_spaces
    #     except Exception as e:
    #         self.logger_object.log(self.file_object, 'Exception occurred in remove_unwanted_spaces method of the Preprocessor class. Exception message: ' + str(e))
    #         self.logger_object.log(self.file_object, 'Unwanted space removal Unsuccessful. Exited the remove_unwanted_spaces method of the Preprocessor class')
    #         raise Exception()
        
        
    def remove_columns(self ,data, columns):
        """
        Method Name: remove_columns
        Description: This method removes the given columns from a pandas dataframe.
        Output: A pandas DataFrame after removing the specified columns.
        On Failure: Raise Exception
        """    
        
        self.logger_object.log(self.file_object, 'Entered the remove_columns method of the Preprocessor class')
        self.data=data
        self.columns=columns
        
        try:
            self.useful_data=self.data.drop(columns=self.columns, axis=1)
            self.logger_object.log(self.file_object,
                                    'Column removed Successful.Exited the remove_columns method of the Preprocessor class')
            return self.useful_data 
          
        except Exception as e:
            self.logger_object.log(self.file_object,'Exception occured in remove_columns method of the Preprocessor class. Exception message:  '+str(e))
            self.logger_object.log(self.file_object,
                                   'Column removed Unsuccessful. Exited the remove_columns method of the Preprocessor class')
            raise Exception()
        
    def separate_label_feature(self, data, label_column_name):
        """
        Method Name: separate_label_feature
        Description: This method separates the features and a Label Coulmns.
        Output: Returns two separate Dataframes, one containing features and the other containing Labels .
        On Failure: Raise Exception
        """
        
        self.logger_object.log(self.file_object, 'Entered the separate_label_feature method of the Preprocessor class')
        try:
            X = data.drop(columns=label_column_name)
            Y = data[label_column_name]
            self.logger_object.log(self.file_object, 'Label Separated Successfully. Exiting the separate_label_feature method of the Preprocessor class')
            return X, Y
        except Exception as e:
            self.logger_object.log(self.file_object, f'Exception occurred in separate_label_feature method of the Preprocessor class. Exception message: {str(e)}')
            self.logger_object.log(self.file_object, 'Label Separation Unsuccessful. Exited the separate_label_feature method of the Preprocessor class')
            
            raise Exception("Label separation failed.")
    
    
    def is_null_present(self, data):
        """
        Method Name: is_null_present
        Description: This method checks whether there are null values present in the pandas Dataframe or not.
        Output: Returns True if null values are present in the DataFrame, False if they are not present and returns the list of columns for which null values are present.
        On Failure: Raise Exception
        """
        self.logger_object.log(self.file_object, 'Entered the is_null_present method of the Preprocessor class')
        null_counts = data.isna().sum() # check for the count of null values per column
        cols_with_missing_values = list(data.columns[null_counts > 0])
        null_present = len(cols_with_missing_values) > 0
        if null_present:
            dataframe_with_null = pd.DataFrame({'columns': cols_with_missing_values,
                                                'missing values count': null_counts[cols_with_missing_values]})
            dataframe_with_null.to_csv('preprocessing_data/null_values.csv') # storing the null column information to file
            self.logger_object.log(self.file_object, 'Finding missing values is a success. Data written to the null values file. Exited the is_null_present method of the Preprocessor class')
        else:
            self.logger_object.log(self.file_object, 'No missing values found. Exited the is_null_present method of the Preprocessor class')
        return null_present, cols_with_missing_values
    
    
    
    def scale_numerical_columns(self,data):
        """
        Method Name: scale_numerical_columns
        Description: This method scales the numerical values using the Standard scaler.
        Output: A dataframe with scaled
        On Failure: Raise Exception
        """
        self.logger_object.log(self.file_object,
                               'Entered the scale_numerical_columns method of the Preprocessor class')

        self.data=data

        try:
            self.num_df = self.data.select_dtypes(include=['int64']).copy()
            self.scaler = StandardScaler()
            self.scaled_data = self.scaler.fit_transform(self.num_df)
            self.scaled_num_df = pd.DataFrame(data=self.scaled_data, columns=self.num_df.columns)

            self.logger_object.log(self.file_object,f'After scaling the Df :{self.scaled_num_df.shape[0]}')
            self.logger_object.log(self.file_object, 'scaling for numerical values successful. Exited the scale_numerical_columns method of the Preprocessor class')
            return self.scaled_num_df

        except Exception as e:
            self.logger_object.log(self.file_object,'Exception occured in scale_numerical_columns method of the Preprocessor class. Exception message:  ' + str(e))
            self.logger_object.log(self.file_object, 'scaling for numerical columns Failed. Exited the scale_numerical_columns method of the Preprocessor class')
            raise Exception()
        
    def encode_categorical_columns(self, data):
        """ 
        Method Name: encode_categorical_columns
        Description: This method encodes the categorical values to numeric values.
        Output: only the columns with categorical values converted to numerical values
        On Failure: Raise Exception
        """
        
        
        self.logger_object.log(self.file_object, 'Entered the encode_categorical_columns method of the Preprocessor class')

        try:
            cat_cols = data.select_dtypes(include=['object']).columns
            encoded_df = pd.get_dummies(data[cat_cols], columns=cat_cols, prefix=cat_cols, drop_first=True)

            self.logger_object.log(self.file_object, f'After encoding the Df :{encoded_df.shape[0]}')
            self.logger_object.log(self.file_object, 'Encoding for categorical columns successful. Exited the encode_categorical_columns method of the Preprocessor class')
            return encoded_df

        except Exception as e:
            self.logger_object.log(self.file_object, 'Exception occurred in encode_categorical_columns method of the Preprocessor class. Exception message: ' + str(e))
            self.logger_object.log(self.file_object, 'Encoding for categorical columns failed. Exited the encode_categorical_columns method of the Preprocessor class')
            raise Exception()
   
   
    def handle_imbalanced_dataset(self, x, y):
        
        """
        Method Name: handle_imbalanced_dataset
        Description: This method handles the imbalanced dataset to make it a balanced one.
        Output: new balanced feature and target columns
        On Failure: Raise Exception
        """
        
        self.logger_object.log(self.file_object, 'Entered the handle_imbalanced_dataset method of the Preprocessor class')
        
        try:
            rdsmple = RandomOverSampler()
            x_sampled, y_sampled = rdsmple.fit_resample(x, y)
            self.logger_object.log(self.file_object, 'Dataset balancing successful. Exited the handle_imbalanced_dataset method of the Preprocessor class')
            return x_sampled, y_sampled
        except Exception as e:
            self.logger_object.log(self.file_object, 'Exception occured in handle_imbalanced_dataset method of the Preprocessor class. Exception message: ' + str(e))
            self.logger_object.log(self.file_object, 'Dataset balancing failed. Exited the handle_imbalanced_dataset method of the Preprocessor class')
            raise e