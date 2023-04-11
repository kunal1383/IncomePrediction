import pandas as pd
import numpy as np
from File_operation.file_operation import FileOperation
from model_logging import logger
from Data_ingestion.data_loader_prediction import Get_Data_predict
from PredictionDataValidation.PredictionDataValidation import Prediction_Raw_Data_Validation
from Data_preprocessing import preprocessing

class Prediction:

    def __init__(self,path):
        self.pred_data_val = Prediction_Raw_Data_Validation(path)
        self.file_object = open("Prediction_Logs/Prediction_Log.txt", 'a+')
        self.log_writer = logger.App_Logger()

        # Initialize KMeans model
        file_loader = FileOperation(self.file_object, self.log_writer)
        self.kmeans = file_loader.load_model('KMeans')

        # Create dictionary of model names for each cluster
        self.model_dict = {}
        for i in range(len(self.kmeans.labels_)):
            model_name = file_loader.find_correct_model_file(self.kmeans.labels_[i])
            self.model_dict[self.kmeans.labels_[i]] = model_name

    def predictionFromModel(self):
        try:
            # Delete existing prediction file
            self.pred_data_val.deletePredictionFile()

            self.log_writer.log(self.file_object, 'Start of Prediction')
            data_getter = Get_Data_predict(self.file_object, self.log_writer)
            data = data_getter.get_data()

            # Remove unnecessary columns
            preprocessor = preprocessing.Preprocesser(self.file_object, self.log_writer)
            data = preprocessor.remove_columns(data, ['education'])
            data = preprocessor.remove_unwanted_space(data)

            # Replace missing values
            data.replace('?', np.NaN, inplace=True)
            is_null_present, cols_with_missing_values = preprocessor.is_null_present(data)
            if (is_null_present):
                data = preprocessor.impute_missing_values(data, cols_with_missing_values)

            # Scale numerical columns and encode categorical columns
            scaled_num_df = preprocessor.scale_numerical_columns(data)
            cat_df = preprocessor.encode_categorical_columns(data)
            X = pd.concat([scaled_num_df, cat_df], axis=1)

            # Predict clusters
            clusters = self.kmeans.predict(X)
            X['clusters'] = clusters

            # Make predictions for each cluster
            predictions = []
            for i in np.unique(clusters):
                cluster_data = X[X['clusters'] == i]
                cluster_data = cluster_data.drop(['clusters'], axis=1)
                model = FileOperation(self.file_object, self.log_writer).load_model(self.model_dict[i])
                result = model.predict(cluster_data)
                predictions.extend(['<=50K' if res == 0 else '>50K' for res in result])

            # Save predictions to file
            final = pd.DataFrame(predictions, columns=['Predictions'])
            path = "Prediction_Output_File/Predictions.csv"
            final.to_csv(path, header=True, mode='w')

            self.log_writer.log(self.file_object, 'End of Prediction')
            return path

        except Exception as ex:
            self.log_writer.log(self.file_object, 'Error occurred while running the prediction!! Error:: %s' % ex)
            raise ex