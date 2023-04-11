from model_logging.logger import App_Logger
import pandas as pd
import numpy as np
from Data_ingestion.data_loader import Get_Data
from Data_preprocessing.preprocessing import Preprocesser
from Data_preprocessing.clustering import KMeansClustering
from sklearn.model_selection import train_test_split
from Model_selection.model_finder import ModelFinder
from File_operation.file_operation import FileOperation
from sklearn.preprocessing import LabelEncoder

class trainModel:
    def __init__(self):
        self.log_writer = App_Logger()
        self.file_object = open("Training_Logs/ModelTrainingLog.txt", 'a+') 
    
    def trainingModel(self):
        # Logging the start of Training
        self.log_writer.log(self.file_object, 'Start of Training')
        try:
            # Getting the data from the source
            get_data = Get_Data(self.file_object,self.log_writer)
            data = get_data.get_data()
            
            #Data preprocessing
            preprocessor = Preprocesser(self.file_object ,self.log_writer)
            # removing education column as data as education_num which is numerical equivalent of education column
            data = preprocessor.remove_columns(data ,['education'])
            data = preprocessor.remove_unwanted_space(data)
            # replacing '?' with NaN values for imputation
            data.replace('?',np.NaN,inplace=True) 
            
            # create separate features and labels
            X,Y=preprocessor.separate_label_feature(data,label_column_name='Income')
            # print("All The Y values after seperate",Y)
            # print("Y value", Y.dtype )
            # print("Y null value",Y.isna().sum())
            # # # if missing values are present in the target column, remove those rows
            # if Y.isna().sum() > 0:
            #     Y = Y.dropna()
            #     X = X.drop(Y.index)
            #     self.log_writer.log(self.file_object, f'Removed {Y.isnull().sum()} rows with missing values from target column')

            # encoding the label column as income has only two values

            # encode categorical variable
            le = LabelEncoder()
            Y_encoded = pd.Series(le.fit_transform(Y))


            # check if missing values are present in the dataset
            is_null_present,cols_with_missing_values=preprocessor.is_null_present(X)
            
            # if missing values are there, replace them appropriately.
            if(is_null_present):
                # missing value imputation
                X=preprocessor.impute_missing_values(X,cols_with_missing_values)
                print(f'Before Dropping :{X.shape[0]} and Y:{Y.shape[0]}')
                self.log_writer.log(self.file_object, f'After Dropping Y NAN X:{X.shape[0]} and Y:{Y.shape[0]}')

            # print(f"Before Normalizing X and Y: {X.shape}, {Y.shape}")
            # Normalizing and converting categorical data to numerical data
            scaled_num_df=preprocessor.scale_numerical_columns(X)
            cat_df=preprocessor.encode_categorical_columns(X)
            # print(f"Current scaled_df and cat_df: {scaled_num_df.shape}, {cat_df.shape}")
            X=pd.concat([scaled_num_df,cat_df], axis=1)
            # print(f"Current X and Y: {X.shape}, {Y.shape}")
            # drop rows with 'nan' value
            nan_index = Y_encoded[Y_encoded == 2].index
            Y_encoded = Y_encoded.drop(nan_index)
            X = X.drop(nan_index)

            # map encoded values to 0 and 1
            Y = Y_encoded.map({0: 0, 1: 1})
            # print(Y.unique(), Y.value_counts())
            # Y = Y.map({"'<=50K'": int(0), "'>50K'": int(1)})
            # print("Y dtype after encode", Y.dtype)
            # print(Y.unique(), Y.value_counts())
            # print("All The Y values after encode", Y)
            # print(f'Before Dropping :{X.shape[0]} and Y:{Y.shape[0]}')
            self.log_writer.log(self.file_object, f'After Normalizing :{X.shape[0]} and Y:{Y.shape[0]}')
            #Applying the oversampling approach to handle imbalanced dataset
            X,Y=preprocessor.handle_imbalanced_dataset(X,Y)


            
            #Applying the clustering approach
            # print("Entered KMeans")
            kmeans=KMeansClustering(self.file_object,self.log_writer)
            #  using the elbow plot to find the number of optimum clusters
            number_of_clusters=kmeans.elbow_plot(X)
            # print("Created Elbow Plot")
            # Divide the data into clusters
            X=kmeans.create_clusters(X,number_of_clusters)
            # print("Created X Clustred")
            #create a new column in the dataset consisting of the corresponding cluster assignments.
            X['Labels']=Y

            # getting the unique clusters from our dataset
            list_of_clusters=X['Clusters'].unique()
            # print("List of Clusters",list_of_clusters)
            for i in list_of_clusters:
                # filter the data according to the cluster number
                cluster_data=X[X['Clusters']==i]
                # print(f"Clusters_data created:{i}")
                # Prepare the feature and Label columns for each clusters
                cluster_features=cluster_data.drop(['Labels','Clusters'],axis=1)
                cluster_label= cluster_data['Labels']
                # print("Clusters_data with labels and features created")
                # splitting the data into training and test set for each cluster one by one
                x_train, x_test, y_train, y_test = train_test_split(cluster_features, cluster_label, test_size=0.25, random_state=300)
                # print("Data split into train and test")
                # creating instance of modelFinder
                model_finder = ModelFinder(self.file_object ,self.log_writer)
                if len(np.unique(cluster_label)) < 2:
                    print(f"Skipping cluster {i} due to insufficient label classes")
                    continue
                #Getting best model for cluster
                best_model_name,best_model= model_finder.get_best_model(x_train,y_train,x_test,y_test)
                
                #saving the best model to the directory.
                file_op = FileOperation(self.file_object,self.log_writer)
                save_model=file_op.save_model(best_model,best_model_name+str(i))
                
                # logging the successful Training
                self.log_writer.log(self.file_object, 'Successful End of Training')

            # Close the file object after all iterations of the loop are completed
            self.file_object.close()
        
        except Exception as e:
            # logging the unsuccessful Training
            self.log_writer.log(self.file_object, 'Unsuccessful End of Training')
            self.file_object.close()
            raise Exception  
        
          