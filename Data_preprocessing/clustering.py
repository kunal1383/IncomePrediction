from sklearn.cluster import KMeans
from kneed import KneeLocator
import matplotlib.pyplot as plt
from File_operation.file_operation import FileOperation


class KMeansClustering:
    def __init__(self, file_object, logger_object):
        self.file_object = file_object
        self.logger_object = logger_object
        

    def elbow_plot(self, data):
        """
            Plots the elbow curve and returns the optimum number of clusters using the KneeLocator algorithm.
            Args:
            - data : numpy array : Input data on which clustering is to be performed.
            Returns:
            - int : The optimum number of clusters.
            Raises:
            - Exception : If any exception occurs during the execution of the method.
            """
        self.logger_object.log(self.file_object, 'Entered the elbow_plot method of the KMeansClustering class')
        wcss = []  # initializing an empty list
        try:
            for i in range(1, 11):
                kmeans = KMeans(n_clusters=i, init='k-means++', random_state=42)  # initializing the KMeans object
                kmeans.fit(data)  # fitting the data to the KMeans Algorithm
                wcss.append(kmeans.inertia_)
            
            kn = KneeLocator(range(1, 11), wcss, curve='convex', direction='decreasing')
            #bx- represent color ='blue' ,x = 'data' , - ='line represent'
            plt.plot(wcss, 'bx-')
            plt.xlabel('Number of clusters (k)')
            plt.ylabel('Within-cluster sum of squares (WCSS)')
            plt.title('The Elbow Method')
            plt.vlines(kn.knee, plt.ylim()[0], plt.ylim()[1], linestyles='dashed')
            plt.savefig('preprocessing_data/K-Means_Elbow.PNG')  # saving the elbow plot locally
            self.logger_object.log(self.file_object, 'The optimum number of clusters is: '+str(kn.knee)+' . Exited the elbow_plot method of the KMeansClustering class')
            return kn.knee
        except Exception as e:
            self.logger_object.log(self.file_object,'Exception occurred in elbow_plot method of the KMeansClustering class. Exception message:  ' + str(e))
            self.logger_object.log(self.file_object,'Finding the number of clusters failed. Exited the elbow_plot method of the KMeansClustering class')
            raise Exception()
        
    def create_clusters(self, data, number_of_clusters):
        """
        Divides the input data into specified number of clusters using KMeans algorithm and adds a new column to the dataset
        for storing the cluster information.

        Args:
            data: A pandas dataframe containing the input data.
            number_of_clusters: An integer value specifying the number of clusters to divide the data into.

        Returns:
            A pandas dataframe with a new column added containing the cluster information.

        Raises:
            Exception: If fitting the data to clusters fails.
        """
        self.logger_object.log(self.file_object, 'Entered the create_clusters method of the KMeansClustering class')
        
        try:
            kmeans = KMeans(n_clusters=number_of_clusters, init='k-means++',  random_state=42)
            y_kmeans = kmeans.fit_predict(data) #  divide data into clusters

            file_op = FileOperation(self.file_object,self.logger_object)
            save_model = file_op.save_model(kmeans, 'KMeans') # saving the KMeans model to directory

            data_with_clusters = data.copy()
            data_with_clusters['Clusters'] = y_kmeans  # create a new column in dataset for storing the cluster information
            
            self.logger_object.log(self.file_object, f'Successfully created {number_of_clusters} clusters. Exited the create_clusters method of the KMeansClustering class')
            return data_with_clusters
    
        except Exception as e:
            self.logger_object.log(self.file_object,'Exception occurred in create_clusters method of the KMeansClustering class. Exception message:  ' + str(e))
            self.logger_object.log(self.file_object,'Fitting the data to clusters failed. Exited the create_clusters method of the KMeansClustering class')
            raise Exception()  
                    