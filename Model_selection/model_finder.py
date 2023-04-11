from sklearn.naive_bayes import GaussianNB
from xgboost import XGBClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.tree import DecisionTreeClassifier
from sklearn.model_selection import GridSearchCV
from sklearn.metrics  import roc_auc_score,accuracy_score
import numpy as np

class ModelFinder:
    def __init__(self,file_object,logger_object):
        self.file_object = file_object
        self.logger_object = logger_object

        
    def get_best_model(self, train_x, train_y, test_x, test_y):
        print("Entered get best model")
        self.logger_object.log(self.file_object,
                               'Entered the get_best_model method of the Model_Finder class')
        # Check the number of unique classes in the target variable of both training and testing data
        if len(np.unique(train_y)) != len(np.unique(test_y)):
            raise ValueError('Invalid classes inferred from unique values of "y"')

        # define the models and their hyperparameters to search over
        models = [
            {
                'name': 'DecisionTreeClassifier',
                'estimator': DecisionTreeClassifier(),
                'hyperparameters': {
                'criterion': ['gini', 'entropy'],
                'max_depth': [None, 5, 10],
                'min_samples_leaf': [1, 2],
                'max_features': ['sqrt']
                }
            },
            {
                'name': 'RandomForestClassifier',
                'estimator': RandomForestClassifier(),
                'hyperparameters': {
                    'n_estimators': [50, 100],
                    'criterion': ['gini', 'entropy'],
                    'min_samples_leaf': [1, 2],
                    'max_features': ['sqrt']
                }

            },
            {
                'name': 'XGBClassifier',
                'estimator': XGBClassifier(objective='binary:logistic',n_jobs=-1),
                'hyperparameters': {
                    'learning_rate': [0.05, 0.1, 0.15],
                    'max_depth': [3, 5],
                    'n_estimators': [50, 100],
                    'gamma': [0, 0.1]



                }
            },
            {
                'name': 'GaussianNB',
                'estimator': GaussianNB(),
                'hyperparameters': {"var_smoothing": [1e-9, 1e-8, 1e-7, 1e-6, 1e-5, 1e-4, 1e-3, 1e-2, 1e-1, 0.5, 1.0]}
            }
        ]
        
        # initialize variables to keep track of the best model and its score
        best_model = None
        best_score = -1
        best_model_name = ""
        
        # iterate over each model and its hyperparameters
        for model in models:
            self.logger_object.log(self.file_object, f'Training {model["name"]}...')
            print("started Grid cV")
            # use GridSearchCV to find the best hyperparameters for this model
            grid = GridSearchCV(model['estimator'], model['hyperparameters'], scoring='roc_auc', cv=5 ,verbose=3)
            grid.fit(train_x, train_y)
            print("Grid Fit done")
            # make predictions on the test set using the best model
            prediction = grid.predict(test_x)
            print("Grid prediction")
            # calculate the score for this model
            if len(np.unique(test_y)) == 1:
                score = accuracy_score(test_y, prediction)
                print("score accuracy",score)
            else:
                score = roc_auc_score(test_y, grid.predict_proba(test_x)[:, 1])
                print("score auc",score)
            
            self.logger_object.log(self.file_object, f'{model["name"]} score: {score}')
            
            # if this model has a higher score than the previous best model, update the best model and score
            if score > best_score:
                best_model = grid.best_estimator_
                best_score = score
                best_model_name = model['name']
        
        # log the best model and score
        self.logger_object.log(self.file_object, f'Best model: {best_model.__class__.__name__}, score: {best_score}')
        
        return best_model_name ,best_model
    