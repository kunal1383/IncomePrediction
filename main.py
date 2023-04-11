from flask import Flask, request, render_template
from flask import Response
import os
from werkzeug.serving import make_server
from training_Validation import train_validation
from prediction_Validation import pred_validation
from trainModel import trainModel
from PredictFromModel import Prediction
from flask_cors import CORS, cross_origin


app = Flask(__name__)
CORS(app)

@app.route("/", methods=['GET'])
@cross_origin()
def home():
    return render_template('index.html')


@app.route("/train",methods = ['POST'])
@cross_origin()
def trainRouteClient():
    try:
        if request.json['folderPath'] is not None:
            path = request.json['folderPath']
            
            #object initialization
            train_validation_object = train_validation(path)
            #calling the training_validation function
            train_validation_object.train_validation_function()
            
            trainModel_object = trainModel()
            trainModel_object.trainingModel()

            return Response("Training and model training completed successfully!")
    except ValueError:
        return Response("Error Occured %s" % ValueError)
    
@app.route("/predict", methods=['POST'])
def predictRouteClient():
    print("Entering Predict method")

    # Try to parse the request as JSON
    try:
        filepath = request.json['filepath']
        print('Prediction JSON file path:', filepath)
    except:
        # If parsing as JSON fails, assume it's form data
        filepath = request.form['filepath']
        print('Prediction form data file path:', filepath)

    try:
        path = os.path.join(os.getcwd(), filepath)
        print('Complete file path:', path)
        pred_val = pred_validation(path) #object initialization
        pred_val.prediction_validation_function() #calling the prediction_validation function
        pred = Prediction(path) #object initialization
        path = pred.predictionFromModel() # predicting for dataset present in database
        return Response("Prediction File created at %s!!!" % path)
    except ValueError:
        return Response("Error Occurred! %s" %ValueError)
    except KeyError:
        return Response("Error Occurred! %s" %KeyError)
    except Exception as e:
        return Response("Error Occurred! %s" %e)



if __name__ == '__main__':
    # Use environment variables for host and port
    host = os.environ.get('FLASK_RUN_HOST', '0.0.0.0')
    port = int(os.environ.get('FLASK_RUN_PORT', 5000))

    # Use a production-ready server like Gunicorn or uWSGI
    httpd = make_server(host, port, app)
    httpd.serve_forever()


















