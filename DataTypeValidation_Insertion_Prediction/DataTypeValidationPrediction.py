import os
import csv
import shutil
import sqlite3
from os import listdir
from model_logging.logger import App_Logger

class DBOperation :
    """
    Class will handle sql database operation.
    """
    def __init__(self):
        self.path = 'Prediction_Database/'
        self.badFilePath = "Prediction_Raw_files_validated/Bad_Raw"
        self.goodFilePath = "Prediction_Raw_files_validated/Good_Raw"
        self.logger = App_Logger()
    
    def DBconnection(self,DatabaseName):
        """
        Method Name: dBconnection
        Description: This method creates new database with the given name and if Database already exists then opens the connection to the DB.
        Output: Connection to the DB
        On Failure: Raise ConnectionError
        """   
        
        try:
            connection = sqlite3.connect(self.path+DatabaseName+'.db')

            file = open("Prediction_Logs/DataBaseConnectionLog.txt", 'a+')
            self.logger.log(file, "Opened %s database successfully" % DatabaseName)
            file.close()
        except ConnectionError:
            file = open("Prediction_Logs/DataBaseConnectionLog.txt", 'a+')
            self.logger.log(file, "Error while connecting to database: %s" %ConnectionError)
            file.close()
            raise ConnectionError
        return connection     
    
    
    def createTable(self ,DatabaseName ,column_names):
        """
            Method Name: createTable
            Description: This method creates a table in the given database which will be used to insert the Good data after raw data validation.
            Output: None
            On Failure: Raise Exception
        """
        conn = None
        try:
            conn = self.DBconnection(DatabaseName)
            c = conn.cursor()
            # checking if table already exist or not .If yes qury will return 1 else 0
            c.execute("SELECT count(name) FROM sqlite_master WHERE type = 'table' AND name = 'Good_Raw_Data'")
            if c.fetchone()[0] == 1:
                conn.close()
                file = open("Prediction_Logs/DbTableCreateLog.txt", 'a+')
                self.logger.log(file,"Tables already exist in the database!")
                file.close()

            else:
                for key in column_names.keys():
                    data_type = column_names[key]

                    try:
                        conn.execute('ALTER TABLE Good_Raw_Data ADD COLUMN "{column_name}" {dataType}'.format(column_name=key, dataType=data_type))
                        file = open("Prediction_Logs/DbTableCreateLog.txt", 'a+')
                        self.logger.log(file,"Column %s added to Good_Raw_Data table!" % key)
                        file.close()
                    except sqlite3.OperationalError:
                        conn.execute('CREATE TABLE Good_Raw_Data ({column_name} {dataType})'.format(column_name=key, dataType=data_type))
                        file = open("Prediction_Logs/DbTableCreateLog.txt", 'a+')
                        self.logger.log(file,"Table Good_Raw_Data created with column %s!" % key)
                        file.close()

                conn.commit()
                conn.close()
                file = open("Prediction_Logs/DbTableCreateLog.txt", 'a+')
                self.logger.log(file,"Tables created successfully!")
                file.close()
        except Exception as e:
            file = open("Prediction_Logs/DbTableCreateLog.txt", 'a+')
            self.logger.log(file,"Error while creating table: %s " % e)
            file.close()
            if conn:
                conn.close()
            raise e




    def InsertIntoTable(self,DatabaseName):
        """
        Method Name: createTableDb
        Description: This method creates a table in the given database which will be used to insert the Good data after raw data validation.
        Output: None
        On Failure: Raise Exception
        """ 
        
        conn = self.DBconnection(DatabaseName)
        goodFilePath = self.goodFilePath
        badFilePath = self.badFilePath
        onlyfiles = [f for f in listdir(goodFilePath)]
        log_file = open("Prediction_Logs/DbInsertLog.txt", 'a+')
        try:
            for file in onlyfiles:
                with open(os.path.join(goodFilePath, file), "r") as f:
                    next(f)
                    reader = csv.reader(f)
                    rows = [tuple(row) for row in reader]
                    conn.executemany("INSERT INTO Good_Raw_Data VALUES (?{})".format(",?"*(len(rows[0])-1)), rows)
                    self.logger.log(log_file," %s: File loaded successfully!!" % file)
                    conn.commit()
        except Exception as e:
            conn.rollback()
            self.logger.log(log_file,"Error while creating table: %s " % e)
            shutil.move(os.path.join(goodFilePath, file), badFilePath)
            self.logger.log(log_file, "File Moved Successfully %s" % file)

        conn.close()
        log_file.close()  
        
    def selectingDatafromtableintocsv(self, Database):
        self.fileFromDb = 'Prediction_FileFromDB/'
        self.fileName = 'InputFile.csv'
        log_file = open("Prediction_Logs/ExportToCsv.txt", 'a+')
        try:
            conn = self.DBconnection(Database)
            sqlSelect = "SELECT *  FROM Good_Raw_Data"
            cursor = conn.cursor()
            cursor.execute(sqlSelect)
            results = cursor.fetchall()
            # Get the headers of the csv file
            headers = [i[0] for i in cursor.description]

            #Make the CSV ouput directory
            os.makedirs(self.fileFromDb, exist_ok=True)

            # Open CSV file for writing.
            with open(self.fileFromDb + self.fileName, 'w', newline='') as f:
                csvFile = csv.writer(f, delimiter=',', lineterminator='\r\n', quoting=csv.QUOTE_ALL, escapechar='\\')

                # Add the headers and data to the CSV file.
                csvFile.writerow(headers)
                csvFile.writerows(results)

            self.logger.log(log_file, "File exported successfully!!!")
            
        except Exception as e:
            self.logger.log(log_file, "File exporting failed. Error : %s" %e)
        finally:
            log_file.close()        