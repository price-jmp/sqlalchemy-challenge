# Import the dependencies.
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, text, func
import datetime

from flask import Flask, jsonify
import pandas as pd
from sqlHelper import SQLHelper


#################################################
# Database Setup
#################################################
class SQLHelper():
    def __init__(self):
        self.engine = create_engine("sqlite:///hawaii.sqlite")
        self.Base = None
        self.init_base()

    # Reflect an existing database into a new model
    # Reflect the tables
    def init_base(self):
        self.Base = automap_base()
        self.Base.prepare(autoload_with = self.engine)

#################################################
# Database Queries
#################################################
    def query_precipitation(self):
        # Find the most recent date in the dataset. Using that date, get the previous 12 months of 
            # precipitation data by querying the previous 12 months of data.
            # Convert the query results from your precipitation analysis to a dictionary using 
            # date as the key and prcp as the value.
        query  =  """
                  SELECT 
                    date AS Date, 
                    prcp AS Precipitation
                  FROM 
                    measurement
                  WHERE 
                    Date <= "2017-08-23" AND Date >= "2016-08-23"
                  ORDER BY 
                    Date ASC;
                  """
        df = pd.read_sql(text(query), con = self.engine)
        data = df.to_dict(orient = "records")
        return(data)
    
    def query_stations(self):
        # Return a JSON list of all stations in the dataset
        query = """
                SELECT
                    id,
                    station,
                    name
                FROM
                    station
                """
        df = pd.read_sql(text(query), con = self.engine)
        data = df.to_dict(orient = "records")
        return(data)
    
    def query_tobs(self):
        # Query the dates and temperature observations of the most-active station for the previous year of data.
        # Return a JSON list of temperature observations for the previous year.
        query = """
                SELECT
                    date,
                    tobs,
                FROM 
                    measurement
                WHERE 
                    date <= "2017-08-23" AND Date >= "2016-08-23"
                    station = "USC00519281"
                """

        df = pd.read_sql(text(query), con = self.engine)
        data = df.to_dict(orient = "records")
        return(data)
    
    def query_tobs_start_end(self, start, end):
        query = """
        SELECT
            MIN(tobs) AS min_temp,
            MAX(tobs) AS max_temp,
            AVG(tobs) AS avg_temp
        FROM 
            measurement
        WHERE 
            date >= "{start}"
            AND date < "{end}";
        """

        df = pd.read_sql(text(query), con = self.engine)
        data = df.to_dict(orient = "records")
        return(data)
    

#################################################
# Flask Setup
#################################################

app = Flask(__name__)
sql = SQLHelper()

#################################################
# Flask Routes
#################################################

@app.route("/")
def welcome():
    return (
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/2017-01-01/2017-08-23"
    )

@app.route("/api/v1.0/precipitation")
def precipitation():
    data = sql.query_precipitation()
    return(jsonify(data))

@app.route("/api/v1.0/stations")
def stations():
    data = sql.query_stations()
    return(jsonify(data))

@app.route("/api/v1.0/tobs")
def tobs():
    data = sql.query_tobs()
    return(jsonify(data))

# Date format is YYYY-MM-DD
@app.route("/api/v1.0/<start>/<end>")
def tobs_start_end(start, end):
    data = sql.query_tobs_start_end(start, end)
    return(jsonify(data))


# Run the App
if __name__ == '__main__':
    app.run(debug=True)