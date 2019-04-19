import numpy as np
import pandas as pd

import datetime as dt
from datetime import datetime
from datetime import date, time
from dateutil.parser import parse

from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func, inspect,desc

from flask import Flask, jsonify

#################################################
# Database Setup
#################################################
engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(engine, reflect=True)

# Save references to each table
Measurement = Base.classes.measurement
Station = Base.classes.station

# Create our session (link) from Python to the DB
session = Session(engine)

#################################################
#Query data from Database
#################################################
last_date= list(np.ravel(session.query(func.max(Measurement.date)).all()))
last_date = parse(last_date[0])
#Calculate the first date of last 12 months of the Measurement data 
first_date = last_date - dt.timedelta(days=365)

# Design a query to retrieve the last 12 months of precipitation data and plot the results
data_prcp = session.query(Measurement.station,Measurement.date,Measurement.prcp)\
.filter(Measurement.date > str(first_date-dt.timedelta(days=1)))\
.order_by(Measurement.date).all()
# Save the query results as a Pandas DataFrame and set the index to the date column
PRCP_data = pd.DataFrame(data_prcp,columns=["Station","Date","PRCP"])
#Fill NaN with zero
PRCP_data.fillna({'PRCP': 0},inplace=True)

#Station information
station_count = session.query(Measurement.station,func.count(Measurement.date))\
.filter(Measurement.station == Station.station)\
.group_by(Station.station)\
.order_by(desc(func.count(Measurement.date))).all()

#Tobs information
Tobs_data = session.query(Measurement.tobs).filter(Measurement.station == station_count[0][0])\
.filter(Measurement.date > str(first_date-dt.timedelta(days=1))).all()

#################################################
# Flask Setup
#################################################
Climate_app = Flask(__name__)

#################################################
# Flask Routes
#################################################

@Climate_app.route("/")
def welcome():
    """List all available api routes."""
    return (
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/start_date <br/>"
        f"/api/v1.0/start_date/end_date <br/>"
    )

@Climate_app.route("/api/v1.0/precipitation")
def prcp_result():
    data1 = PRCP_data.copy()
    ID = data1.Station + "_" + data1.Date.astype(str)
    data1.index = ID
    data1.drop(["Station","Date"],axis = 1,inplace = True)
    PRCP_DICT = data1.to_dict()

    return (jsonify(PRCP_DICT))


@Climate_app.route("/api/v1.0/stations")
def stations_result():
    return ( jsonify([ (station_count[x][0] + ":" + str(station_count[x][1])) for x in range(len(station_count))]) )

@Climate_app.route("/api/v1.0/tobs")
def tobs_result():
    return ( jsonify( list(np.ravel(Tobs_data)) ) )

@Climate_app.route("/api/v1.0/<start>")
def calc_temps(start):
    
    session = Session(engine)
    
    start_date = dt.datetime.strptime(start,'%Y-%m-%d').date()
    
    result_temp = session.query(func.min(Measurement.tobs), 
                                func.avg(Measurement.tobs), 
                                func.max(Measurement.tobs)).\
                                filter(Measurement.date >= start_date).\
                                all()
    
    return( jsonify(list(np.ravel(result_temp))) )

@Climate_app.route("/api/v1.0/<start_date>/<end_date>")
def calc_temps_with_end(start_date, end_date):
    
    session = Session(engine)
    
    #start_date = dt.datetime.strptime(start,'%Y-%m-%d').date()
    #end_date = dt.datetime.strptime(end,'%Y-%m-%d').date()
    
    result_temp = session.query(func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)).\
        filter(Measurement.date >= start_date).filter(Measurement.date <= end_date).all()
    
    return( jsonify(list(np.ravel(result_temp))) )

if __name__ == '__main__':
    Climate_app.run(debug=True)
