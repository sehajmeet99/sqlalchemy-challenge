
import numpy as np
import datetime as dt
from flask import Flask, jsonify

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func

#################################################
# Database Setup
#################################################

engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()

# reflect the tables
Base.prepare(autoload_with=engine)

# Save references to each table
measurement = Base.classes.measurement
station = Base.classes.station

# Create our session (link) from Python to the DB
#################################################
# Flask Setup
#################################################
app = Flask(__name__)

#################################################
# Flask Routes
#################################################

@app.route("/")
def welcome():
    """List all available api routes."""
    return (
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/[start date in YYYY-MM-DD fortmat]<br/>"
        f"/api/v1.0/[start date in YYYY-MM-DD fortmat]/[end date in YYYY-MM-DD fortmat]<br/>"
    )

@app.route("/api/v1.0/precipitation")
def precipitation():
    
     # Create our session (link) from Python to the DB
    session = Session(engine)
    # Convert the query results to a Dictionary using date as the key and prcp as the value.
    # Perform a query to retrieve the data and precipitation scores

    msmt_test = session.query(Measurement).\
            order_by(Measurement.date.desc())\
            .first()
    last_test = pd.to_datetime(msmt_test.date)
    first_test = last_test - timedelta(days=365)
    first_date = dt.date(first_test.year, first_test.month, first_test.day)
    last_date = dt.date(last_test.year, last_test.month, last_test.day)

    msmt_year = session.query(Measurement.date,Measurement.prcp).\
        filter(Measurement.date >= first_date).\
        order_by(Measurement.date.asc()).\
        all()
    session.close()

    precip_data = []
    for date, prcp in msmt_year:
        precip_dict = {date:prcp}
        precip_data.append(precip_dict)
    d = defaultdict(list)
    for date,prcp in msmt_year:
        d[date].append(prcp)
    precip_dict_defaultdict = dict(d)

    # Return the JSON representation of your dictionary.
    return jsonify(precip_dict_defaultdict) #****** has null values ******


@app.route("/api/v1.0/stations")
def stations():
    # Create our session (link) from Python to the DB
    session = Session(engine)
    # Return a JSON list of stations from the dataset.
    station_count = session.query(Measurement.station).group_by(Measurement.station).order_by(func.count(Measurement.station).desc()).all()#.distinct(Station.station).count()#group_by(Station.station).all()
    station_list = list(np.ravel(station_count))
    session.close()
    return(jsonify(station_list))

@app.route('/api/v1.0/tobs')
def tobs():
       # Create our session (link) from Python to the DB
    session = Session(engine)
    
    # Find the most recent date in the data set.
    date_recent = session.query(Measurement.date).order_by(Measurement.date.desc()).first()[0]
    
    # Calculate the date one year from the last date in data set.
    d = dt.datetime.strptime(date_recent, "%Y-%m-%d")
    last_date = d.date() - relativedelta(years=1)

    # Perform a query to retrieve the data for station USC00519281 and its temperature for last 12 months
    data = session.query(Measurement.date, Measurement.tobs).\
                    filter(Measurement.station == 'USC00519281').\
                    filter(func.strftime("%Y-%m-%d", Measurement.date) >= last_date).\
                    all()
    
    # Store the temperature data in an empty list with every row as dictionary
    temperature_data = []
    for date, tobs in data:
        temp_data = {}
        temp_data[date] = tobs
        temperature_data.append(temp_data)
    
    # Close Session
    session.close()
    
    return jsonify(temperature_data)
@app.route('/api/v1.0/<start>')
@app.route('/api/v1.0/<start>/<end>')
def dateRange(start,end='2017-08-23'):
    # Create our session (link) from Python to the DB
    session = Session(engine)

    time_btw = pd.to_datetime(end) - pd.to_datetime(start)

    test_range = pd.Series(pd.date_range(start,periods=time_btw.days+1,freq='D'))
    date_list = []
    for trip in test_range:
        date_list.append(trip.strftime('%Y-%m-%d'))
    def daily_normals(start_date):
        '''Daily Normals. 
        Args:
            date (str): A date string in the format '%Y-%m-%d'
            
        Returns:
            A list of tuples containing the daily normals, tmin, tavg, and tmax
        
        '''
    
        sel = [Measurement.date, func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)]
        norms = session.query(*sel).filter(func.strftime("%Y-%m-%d", Measurement.date) == start_date).all()
        return(norms)

    normals= []
    for date in date_list:
        normals.append(daily_normals(date))
    # Return a JSON list of the minimum temperature, the average temperature, and the max temperature for a given start or start-end range.
    session.close()
    return jsonify(normals)





if __name__ == '__main__':
    app.run(debug=True)