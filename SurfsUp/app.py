# Import the dependencies.
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func
from flask import Flask, jsonify
import datetime as dt

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
session = Session(engine)

#################################################
# Flask Setup
#################################################
app = Flask(__name__)


#################################################
# Flask Routes
#################################################


# helper function to parse the date and prcp output of `.all()`
def _date_prcp_map(query_obj):
    return {x.date: x.prcp for x in query_obj}


# helper function to parse the date and tobs output of `.all()`
def _date_tobs_map(query_obj):
    return {x.date: x.tobs for x in query_obj}


# the date one year prior to the last day in the dataset
LAST_YEAR_DATE = dt.date(2017, 8, 23) - dt.timedelta(days=365)

# Query object for quick access to the measurement data
measurement_query = session.query(measurement)

# Query object for efficient access to the min, max, and avg tobs
tobs_stats_query = session.query(
        func.min(measurement.tobs), func.max(measurement.tobs), func.avg(measurement.tobs))


@app.route("/")
def index():
    return ("Welcome to the Climate API! <br>"
            "Available Routes: <br>"
            "/api/v1/precipitation <br>"
            "/api/v1/precipitation_last_year <br>"
            "/api/v1/stations <br>"
            "/api/v1/tobs_most_active <br>"
            "/api/v1/tobs_most_active_last_year <br>"
            "/api/v1.0/temperature_stats/<date> <br>"
            "/api/v1.0/temperature_stats/<start_date>/<end_date> <br>"
            )


@app.route("/api/v1/precipitation")
def precipitation():
    """
    Returns json with the date as the key and the value as the precipitation
    """
    query_res = measurement_query.all()
    return jsonify(_date_prcp_map(query_res))


@app.route("/api/v1/precipitation_last_year")
def precipitation_last_year():
    """
    Returns the json-ified precipitation data for the last year in the database
    """
    query_res = measurement_query.filter(measurement.date >= LAST_YEAR_DATE).all()
    return jsonify(_date_prcp_map(query_res))


@app.route("/api/v1/stations")
def stations():
    """
    Returns json-ified data of all the stations in the database
    """
    unique_stations = [x.station for x in session.query(station).distinct().all()]
    return jsonify(unique_stations)


@app.route("/api/v1/tobs_most_active")
def tobs_most_active():
    """
    Returns json-ified data for the most active station (USC00519281)
    """
    query_res = measurement_query.filter(measurement.station == 'USC00519281').all()
    return jsonify(_date_tobs_map(query_res))


@app.route("/api/v1/tobs_most_active_last_year")
def tobs_most_active_last_year():
    """
    Only returns the json-ified data for the last year of data
    """
    query_res = measurement_query.filter(measurement.date >= LAST_YEAR_DATE).all()
    return jsonify(_date_tobs_map(query_res))


@app.route("/api/v1.0/temperature_stats/<date>")
def temperature_stats_from(date):
    """
    Returns the min, max, and average temperatures calculated from the given start `date` to the end of the dataset.

    :param date: str with the form YYYY-MM-DD
    """
    min_, max_, avg_ = tobs_stats_query.filter(measurement.date >= date) \
        .all().pop()
    return jsonify({'min_temp': min_, 'max_temp': max_, 'avg_temp': avg_})


@app.route("/api/v1.0/temperature_stats/<start_date>/<end_date>")
def temperature_stats_between(start_date, end_date):
    """
    Returns the min, max, and average temperatures calculated from the given start date to the given end date

    :param start_date: str with the form YYYY-MM-DD
    :param end_date: str with the form YYYY-MM-DD
    """
    min_, max_, avg_ = tobs_stats_query.filter(measurement.date >= start_date).filter(measurement.date <= end_date) \
        .all().pop()
    return jsonify({'min_temp': min_, 'max_temp': max_, 'avg_temp': avg_})


if __name__ == '__main__':
    app.run(debug=False)
