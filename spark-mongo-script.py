"""
$ pyspark --packages org.mongodb.spark:mongo-spark-connector_2.11:2.2.6 --master yarn --queue default --name machine_leaning 
"""

from pymongo import mongoclient
from datetime import datetime
from math import radians, cos, sin, asin, sqrt
from pyspark.sql.types import structtype, structfield
from pyspark.sql.types import doubletype, integertype, stringtype, datetype, timestamptype
from pyspark.sql import functions
from pyspark.sql.functions import *
from pyspark.sql.window import window
from pyspark.sql import row

def get_distance(longit_a, latit_a, longit_b, latit_b):\
    # transform to radians\
    longit_a, latit_a, longit_b, latit_b = map(radians, [longit_a,  latit_a, longit_b, latit_b]);\
    dist_longit = longit_b - longit_a;\
    dist_latit = latit_b - latit_a;\
    # calculate area\
    area = sin(dist_latit/2)**2 + cos(latit_a) * sin(dist_longit/2)**2;\
    # calculate the central angle\
    central_angle = 2 * asin(sqrt(area));\
    radius = 6371;\
    # calculate distance\
    distance = central_angle * radius;\
    return distance
   
def makehistory_by_line(linha, latitude_is, longitude_is, latitude_fs, longitude_fs, min_bus=4):
    df = spark.read.format("com.mongodb.spark.sql.defaultsource").option("uri","mongodb://cassio.cluster.teste.com/test.rio_bus_linha_{}".format(linha)).load()
    df = df.withcolumn("date", to_date("datahora", 'dd-mm-yyyy'))
    df = df.withcolumn("epoch_time", unix_timestamp("datahora", "dd-mm-yyyy hh:mm:ss"))
    df = df.withcolumn("time_0", unix_timestamp("datahora", "dd-mm-yyyy"))
    df = df.withcolumn("time_of_day", df.epoch_time - df.time_0)
    df = df.drop("time_0")
    df = df.withcolumn("weekday", date_format('date', 'eeee'))
    df = df.withcolumn("latitude_is", lit(latitude_is))
    df = df.withcolumn("longitude_is", lit(longitude_is))
    df = df.withcolumn("latitude_fs", lit(latitude_fs))
    df = df.withcolumn("longitude_fs", lit(longitude_fs))
    df = df.withcolumn("distance_is",get_distance(df.longitude, df.latitude, df.longitude_is, df.latitude_is))
    df = df.withcolumn("distance_fs",get_distance(df.longitude, df.latitude, df.longitude_fs, df.latitude_fs))
    df = df.orderby(asc("epoch_time"))
    df_counter = df.groupby("ordem").count().withcolumnrenamed("count", "ordem_counter").withcolumnrenamed("ordem", "ordem_id")
    df = df.join(df_counter, df.ordem == df_counter.ordem_id).drop("ordem_id")
    df = df.where(col("ordem_counter") > min_bus).drop("ordem_counter")
    return df

def makehistory_by_bus(line_dataframe, bus_id, max_velocity, max_distance, max_var_time, min_trip, collection):
    df = line_dataframe.filter(line_dataframe.ordem.like("%{}%".format(bus_id))) #filter by bus
    #epoch_lag - create lag - one line below - of epoch_time
    w = window.orderby("epoch_time") 
    value_lag = lag('epoch_time').over(w)
    df = df.withcolumn('epoch_lag', value_lag) 
    #var_epoch_time - create variation of epoch_time
    df = df.withcolumn('var_epoch_time', df.epoch_time - df.epoch_lag).fillna(0, subset=['var_epoch_time']) 
    #var_et_lead - create lead - one line above - of variation of epoch time
    w2 = window.orderby("epoch_time") 
    value_lead = lead('var_epoch_time').over(w2)
    df = df.withcolumn('var_et_lead', value_lead) 
    #terminal with respect to distance_is - define conditions to create terminal with respect to the initial station
    new_column = when((col("velocidade") < max_velocity) & (col("distance_is") < max_distance), 1).otherwise(0) 
    df = df.withcolumn("terminal", new_column)
    #terminal with respect to distance_fs - define conditions to create terminal with respect to the final station
    new_column2 = when((col("velocidade") < max_velocity) & (col("distance_fs") < max_distance), 1).otherwise(col("terminal")) 
    df = df.withcolumn("terminal", new_column2)
    #arrive_out_terminal - define conditions which terminal doesn't make sense and drop those rows that doesn't fit inside a trip
    df = df.withcolumn("arrive_out_terminal", when((col("var_epoch_time") > max_var_time) & (col("terminal") == 0), 1).otherwise(0)) 
    df = df.where(col("arrive_out_terminal") == 0)
    #terminal with respect to var_et_lead - if it's passed too much time -30 min- between the last register, then assume this is a new trip
    new_column3 = when((col("var_et_lead") > max_var_time), 1).otherwise(col("terminal"))
    df = df.withcolumn("terminal", new_column3).fillna(0, subset=['var_et_lead'])
    #terminal_lag - create lag of terminal
    w3 = window.orderby("epoch_time")
    value_lag = lag('terminal').over(w3)
    df = df.withcolumn('terminal_lag', value_lag).fillna(0, subset=['terminal_lag'])
    #trip - create trip id
    w4 = (window.orderby('epoch_time').rangebetween(window.unboundedpreceding, 0))
    df = df.withcolumn('trip', sum('terminal_lag').over(w4))
    #trip_counter - create trip counter and drop all the trips that have too little registers
    trip_counter = df.groupby('trip').count().withcolumnrenamed("count", "trip_counter").withcolumnrenamed("trip", "trip_id")
    df = df.join(trip_counter, df.trip == trip_counter.trip_id).drop("trip_id") 
    df = df.where(col("trip_counter") > min_trip)
    #et_next_terminal - create time to next terminal
    w5 = window.partitionby("trip")
    df = df.withcolumn("et_next_terminal", max("epoch_time").over(w5))
    #delta_time - create the dependent variable
    df = df.withcolumn("delta_time", col("et_next_terminal") - col("epoch_time")) 
    #drop rows 
    df = df.select([c for c in df.columns if c not in {'epoch_lag', 'var_epoch_time', 'var_et_lead', 'arrive_out_terminal', 'terminal_lag', 'trip', 'trip_counter', 'et_next_terminal', 'datahora', 'epoch_time', 'latitude_is', 'longitude_is' ,'latitude_fs' ,'longitude_fs'}])
    #write to mongo
    df.write.format("com.mongodb.spark.sql.defaultsource")\
    .mode("append").option("uri","mongodb://cassio.cluster.teste.com")\
    .option("database","test").option("collection", "{}".format(collection)).save()
    return df
    
def run_funcs(linha, latitude_is, longitude_is, latitude_fs, longitude_fs):
    df_linha = makehistory_by_line(linha, latitude_is, longitude_is, latitude_fs, longitude_fs, 4)
    ordem_list = df_linha.groupby("ordem").count().select("ordem")
    for i in ordem_list.rdd.collect():
        makehistory_by_bus(df_linha, i[0], 30.0, 1.0, 1800, 2, "linha{}_history".format(linha))

# this has to be automated by airflow. each run is a workflow with its own parameters
def makehistory():
    linha = row("linha", "latitude_is", "longitude_is", "latitude_fs", "longitude_fs")
    linha864 = linha('864', -22.901986, -43.555818, -22.874226, -43.468544)
    linha565 = linha('565', -22.915587, -43.361235, -22.978734, -43.223393)
    linha232 = linha('232', -22.909037, -43.170912, -22.903672, -43.290264)
    linha639 = linha('639', -22.811110, -43.329860, -22.920703, -43.225008)
    linha_seq = [linha864, linha565, linha232, linha639]
    for i in linha_seq:
        run_funcs(i[0], i[1], i[2], i[3], i[4])
        
makehistory()
