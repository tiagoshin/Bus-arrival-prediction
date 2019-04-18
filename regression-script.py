$ pyspark --packages org.mongodb.spark:mongo-spark-connector_2.11:2.2.6 --master yarn --queue default --name machine_leaning 

from pymongo import mongoclient
from datetime import datetime
from datetime import timedelta
from math import radians, cos, sin, asin, sqrt
from pyspark.sql.types import structtype, structfield
from pyspark.sql.types import doubletype, integertype, stringtype, datetype, timestamptype
from pyspark.sql import functions
from pyspark.sql.functions import *
from pyspark.sql.window import window
from pyspark.sql import row
from pyspark.mllib.util import mlutils
from pyspark.ml.feature import standardscaler
from pyspark.ml.feature import onehotencoder, stringindexer
from pyspark.mllib.linalg import vectors
from pyspark.ml.feature import vectorassembler
from pyspark.ml import pipeline
from pyspark.ml.regression import randomforestregressor
from pyspark.ml.feature import vectorindexer
from pyspark.ml.evaluation import regressionevaluator


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

def get_features_by_line(linha, latitude_is, longitude_is, latitude_fs, longitude_fs, min_trip):
    df = spark.read.format("com.mongodb.spark.sql.defaultsource").option("uri","mongodb://cassio.cluster.teste.com/test.rio_bus_linha_{}".format(linha)).load()
    df = df.withcolumn("date", to_date("datahora", 'dd-mm-yyyy'))
    df = df.withcolumn("epoch_time", unix_timestamp("datahora", "dd-mm-yyyy hh:mm:ss"))
    df = df.withcolumn("time_0", unix_timestamp("datahora", "dd-mm-yyyy"))
    df = df.withcolumn("time_of_day", df.epoch_time - df.time_0)
    df = df.drop("time_0") 
    df = df.withcolumn("weekday", date_format('date', 'eeee'))
    df = df.withcolumn("date", unix_timestamp(col("date"), "dd-mm-yyyy"))
    df = df.withcolumn("latitude_is", lit(latitude_is))
    df = df.withcolumn("longitude_is", lit(longitude_is))
    df = df.withcolumn("latitude_fs", lit(latitude_fs))
    df = df.withcolumn("longitude_fs", lit(longitude_fs))
    df = df.withcolumn("distance_is",get_distance(df.longitude, df.latitude, df.longitude_is, df.latitude_is))
    df = df.withcolumn("distance_fs",get_distance(df.longitude, df.latitude, df.longitude_fs, df.latitude_fs))
    df = df.orderby(desc("epoch_time"))
    df_counter = df.groupby("ordem").count().withcolumnrenamed("count", "ordem_counter").withcolumnrenamed("ordem", "ordem_id")
    df = df.join(df_counter, df.ordem == df_counter.ordem_id).drop("ordem_id")
    df = df.where(col("ordem_counter") > min_trip).drop("ordem_counter") 
    df = df.select([c for c in df.columns if c not in {'latitude_is', 'longitude_is' ,'latitude_fs' ,'longitude_fs'}]) 
    return df
    
def get_features_by_bus(line_dataframe, bus_id, max_velocity, max_distance, scaler):    
    df = line_dataframe.filter(line_dataframe.ordem.like("%{}%".format(bus_id))) #filter by bus
    #continuous
    assembler = vectorassembler(inputcols=['velocidade', 'time_of_day', 'distance_is', 'distance_fs'], outputcol="continuous_features")
    df = assembler.transform(df)
    scalermodel = scaler.fit(df)
    df = scalermodel.transform(df)
    #terminal creation
    w = window.orderby("epoch_time")
    value_lag = lag('epoch_time').over(w)
    df = df.withcolumn('epoch_lag', value_lag) #create lag - one line below - of epoch_time
    df = df.withcolumn('var_epoch_time', df.epoch_time - df.epoch_lag).fillna(0, subset=['var_epoch_time']) #create variation of epoch_time
    w2 = window.orderby("epoch_time") 
    value_lead = lead('var_epoch_time').over(w2)
    df = df.withcolumn('var_et_lead', value_lead) #create lead - one line above - of variation of epoch time
    new_column = when((col("velocidade") < max_velocity) & (col("distance_is") < max_distance), 1).otherwise(0) #define conditions to create terminal with respect to the initial station
    df = df.withcolumn("terminal", new_column)
    new_column2 = when((col("velocidade") < max_velocity) & (col("distance_fs") < max_distance), 1).otherwise(col("terminal")) #define conditions to create terminal with respect to the final station
    df = df.withcolumn("terminal", new_column2)
    #assembler
    va = vectorassembler(inputcols=["scaledfeatures", "terminal"], outputcol="features")
    df = va.transform(df)
    #select last register
    i = df.select(max("epoch_time")).first()[0]
    df = df.where(col("epoch_time") == i)
    df = df.select([c for c in df.columns if c not in {'epoch_lag','epoch_time','var_epoch_time', 'var_et_lead'}])
    return df
    
def offline_evaluation(linha):
    df = spark.read.format("com.mongodb.spark.sql.defaultsource").option("uri","mongodb://cassio.cluster.teste.com/test.linha{}_history".format(linha)).load()
    df = df.withcolumn("date", unix_timestamp(col("date"), "dd-mm-yyyy"))
    #continuous
    assembler = vectorassembler(inputcols=['velocidade', 'time_of_day', 'distance_is', 'distance_fs', 'latitude', 'longitude'],outputcol="continuous_features")
    df = assembler.transform(df)
    scaler = standardscaler(inputcol="continuous_features", outputcol="scaledfeatures",withstd=true, withmean=false)
    scalermodel = scaler.fit(df)
    df = scalermodel.transform(df)
    #label
    df = df.withcolumnrenamed("delta_time", "label")
    #assembler
    va = vectorassembler(inputcols=["scaledfeatures", "terminal"], outputcol="features")
    df = va.transform(df)
    #split
    (trainingdata, testdata) = df.randomsplit([0.7, 0.3])
    #training
    rf = randomforestregressor(featurescol="features", maxdepth=20, numtrees=20)
    model = rf.fit(trainingdata)
    #test
    predictions = model.transform(testdata)
    #evaluation
    evaluator = regressionevaluator(labelcol="label", predictioncol="prediction", metricname="mae")
    mae = evaluator.evaluate(predictions)
    print("mean absolute error (mae) on test data = %g" % mae)
    return predictions

def train_history(linha):
    df = spark.read.format("com.mongodb.spark.sql.defaultsource").option("uri","mongodb://cassio.cluster.teste.com/test.linha{}_history".format(linha)).load()
    df = df.withcolumn("date", unix_timestamp(col("date"), "dd-mm-yyyy"))
    #continuous
    assembler = vectorassembler(inputcols=['velocidade', 'time_of_day', 'distance_is', 'distance_fs', 'latitude', 'longitude'],outputcol="continuous_features")
    df = assembler.transform(df)
    scaler = standardscaler(inputcol="continuous_features", outputcol="scaledfeatures",withstd=true, withmean=false)
    scalermodel = scaler.fit(df)
    df = scalermodel.transform(df)
    #label
    df = df.withcolumnrenamed("delta_time", "label")
    #assembler
    va = vectorassembler(inputcols=["scaledfeatures", "terminal"], outputcol="features")
    df = va.transform(df)
    rf = randomforestregressor(featurescol="features", maxdepth=20, numtrees=20)
    model = rf.fit(df)
    return model, scaler
    
def get_bus_id(desired_line):
    linha = row("linha", "latitude_is", "longitude_is", "latitude_fs", "longitude_fs")
    linha864 = linha('864', -22.901986, -43.555818, -22.874226, -43.468544)
    linha565 = linha('565', -22.915587, -43.361235, -22.978734, -43.223393)
    linha232 = linha('232', -22.909037, -43.170912, -22.903672, -43.290264)
    linha639 = linha('639', -22.811110, -43.329860, -22.920703, -43.225008)
    if desired_line == 864:
        linha_seq = [linha864]
    elif desired_line == 565:
        linha_seq = [linha565]
    elif desired_line == 232:
        linha_seq = [linha232]
    elif desired_line == 639:
        linha_seq = [linha639]
    else:
        print("o parâmetro desired_line deve conter somente números")
    for i in linha_seq:
        df_linha = get_features_by_line(i[0], i[1], i[2], i[3], i[4], 4)
    ordem_list = df_linha.orderby(desc("epoch_time")).select(['ordem', 'datahora'])
    return ordem_list.show()  #return list of bus_ids
    
def get_pred_time(desired_line, desired_bus, model, scaler):
    linha = row("linha", "latitude_is", "longitude_is", "latitude_fs", "longitude_fs")
    linha864 = linha('864', -22.901986, -43.555818, -22.874226, -43.468544)
    linha565 = linha('565', -22.915587, -43.361235, -22.978734, -43.223393)
    linha232 = linha('232', -22.909037, -43.170912, -22.903672, -43.290264)
    linha639 = linha('639', -22.811110, -43.329860, -22.920703, -43.225008)
    if desired_line == 864:
        linha_seq = [linha864]
    elif desired_line == 565:
        linha_seq = [linha565]
    elif desired_line == 232:
        linha_seq = [linha232]
    elif desired_line == 639:
        linha_seq = [linha639]
    else:
        print("o parâmetro desired_line deve conter somente números")
    for i in linha_seq:
        bus = get_features_by_bus(get_features_by_line(i[0], i[1], i[2], i[3], i[4], 4), 'd86200', 30.0, 1.0, scaler)
    predictions = model.transform(bus)
    predictions.select(['prediction', 'latitude', 'longitude', 'distance_is', 'distance_fs', 'datahora']).show()
    return predictions
    
get_bus_id(864)
model, scaler = train_history(864)
pred = get_pred_time(864, 'd86200', model, scaler)



