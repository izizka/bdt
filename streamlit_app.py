import streamlit as st
import pandas as pd
import numpy as np
import datetime


st.title('Public Transport Data')
st.write("""Prague offers information about the movements of the public transport vehicles via its data
         platform Golemio ( https://golemio.cz/data ). Data are collected and pushed with Kafka topics (hosted on AWS). 
         Data are preprocessed and divided into 5 topics.

        TOPICS:
        * Trains
        * Buses
        * RegBuses
        * Trams
        * Boats
        """)
st.write("Streams are processed with DataBricks and data are stored directly to HIVE table. Main process is done with SQL and Spark.")
st.write("""I would divide Kafka topics into three groups:""")
st.write("  1) where car_id is not null (Trams + Buses + RegBuses)")
st.write("  2) where car_id is null (Trains)")
st.write("  3) low number of records (Boats)")

st.header("Number of vehicles operating in each hour of given date")
st.write("""The main goal is to find number of unique car_ids in specific hour. As I said there are three logic groups of vehicles. 
For groups number 1 and 3 the approach is fairly straightforward - just group vehicles by time and car_id and count them.
Last group is more complicated, there is no vehicle identifier. 
It makes sense that trains do not have specific ID because wagons can be dropped or added between lines.
So I changed the definition of uniques - I counted number of unique train's trip_ids (unique within hour).
""")
st.subheader("Select Date")
date_selected = st.date_input(
    " ",
    value=datetime.date(2022, 12, 20),
    min_value=datetime.date(2022, 12, 18),
    max_value=datetime.date(2022, 12, 23))
day_selected = date_selected.day


st.subheader("Buses")
df_buses = pd.read_csv("csvs/count_buses.csv", header=0)
filter_df = df_buses.loc[df_buses["day_number"] == day_selected]
filter_df = filter_df[["hour_number", "number_of_vehicles"]]
st.bar_chart(filter_df, x='hour_number', y='number_of_vehicles')

st.subheader("RegBuses")
df_buses = pd.read_csv("csvs/count_regbuses.csv", header=0)
filter_df = df_buses.loc[df_buses["day_number"] == day_selected]
filter_df = filter_df[["hour_number", "number_of_vehicles"]]
st.bar_chart(filter_df, x='hour_number', y='number_of_vehicles')

st.subheader("Trams")
df_buses = pd.read_csv("csvs/count_trams.csv", header=0)
filter_df = df_buses.loc[df_buses["day_number"] == day_selected]
filter_df = filter_df[["hour_number", "number_of_vehicles"]]
st.bar_chart(filter_df, x='hour_number', y='number_of_vehicles')

st.subheader("Boats")
df_buses = pd.read_csv("csvs/count_boats.csv", header=0)
filter_df = df_buses.loc[df_buses["day_number"] == day_selected]
filter_df = filter_df[["hour_number", "number_of_vehicles"]]
st.bar_chart(filter_df, x='hour_number', y='number_of_vehicles')

st.subheader("Trains")
df_buses = pd.read_csv("csvs/count_trains.csv", header=0)
filter_df = df_buses.loc[df_buses["day_number"] == day_selected]
filter_df = filter_df[["hour_number", "number_of_vehicles"]]
st.bar_chart(filter_df, x='hour_number', y='number_of_vehicles')


st.header("Average speed of vehicles in each hour of given date")
