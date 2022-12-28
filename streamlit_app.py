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
    "Number of vehicles selector",
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

st.write("""As you can see there is a gap in data on 19th of December between 2 and 5 AM. I did not find any reason for that. 
I assume that stream was down or the gap is linked with changes from weekend schedules to workday schedules.""")
st.write("By information provided by DPP (public transport company), they own 802 trams, 1216 buses. There are around 35 tram lines, 143 bus lines.")


st.header("Average speed of vehicles in each hour of given date")
st.write("""This task is more complex than the previous one. We have two types of approaches to solve it.""")
st.write("""First one is to use information sent by vehicle about current speed. There are two problems with this approach. First - data are not send with same period. 
Second - Trains, Trams do not sent this type of information (always null) on the other hand boats send current speed which is equal to zero (waiting time on the bank of river).
Second issue can be avoided by computing the average speed on given line within an hour. The average speed for all vehicles within an hour is just 
average of the average line speeds. This way of solving counts with the randomness in the period when the message is sent. Let's say that there is a uniform
random distribution of place where the message is sent. So it means that the probability of sending message at a stop is the same as probability of sending message in part of the road between stops.""")
st.write("""Second attitude is easier and more stable/consistent. The main idea is to sum traveled distances for each car within an hour and compute average distance traveled.
The time window is equal to one hour so average traveled distance is also the average speed for vehicles. I can use similar attitude as in the first approach where the current speed is missing.
Instead of computing an average speed I would use just distance and time would be one hour.""")

st.write("""In the following section you can find average speeds computed with both approaches (first attitude = first figure | second attitude = second figure)""")
st.write("Note that there are missing data on the 19th of December between 2 and 5 AM (same problem as with counting of vehicles).")

st.subheader("Select Date")
date_selected_avg = st.date_input(
    "AVG speed selector",
    value=datetime.date(2022, 12, 20),
    min_value=datetime.date(2022, 12, 18),
    max_value=datetime.date(2022, 12, 23))
date_selected_avg = date_selected_avg.day

st.subheader("Buses")
df_buses = pd.read_csv("csvs/avg_random_buses.csv", header=0)
filter_df = df_buses.loc[df_buses["day_number"] == date_selected_avg]
filter_df = filter_df[["hour_number", "avg_speed"]]
st.bar_chart(filter_df, x='hour_number', y='avg_speed')

df_buses = pd.read_csv("csvs/avg_buses.csv", header=0)
filter_df = df_buses.loc[df_buses["day_number"] == date_selected_avg]
filter_df = filter_df[["hour_number", "avg_speed"]]
st.bar_chart(filter_df, x='hour_number', y='avg_speed')


st.subheader("RegBuses")
df_regbuses = pd.read_csv("csvs/avg_random_regbuses.csv", header=0)
filter_df = df_regbuses.loc[df_regbuses["day_number"] == date_selected_avg]
filter_df = filter_df[["hour_number", "avg_speed"]]
st.bar_chart(filter_df, x='hour_number', y='avg_speed')

df_regbuses = pd.read_csv("csvs/avg_regbuses.csv", header=0)
filter_df = df_regbuses.loc[df_regbuses["day_number"] == date_selected_avg]
filter_df = filter_df[["hour_number", "avg_speed"]]
st.bar_chart(filter_df, x='hour_number', y='avg_speed')

st.subheader("Trams")
df_trams = pd.read_csv("csvs/avg_random_trams.csv", header=0)
filter_df = df_trams.loc[df_trams["day_number"] == date_selected_avg]
filter_df = filter_df[["hour_number", "avg_speed"]]
st.bar_chart(filter_df, x='hour_number', y='avg_speed')

df_trams = pd.read_csv("csvs/avg_trams.csv", header=0)
filter_df = df_trams.loc[df_trams["day_number"] == date_selected_avg]
filter_df = filter_df[["hour_number", "avg_speed"]]
st.bar_chart(filter_df, x='hour_number', y='avg_speed')

st.subheader("Boats")
df_boats = pd.read_csv("csvs/avg_random_boats.csv", header=0)
filter_df = df_boats.loc[df_boats["day_number"] == date_selected_avg]
filter_df = filter_df[["hour_number", "avg_speed"]]
st.bar_chart(filter_df, x='hour_number', y='avg_speed')

df_boats = pd.read_csv("csvs/avg_boats.csv", header=0)
filter_df = df_boats.loc[df_boats["day_number"] == date_selected_avg]
filter_df = filter_df[["hour_number", "avg_speed"]]
st.bar_chart(filter_df, x='hour_number', y='avg_speed')
st.write("The main problem with boats is that there are not enough data. Also there is not a huge difference between the approaches.")

st.subheader("Trains")

df_trains = pd.read_csv("csvs/avg_random_trains.csv", header=0)
filter_df = df_trains.loc[df_trains["day_number"] == date_selected_avg]
filter_df = filter_df[["hour_number", "avg_speed"]]
st.bar_chart(filter_df, x='hour_number', y='avg_speed')

df_trains = pd.read_csv("csvs/avg_trains.csv", header=0)
filter_df = df_trains.loc[df_trains["day_number"] == date_selected_avg]
filter_df = filter_df[["hour_number", "avg_speed"]]
st.bar_chart(filter_df, x='hour_number', y='avg_speed')
st.write("""Here is the biggest difference between the approaches. I double checked the results and I did not find any mistake.
    I would say that the second figure is more accurate. The first attitude does not work in this case because of the speed between stops. 
    Train is really fast if it is not at a stop. The speed limit is around 160 km/h in some cases 120 km/h. We can assume that the speed limit 
    in Prague's lines is lower. On the other hand, it is still very high compared to other vehicles. I believe that it is the reason behind the
    difference. 
""")

st.header("Conclusion")
st.subheader("Number of operating vehicles")
st.write("""
    As you can see, there is a huge difference in the number of operating vehicles between weekdays (18.12.2022) and workdays (19.12 - 23.12.2022). 
    Workdays have two main peaks. The first one is at 7 AM (people travel to work/school), and the second peak is in the afternoon.
    In the second case, the maximum amount of vehicles depends on vehicle type. Graphs show that the busy period is between 3 PM and 6 PM.
    Weekday has a constant number of vehicles within a day. Peeks are insignificant.
""")
st.subheader("Average speed vehicles")
st.write("""In this case, I provided two different approaches and compared them. 
            As I said, there is a vast difference between methods in some cases. From my point of view, the second method is more accurate.
            I have already discussed reasons for it in the sections above.
""")
st.write("""Last but not least, I would like to talk about average speed peeks. Similar to the previous task, there is a significant difference between 
        weekends and other days.
        Weekday is usually without heavy traffic, and the average speed is consistent. On the other hand, workdays are more complicated.
        The highest average speed is in the evening or at night. It could be coursed by traffic and the number of traveling people. More 
        people means longer waiting time at stops -> lower average speed. 
""")
