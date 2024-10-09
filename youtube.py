from googleapiclient.discovery import build
import pymongo
import psycopg2
import pandas as pd
import plotly.express as px
import streamlit as st

#API Key Connection

def api_connect():
    api_id = "AIzaSyBqcURxBnQH68xAvzwbLpo44KbE0uXEFpA"
    api_service_name = "youtube"
    api_version = "v3"

    youtube = build(api_service_name, api_version, developerKey=api_id)

    return youtube

youtube = api_connect()

#Get Channel Details
def get_channel_info(channel_id):
    request=youtube.channels().list(
                    part = 'snippet,contentDetails,statistics,status',
                    id = channel_id
    )
    response = request.execute()

    for i in response['items']:
        data=dict(
                Channel_Id = i["id"],
                Channel_Name=i["snippet"]["title"],
                Channel_Type=i["kind"],
                Subscription_Count = i["statistics"]["subscriberCount"],
                Channel_Views = i["statistics"]["viewCount"],
                Channel_Description = i["snippet"]["description"],
                Channel_Status=i["status"]["privacyStatus"]
                )
    return data


#get Video ids
def get_video_ids(channel_id):
    video_ids = []
    response=youtube.channels().list(
                    id = channel_id,
                    part = 'contentDetails'
    ).execute()
    
    Playlist_Id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token= None

    while True:
        response1 = youtube.playlistItems().list(
                            part ='snippet',
                            playlistId=Playlist_Id,
                            maxResults=50,
                            pageToken=next_page_token).execute()

        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token=response1.get('nextPageToken')

        if next_page_token is None:
            break
    return video_ids


#get video details
def get_video_details(video_ids):
    video_data=[]
    for video_id in video_ids:
        request=youtube.videos().list(
            part="snippet, contentDetails, statistics",
            id=video_id
        )

        response2=request.execute()

        for item in response2['items']:
            data=dict(
                Channel_Name = item['snippet']['channelTitle'],
                Channel_Id=item['snippet']['channelId'],
                Video_Id=item['id'],
                Title=item['snippet']['title'],
                Tags=item['snippet'].get('tags'),
                Thumbnail=item['snippet']['thumbnails']['default']['url'],
                Description=item['snippet'].get('description'),
                Published_Date=item['snippet']['publishedAt'],
                Duration=item['contentDetails']['duration'],
                View_Count=item['statistics'].get('viewCount'),
                Like_Count=item['statistics'].get('likeCount'),
                Dislike_Count=item.get('dislikeCount'),
                Favourite_Count=item['statistics']['favoriteCount'],
                Comment_Count=item['statistics']['commentCount'],
                Caption_Status=item['contentDetails']['caption']
            )
            video_data.append(data)
    return video_data


#get comment details
def get_comment_details(video_ids):
    Comment_Details=[]
    try:
        for video_id in video_ids:
            request=youtube.commentThreads().list(
                part='snippet',
                videoId=video_id,
                maxResults=50)

            response = request.execute()

            for item in response['items']:
                data=dict(
                    Comment_Id=item['snippet']['topLevelComment']['id'],
                    Video_Id=item['snippet']['topLevelComment']['snippet']['videoId'],
                    Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                    Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    Comment_Published_Date=item['snippet']['topLevelComment']['snippet']['publishedAt']
                )

                Comment_Details.append(data)

    except:
        pass
    return Comment_Details


#get playlist details
def get_playlist_details(channel_id):
    playlist_data=[]
    next_page_token= None

    while True:

        request=youtube.playlists().list(
            part='snippet,contentDetails',
            channelId=channel_id,
            maxResults=50,
            pageToken=next_page_token
        )

        response=request.execute()

        for item in response['items']:
            data=dict(
                Playlist_Id=item['id'],
                Channel_Id=item['snippet']['channelId'],
                Playlist_Name=item['snippet']['title'],
                Playlist_Video_Count=item['contentDetails']['itemCount']
            )

            playlist_data.append(data)
        
        next_page_token=response.get('nextPageToken')

        if next_page_token is None:
            break

    return playlist_data


#Connect to MongoDB

client=pymongo.MongoClient("mongodb+srv://percikalayola:S45Agy27X0nx39Cz@cluster0.geqvh.mongodb.net/?retryWrites=true&w=majority")
db=client["Youtube_DataHarvest"]


#Upload to MongoDB
def channel_details(channel_id):
    ch_details=get_channel_info(channel_id)
    pl_details=get_playlist_details(channel_id)
    vi_ids=get_video_ids(channel_id)
    vi_details=get_video_details(vi_ids)
    com_details=get_comment_details(vi_ids)

    coll1=db["channel_details"]
    coll1.insert_one({"channel_information":ch_details,"playlist_information":pl_details,
                      "video_information":vi_details,
                      "comment_information":com_details})

    
    return "Upload Completed Successfully"

#Channel Table Creation and Insert in PostgreSQL
def channel_creation(one_channel_name):
    mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="PostPer@24",
                        database="youtube_datawarehouse",
                        port="5432")

    cursor=mydb.cursor()

    create_query='''create table if not exists channels(Channel_Id varchar(100) primary key,
    Channel_Name varchar(1000),
    Channel_Type varchar(500),
    Subscription_Count bigint,
    Channel_Views bigint,
    Channel_Description text,
    Channel_Status varchar(100))'''

    cursor.execute(create_query)
    mydb.commit()

    single_channel_detail=[]
    db=client["Youtube_DataHarvest"]
    coll1=db["channel_details"]

    for ch_data in coll1.find({"channel_information.Channel_Name":one_channel_name},{"_id":0}):
        single_channel_detail.append(ch_data["channel_information"])
    df_single_channel_detail=pd.DataFrame(single_channel_detail)

    for index, row in df_single_channel_detail.iterrows():
        insert_query='''insert into channels(Channel_Id,
        Channel_Name,
        Channel_Type,
        Subscription_Count,
        Channel_Views,
        Channel_Description,
        Channel_Status)
        values(%s,%s,%s,%s,%s,%s,%s)'''
        values=(row['Channel_Id'],
                row['Channel_Name'],
                row['Channel_Type'],
                row['Subscription_Count'],
                row['Channel_Views'],
                row['Channel_Description'],
                row['Channel_Status'])
        
        try:
            cursor.execute(insert_query,values)
            mydb.commit()
        except:
            msg = f"Your provided Channel Name {one_channel_name} already exists"
            return msg
    

#Playlist Table Creation and Insert in PostgreSQL
def playlist_creation(one_channel_name):
    mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="PostPer@24",
                        database="youtube_datawarehouse",
                        port="5432")

    cursor=mydb.cursor()

    create_query='''create table if not exists playlists(Playlist_Id varchar(100) primary key,
    Channel_Id varchar(100),
    Playlist_Name varchar(1000),
    Playlist_Video_Count bigint)'''

    cursor.execute(create_query)
    mydb.commit()


    single_playlist_detail=[]
    db=client["Youtube_DataHarvest"]
    coll1=db["channel_details"]

    for ch_data in coll1.find({"channel_information.Channel_Name":one_channel_name},{"_id":0}):
        single_playlist_detail.append(ch_data["playlist_information"])
    df_single_playlist_detail=pd.DataFrame(single_playlist_detail[0])

    for index, row in df_single_playlist_detail.iterrows():
        insert_query='''insert into playlists(Playlist_Id,
        Channel_Id,
        Playlist_Name,
        Playlist_Video_Count)
        
        values(%s,%s,%s,%s)'''
        values=(row['Playlist_Id'],
                row['Channel_Id'],
                row['Playlist_Name'],
                row['Playlist_Video_Count'])
        
        try:
            cursor.execute(insert_query,values)
            mydb.commit()
        except:
            print('Playlist values are already inserted')


#Video Table Creation and Insert in PostgreSQL
def video_creation(one_channel_name):
    mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="PostPer@24",
                        database="youtube_datawarehouse",
                        port="5432")

    cursor=mydb.cursor()

    create_query='''create table if not exists videos(
        Channel_Name varchar(1000),
                    Channel_Id varchar(100),
                    Video_Id varchar(50) primary key,
                    Title varchar(1000),
                    Tags text,
                    Thumbnail varchar(500),
                    Description text,
                    Published_Date timestamp,
                    Duration interval,
                    View_Count bigint,
                    Like_Count bigint,
                    Dislike_Count bigint,
                    Favourite_Count int,
                    Comment_Count int,
                    Caption_Status varchar(50)
    )'''

    cursor.execute(create_query)
    mydb.commit()

    single_video_detail=[]
    db=client["Youtube_DataHarvest"]
    coll1=db["channel_details"]

    for ch_data in coll1.find({"channel_information.Channel_Name":one_channel_name},{"_id":0}):
        single_video_detail.append(ch_data["video_information"])
    df_single_video_detail=pd.DataFrame(single_video_detail[0])

    for index, row in df_single_video_detail.iterrows():
        insert_query='''insert into videos(
                    Channel_Name,
                    Channel_Id,
                    Video_Id,
                    Title,
                    Tags,
                    Thumbnail,
                    Description,
                    Published_Date,
                    Duration,
                    View_Count,
                    Like_Count,
                    Dislike_Count,
                    Favourite_Count,
                    Comment_Count,
                    Caption_Status)

        values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        values=(row['Channel_Name'],
                row['Channel_Id'],
                row['Video_Id'],
                row['Title'],
                row['Tags'],
                row['Thumbnail'],
                row['Description'],
                row['Published_Date'],
                row['Duration'],
                row['View_Count'],
                row['Like_Count'],
                row['Dislike_Count'],
                row['Favourite_Count'],
                row['Comment_Count'],
                row['Caption_Status'])

        try:
            cursor.execute(insert_query,values)
            mydb.commit()
        except:
            print('Video values are already inserted')


#Comment table creation and insert in PostgreSql
def comments_creation(one_channel_name):
    mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="PostPer@24",
                        database="youtube_datawarehouse",
                        port="5432")

    cursor=mydb.cursor()

    create_query='''create table if not exists comments(
    Comment_Id varchar(100) primary key,
    Video_Id varchar(100),
    Comment_Text text,
    Comment_Author varchar(200),
    Comment_Published_Date timestamp)'''

    cursor.execute(create_query)
    mydb.commit()

    single_comment_detail=[]
    db=client["Youtube_DataHarvest"]
    coll1=db["channel_details"]

    for ch_data in coll1.find({"channel_information.Channel_Name":one_channel_name},{"_id":0}):
        single_comment_detail.append(ch_data["comment_information"])
    df_single_comment_detail=pd.DataFrame(single_comment_detail[0])

    for index, row in df_single_comment_detail.iterrows():
        insert_query='''insert into comments(
                    Comment_Id,
                    Video_Id,
                    Comment_Text,
                    Comment_Author,
                    Comment_Published_Date)
        values(%s,%s,%s,%s,%s)'''
        values=(row['Comment_Id'],
                row['Video_Id'],
                row['Comment_Text'],
                row['Comment_Author'],
                row['Comment_Published_Date'])
        try:
            cursor.execute(insert_query,values)
            mydb.commit()
        except:
            print('Comments are already inserted')


#Insert into PostgreSQL Tables
def postgresql_table_creation(one_channel):
    msg= channel_creation(one_channel)

    if msg:
        return msg
    
    else:
        playlist_creation(one_channel)
        video_creation(one_channel)
        comments_creation(one_channel)

        return "Tables Created Successfully for Channel, Playlist, Video and Comments"

#Channel Details for Streamlit
def show_channels_table():
    ch_list=[]
    db=client["Youtube_DataHarvest"]
    coll1=db["channel_details"]

    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])

    df=st.dataframe(ch_list)

    return df

#Playlist Details for Streamlit
def show_playlist_table():
    pl_list=[]
    db=client["Youtube_DataHarvest"]
    coll1=db["channel_details"]

    for pl_data in coll1.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(pl_data["playlist_information"])):
            pl_list.append(pl_data["playlist_information"][i])

    df1=st.dataframe(pl_list)

    return df1

#Video Details for Streamlit
def show_video_table():
    vi_list=[]
    db=client["Youtube_DataHarvest"]
    coll1=db["channel_details"]

    for vi_data in coll1.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])

    df2=st.dataframe(vi_list)

    return df2

#Comment Details for Streamlit
def show_comments_table():
    cm_list=[]
    db=client["Youtube_DataHarvest"]
    coll1=db["channel_details"]

    for cm_data in coll1.find({},{"_id":0,"comment_information":1}):
        for i in range(len(cm_data["comment_information"])):
            cm_list.append(cm_data["comment_information"][i])

    df3=st.dataframe(cm_list)

    return df3

#Streamlit part

with st.sidebar:
    st.title(":red[Home]")
    st.header(":red[Data Harvesting]")
    
    channel_id=st.text_input("Enter the channel ID")

    if st.button("Collect and store data"):
        ch_ids=[]
        db=client["Youtube_DataHarvest"]
        coll1=db["channel_details"]

        for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
            ch_ids.append(ch_data["channel_information"]["Channel_Id"])

        if channel_id in ch_ids:
            st.success("Channel ID already exists")

        else:
            insert=channel_details(channel_id)
            st.success(insert)
    
    st.header(":red[Data Warehousing]")
    all_channels=[]
    db=client["Youtube_DataHarvest"]
    coll1=db["channel_details"]

    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        all_channels.append(ch_data["channel_information"]["Channel_Name"])

    unique_channel = st.selectbox("Select the Channel",all_channels)

    if st.button("Migrate to PostgreSQL"):
        table=postgresql_table_creation(unique_channel)
        st.success(table)

st.header(":red[YOUTUBE DATA HARVESTING AND DATA WAREHOUSING]")
show_table=st.radio("Select Table for View",("Channels","Playlists","Videos","Comments"))

if show_table == "Channels":
    show_channels_table()

elif show_table == "Playlists":
    show_playlist_table()

elif show_table == "Videos":
    show_video_table()

elif show_table == "Comments":
    show_comments_table()


#PostgreSQL Connection

mydb=psycopg2.connect(host="localhost",
                    user="postgres",
                    password="PostPer@24",
                    database="youtube_datawarehouse",
                    port="5432")

cursor=mydb.cursor()

question=st.selectbox("Select your question",("1. All the videos and channel name",
                                            "2. Channels with most number of Videos",
                                            "3. 10 most viewed videos",
                                            "4. Comments in each Videos",
                                            "5. Videos with highest likes",
                                            "6. Likes of all Videos",
                                            "7. Views of each Channel",
                                            "8. Videos published in the year of 2022",
                                            "9. Average duration of all Videos in each Channel",
                                            "10. Videos with highest number of comments"
                                            ))


if question == "1. All the videos and channel name":
    query1='''Select title as Video_Name, channel_name as Channel_Name from videos'''
    cursor.execute(query1)
    mydb.commit()
    t1=cursor.fetchall()
    df=pd.DataFrame(t1,columns=["Video Tiltle","Channel Name"])
    st.write(df)

elif question == "2. Channels with most number of Videos":
    query2='''select channel_name , count(video_id) as video_count from videos group by channel_name order by 2 desc'''
    cursor.execute(query2)
    mydb.commit()
    t2=cursor.fetchall()
    df2=pd.DataFrame(t2,columns=["Channel Name","No of Videos"])
    df2_sorted=df2.sort_values(by ="No of Videos",ascending=False)
    st.bar_chart(df2_sorted,x="Channel Name",y="No of Videos",horizontal=True)
    st.write(df2)
    

elif question == "3. 10 most viewed videos":
    query3='''select title as video_title, channel_name as channel_name, view_count from videos order by 3 desc limit 10'''
    cursor.execute(query3)
    mydb.commit()
    t3=cursor.fetchall()
    df3=pd.DataFrame(t3,columns=["Video Title","Channel Name","View"])
    st.scatter_chart(df3,x="Channel Name",y="Video Title",size="View")
    st.write(df3)
    

elif question == "4. Comments in each Videos":
    query4='''select comment_count, title as video_title from videos where comment_count is not null'''
    cursor.execute(query4)
    mydb.commit()
    t4=cursor.fetchall()
    df4=pd.DataFrame(t4,columns=["No of Comments","Video Title"])
    st.write(df4)

elif question == "5. Videos with highest likes":
    query5='''select title as video_title,channel_name as channel_name,like_count as like_counts from videos where like_count is not null order by like_count desc'''
    cursor.execute(query5)
    mydb.commit()
    t5=cursor.fetchall()
    df5=pd.DataFrame(t5,columns=["Video Title","Channel Name","Like Count"])
    st.write(df5)

elif question == "6. Likes of all Videos":
    query6='''select title as video_title,like_count as like_counts from videos'''
    cursor.execute(query6)
    mydb.commit()
    t6=cursor.fetchall()
    df6=pd.DataFrame(t6,columns=["Video Title","Like Count"])
    fig = px.bar(df6, x='Video Title',y='Like Count', title='Likes of all Videos')
    st.plotly_chart(fig, use_container_width=True)
    st.write(df6)

elif question == "7. Views of each Channel":
    query7='''select channel_name as channel_name,sum(view_count) as view_count from videos group by channel_name'''
    cursor.execute(query7)
    mydb.commit()
    t7=cursor.fetchall()
    df7=pd.DataFrame(t7,columns=["Channel Name","View Count"])
    fig = px.pie(df7, names='Channel Name',
                         values='View Count', hole=0.5)
    st.plotly_chart(fig, use_container_width=True)
    st.write(df7)

elif question == "8. Videos published in the year of 2022":
    query8='''select channel_name as channel_name,title as video_title,published_date  from videos where  extract(year from published_date)=2022'''
    cursor.execute(query8)
    mydb.commit()
    t8=cursor.fetchall()
    df8=pd.DataFrame(t8,columns=["Channel Name","Video Title","Published Date"])
    fig = px.bar(df8, x='Channel Name', template='seaborn')
    st.plotly_chart(fig, use_container_width=True)
    st.write(df8)

elif question == "9. Average duration of all Videos in each Channel":
    query9='''select channel_name as channel_name,cast(avg(duration) as varchar(20)) as avg_runtime from videos group by channel_name'''
    cursor.execute(query9)
    mydb.commit()
    t9=cursor.fetchall()
    df9=pd.DataFrame(t9,columns=["Channel Name","Average Duration"])
    st.line_chart(df9,x="Channel Name",y="Average Duration")
    st.write(df9)

elif question == "10. Videos with highest number of comments":
    query10='''select channel_name as channel_name,title as video_title,comment_count as comment_count from videos where comment_count is not null order by comment_count desc'''
    cursor.execute(query10)
    mydb.commit()
    t10=cursor.fetchall()
    df10=pd.DataFrame(t10,columns=["Channel Name","Video Title","Comments Count"])
    fig = px.pie(df10, names='Channel Name',
                         values='Comments Count', hole=0.5)
    st.plotly_chart(fig, use_container_width=True)
    st.write(df10)




