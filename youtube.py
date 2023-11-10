from googleapiclient.discovery import build
import pymongo
from pymongo import MongoClient
import pandas as pd
import psycopg2
import streamlit as st

#API KEY CONNECTION 
def api_connect():
    api_id= "AIzaSyAKqAuryzO5cJguJTH3YdmwFdENjPpb-qE"
    api_service_name = "youtube"
    api_version="v3"

    youtube=build(api_service_name,api_version,developerKey=api_id)
    return youtube

youtube=api_connect()

def youtube_channel_info(channel_id):
    request=youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id
    )
    response=request.execute()

    for i in response['items']:
        particulars=dict(channel_name=i['snippet']['title'],
                        channel_id=i['id'],
                        channel_discp=i['snippet']['description'],
                        totalvideos=i['statistics']['videoCount'],
                        totalviews=i['statistics']['viewCount'],
                        subconunt=i['statistics']['subscriberCount'],
                        playlist_id=i['contentDetails']['relatedPlaylists']['uploads'])
    return particulars

#GET VIDEOS ID

def fetch_vid_ids(channel_id):
        vid_ids=[]
        response=youtube.channels().list(
                part="contentDetails",
                id=channel_id).execute()
        playlistid=response['items'][0]['contentDetails']['relatedPlaylists']["uploads"]

        nxt_page=None
        while True:
                response1=youtube.playlistItems().list(
                                                part="snippet",
                                                playlistId=playlistid,
                                                pageToken=nxt_page,
                                                maxResults=50).execute()
                for i in range(len(response1['items'])):
                        vid_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
                nxt_page=response1.get('nextPageToken')

                if nxt_page is None:
                        break
        return vid_ids

#GET VIDEO DETAILS

def fectch_vid_details(vid_details):
    video_data=[]
    for i in vid_details:
        response=youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=i).execute()
        for i in response['items']:
            data=dict(channel_name=i['snippet']['channelTitle'],
                    channel_id=i['snippet']['channelId'],
                    vid_name=i['snippet']['title'],
                    vid_id=i['id'],
                    vid_description=i['snippet'].get('description'),
                    vid_thum=i['snippet']['thumbnails']['default']['url'],
                    vid_tags=i['snippet'].get('tags'),
                    vid_publish_date=i['snippet']['publishedAt'],
                    vid_duration=i['contentDetails']['duration'],
                    vid_viewcont=i['statistics'].get('viewCount'),
                    vid_likes=i['statistics'].get('likeCount'),
                    vid_comments=i['statistics'].get('commentCount'),
                    vid_favorites=i['statistics']['favoriteCount'],
                    vid_caption=i['contentDetails']['caption'])
        video_data.append(data)
    return video_data

#GET VIDEO COMMENTS

def get_comments(video_ids):
        comment_1=[]
        try:
                for vid in video_ids:    
                        request=youtube.commentThreads().list(
                                        part="snippet",
                                        videoId= vid,
                                        maxResults=50)
                        response=request.execute()

                        for i in response['items']:
                                data=dict(comment_id=i['snippet']['topLevelComment']['id'],
                                        videoId=i['snippet']['topLevelComment']['snippet']['videoId'],
                                        comment_txt=i['snippet']['topLevelComment']['snippet']['textDisplay'],
                                        authorname=i['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                                        publish_date=i['snippet']['topLevelComment']['snippet']['publishedAt'])
                                
                                comment_1.append(data)
        except:
                pass
        return comment_1


#get playlist details

def get_playlist(channel_id):
    nxtpage_token=None
    playlist_data=[]

    while True:
        request=youtube.playlists().list(
                part="snippet,contentDetails",
                channelId=channel_id,
                maxResults=25,
                pageToken=nxtpage_token)
        response = request.execute()

        for i in response['items']:
            data=dict(playlistid=i['id'],
                    channel_id=i['snippet']['channelId'],
                    playlist_name=i['snippet']['title'],
                    channel_name=i['snippet']['channelTitle'],
                    video_count=i['contentDetails']['itemCount'])
            playlist_data.append(data)

        nxtpage_token=response.get('nextPageToken')

        if nxtpage_token is None:
            break
    return playlist_data

#UPLOAD MONGODB
client=pymongo.MongoClient("mongodb://localhost:27017/")
db=client['youtube_data']

def connect_mongodb(channel_ids):
    ch_info=youtube_channel_info(channel_ids)
    py_info=get_playlist(channel_ids)
    vd_id=fetch_vid_ids(channel_ids)
    vd_info=fectch_vid_details(vd_id)
    com_info=get_comments(vd_id)

    collection=db["channel_details"]
    collection.insert_one({"channel_info":ch_info,
                           "playlist_info":py_info,
                           "video_info":vd_info,
                           "comment_info":com_info})
    return "DATA RETRIEVED AND TRANSFERRED TO MONGODB"


#connecting mysql

def channel_sql():
        mydb1=psycopg2.connect(host='localhost',user='postgres',password='Sql0991',database='youtube_info',port=5432)
        cursor1=mydb1.cursor()

        drop_table='''drop table if exists channel'''
        cursor1.execute(drop_table)
        mydb1.commit()

        try:
                createtable='''create table if not exists channel (channel_name varchar(100),
                                                                channel_id varchar(100) primary key,
                                                                channel_discp varchar(500),
                                                                totalvideos int,
                                                                totalviews bigint,
                                                                subconunt bigint, 
                                                                playlist_id varchar(100))'''

                cursor1.execute(createtable)
                mydb1.commit()
        except:
             st.write("Channel table already created")

        ch_data=[]
        db=client['youtube_data']
        collection=db["channel_details"]
        for i in collection.find({},{'_id':0,"channel_info":1}):
                ch_data.append(i['channel_info'])
        df=pd.DataFrame(ch_data)

        for index,row in df.iterrows():
                insert_query='''insert into channel(channel_name,
                                                channel_id,
                                                channel_discp,
                                                totalvideos,
                                                totalviews,
                                                subconunt,
                                                playlist_id)
                                                
                                                values(%s,%s,%s,%s,%s,%s,%s)'''
                values=(row['channel_name'],
                        row['channel_id'],
                        row['channel_discp'],
                        row['totalvideos'],
                        row['totalviews'],
                        row['subconunt'],
                        row['playlist_id'])
                try:
                        cursor1.execute(insert_query,values)
                        mydb1.commit()
                except:
                     st.write("Channel values was already uploaded")


def playlist_sql():
    
        mydb1=psycopg2.connect(host='localhost',user='postgres',password='Sql0991',database='youtube_info',port=5432)
        cursor1=mydb1.cursor()

        drop_table='''drop table if exists playlist'''
        cursor1.execute(drop_table)
        mydb1.commit()
        try:
                createtable='''create table if not exists playlist(playlistid varchar(100) primary key,
                                                                        channel_id varchar(100),
                                                                        playlist_name varchar(100),
                                                                        channel_name varchar(100),
                                                                        video_count bigint)'''

                cursor1.execute(createtable)
                mydb1.commit()
        except:
                st.write("Playlist table was created")

        py_data=[]
        db=client['youtube_data']
        collection=db["channel_details"]
        for i in collection.find({},{'_id':0,"playlist_info":1}):
                for e in range(len(i['playlist_info'])):
                        py_data.append(i['playlist_info'][e])
        df1=pd.DataFrame(py_data)

        for index,row in df1.iterrows():
                insert_query='''insert into playlist(playlistid,
                                                channel_id,
                                                playlist_name,
                                                channel_name,
                                                video_count)
                                                
                                                values(%s,%s,%s,%s,%s)'''
                values=(row['playlistid'],
                        row['channel_id'],
                        row['playlist_name'],
                        row['channel_name'],
                        row['video_count'])
                try:
                        cursor1.execute(insert_query,values)
                        mydb1.commit()
                except:
                      st.write("playlist values was already uploaded")


def video_sql():
        mydb1=psycopg2.connect(host='localhost',user='postgres',password='Sql0991',database='youtube_info',port=5432)
        cursor1=mydb1.cursor()

        drop_table='''drop table if exists video'''
        cursor1.execute(drop_table)
        mydb1.commit()
        try:
                createtable='''create table if not exists video(channel_name varchar(200),
                                                                channel_id varchar(100),
                                                                vid_name varchar(200),
                                                                vid_id varchar(100) primary key,
                                                                vid_description TEXT,
                                                                vid_thum varchar(200),
                                                                vid_tags TEXT,
                                                                vid_publish_date timestamp,
                                                                vid_duration interval,
                                                                vid_viewcont bigint,
                                                                vid_likes bigint,
                                                                vid_comments int,
                                                                vid_favorites int, 
                                                                vid_caption varchar(100))'''

                cursor1.execute(createtable)
                mydb1.commit()
        except:
              st.write("video table was created")

        vi_data=[]
        db=client['youtube_data']
        collection=db["channel_details"]
        for i in collection.find({},{'_id':0,"video_info":1}):
                for e in range(len(i['video_info'])):
                        vi_data.append(i['video_info'][e])
        df2=pd.DataFrame(vi_data)

        for index,row in df2.iterrows():
                insert_query='''insert into video(channel_name,
                                                channel_id,
                                                vid_name,
                                                vid_id,
                                                vid_description,
                                                vid_thum,
                                                vid_tags,
                                                vid_publish_date,
                                                vid_duration,
                                                vid_viewcont,
                                                vid_likes,
                                                vid_comments,
                                                vid_favorites,
                                                vid_caption)
                                                
                                                values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''

                values=(row['channel_name'],
                        row['channel_id'],
                        row['vid_name'],
                        row['vid_id'],
                        row['vid_description'],
                        row['vid_thum'],
                        row['vid_tags'],
                        row['vid_publish_date'],
                        row['vid_duration'],
                        row['vid_viewcont'],
                        row['vid_likes'],
                        row['vid_comments'],
                        row['vid_favorites'],
                        row['vid_caption'])
                try:
                        cursor1.execute(insert_query,values)
                        mydb1.commit()
                except:
                      st.write("video values was already uploaded")

def comment_sql():
        mydb1=psycopg2.connect(host='localhost',user='postgres',password='Sql0991',database='youtube_info',port=5432)
        cursor1=mydb1.cursor()

        drop_table='''drop table if exists comment'''
        cursor1.execute(drop_table)
        mydb1.commit()
        try:
                createtable='''create table if not exists comment(comment_id varchar(100) primary key,
                                                                        videoId varchar(100),
                                                                        comment_txt text,
                                                                        authorname varchar(100),
                                                                        publish_date timestamp)'''

                cursor1.execute(createtable)
                mydb1.commit()
        except:
              st.write("comment table was already created")

        cm_data=[]
        db=client['youtube_data']
        collection=db["channel_details"]
        for i in collection.find({},{'_id':0,"comment_info":1}):
                for e in range(len(i['comment_info'])):
                        cm_data.append(i['comment_info'][e])
        df3=pd.DataFrame(cm_data)

        for index,row in df3.iterrows():
                insert_query='''insert into comment(comment_id,
                                                videoId,
                                                comment_txt,
                                                authorname,
                                                publish_date)
                                                
                                                values(%s,%s,%s,%s,%s)'''
                values=(row['comment_id'],
                        row['videoId'],
                        row['comment_txt'],
                        row['authorname'],
                        row['publish_date'])
                try:
                        cursor1.execute(insert_query,values)
                        mydb1.commit()
                except:
                      st.write("Comment values was already uploaded")

def table_sql():
    channel_sql()
    playlist_sql()
    video_sql()
    comment_sql()

    return 'SUCCESSFULLY DATA TRANSMITTED TO SQL'

def ch_data():
        ch_data=[]
        db=client['youtube_data']
        collection=db["channel_details"]
        for i in collection.find({},{'_id':0,"channel_info":1}):
                ch_data.append(i['channel_info'])
        channel_table=st.dataframe(ch_data)
        return channel_table

def py_data():
        py_data=[]
        db=client['youtube_data']
        collection=db["channel_details"]
        for i in collection.find({},{'_id':0,"playlist_info":1}):
                for e in range(len(i['playlist_info'])):
                        py_data.append(i['playlist_info'][e])
        playlist_table=st.dataframe(py_data)
        return playlist_table

def vi_data():
        vi_data=[]
        db=client['youtube_data']
        collection=db["channel_details"]
        for i in collection.find({},{'_id':0,"video_info":1}):
                for e in range(len(i['video_info'])):
                        vi_data.append(i['video_info'][e])
        video_table=st.dataframe(vi_data)
        return video_table

def cm_data():
    cm_data=[]
    db=client['youtube_data']
    collection=db["channel_details"]
    for i in collection.find({},{'_id':0,"comment_info":1}):
        for e in range(len(i['comment_info'])):
            cm_data.append(i['comment_info'][e])
    comment_table=st.dataframe(cm_data)
    return comment_table

#STREAMLIT CODE

st.header(':blue[Assortment of YouTube Infomration]')
with st.sidebar:
    st.header(":green[YouTube Project]")
    st.subheader(":gray[Reference]")
    st.caption("Streamlit - https://docs.streamlit.io/library/api-reference")
    st.caption("YouTube API - https://developers.google.com/youtube/v3/getting-started")
    st.caption("SQL - https://www.postgresql.org/docs/current/")
    st.caption("PYTHON - https://docs.python.org/3/reference/index.html")
    st.caption("MONGODB - https://www.mongodb.com/docs/manual/reference/database-references/")
    st.caption("VSCODE - https://code.visualstudio.com/docs/editor/editingevolved")
    

channel_id1=st.text_input(":red[ENTER CHANNEL ID PLEASE]")


if st.button(":green[COLLECT & STORE DATA]"):
    channel_ids1=[]
    db=client['youtube_data']
    collection=db["channel_details"]
    for data in collection.find({},{"_id":0,"channel_info":1}):
        channel_ids1.append(data['channel_info']['channel_id'])
    if channel_id1 in channel_ids1:
        st.success("Channel information of provided Channel Id is Already exists")
    else:
        output=connect_mongodb(channel_id1)
        st.success(output)


if st.button(":green[TRANSMIT TO SQL]"):  
    display=table_sql()
    st.success(display)


radio_button=st.radio(":gray[SELECT THE TABLE YOU WANT TO VIEW]",(":gray[Channels]",":gray[Playlists]",":gray[Videos]",":gray[Comments]"))

if radio_button==":gray[Channels]":
     ch_data()
elif radio_button==":gray[Playlists]":
     py_data()
elif radio_button==":gray[Videos]":
     vi_data()
elif radio_button==":gray[Comments]":
     cm_data()

#SQL CONNECT
mydb1=psycopg2.connect(host='localhost',user='postgres',password='Sql0991',database='youtube_info',port=5432)
cursor1=mydb1.cursor()

question = st.selectbox(
    'PLEASE SELECT YOUR QUESTION',
    ('1. All the videos and the Channel Name',
     '2. Channels with most number of videos',
     '3. 10 most viewed videos',
     '4. Comments in each video',
     '5. Videos with highest likes',
     '6. likes of all videos',
     '7. views of each channel',
     '8. videos published in the year 2022',
     '9. average duration of all videos in each channel',
     '10. videos with highest number of comments'))

if question == '1. All the videos and the Channel Name':
     query1="select vid_name as videoname,channel_name as channelname from video;"
     cursor1.execute(query1)
     mydb1.commit()
     d1=cursor1.fetchall()
     st.write(pd.DataFrame(d1,columns=['Video name','Channel name']))

elif question == '2. Channels with most number of videos':
     query2="select channel_name as channelname,totalvideos as no_videos from channel order by totalvideos desc;"
     cursor1.execute(query2)
     mydb1.commit()
     d2=cursor1.fetchall()
     st.write(pd.DataFrame(d2,columns=["Channel name","No of Video"]))

elif question == '3. 10 most viewed videos':
     query3='''select channel_name as channelname,vid_name as videoname,vid_viewcont as viewcount from video
                where vid_viewcont is not null order by vid_viewcont desc limit 10;'''
     cursor1.execute(query3)
     mydb1.commit()
     d3=cursor1.fetchall()
     st.write(pd.DataFrame(d3,columns=["Channel Name","Video Name","NO of Views"]))

elif question== '4. Comments in each video':
     query4="select vid_name as videoname,vid_comments as comments from video;"
     cursor1.execute(query4)
     mydb1.commit()
     d4=cursor1.fetchall()
     st.write(pd.DataFrame(d4,columns=['Video name','Comments']))

elif question=='5. Videos with highest likes':
     query5='''select vid_name as videoname,channel_name as channelname,vid_likes as likes from video
                where vid_likes is not null order by vid_likes desc;'''
     cursor1.execute(query5)
     mydb1.commit()
     d5=cursor1.fetchall()
     st.write(pd.DataFrame(d5,columns=['video name','channel name','likes']))

elif question=='6. likes of all videos':
     query6='select vid_name as videoname,vid_likes as likes from video;'
     cursor1.execute(query6)
     mydb1.commit()
     d6=cursor1.fetchall()
     st.write(pd.DataFrame(d6,columns=['video name','likes']))

elif question=='7. views of each channel':
     query7='select channel_name as channelname,totalviews as views from channel;'
     cursor1.execute(query7)
     mydb1.commit()
     d7=cursor1.fetchall()
     st.write(pd.DataFrame(d7,columns=['channel name','view count']))

elif question=='8. videos published in the year 2022':
     query8='''select channel_name as channelname,vid_name as videoname,vid_publish_date as PublishDate from video 
                where extract(year from vid_publish_date)=2022;'''
     cursor1.execute(query8)
     mydb1.commit()
     d8=cursor1.fetchall()
     st.write(pd.DataFrame(d8,columns=['channel name','video name','publish data']))

elif question=='9. average duration of all videos in each channel':
     query9='''select channel_name as channelname,AVG(vid_duration) as Average_duration from video
                group by channel_name;'''
     cursor1.execute(query9)
     mydb1.commit()
     d9=cursor1.fetchall()
     d9=pd.DataFrame(d9, columns=['ChannelTitle', 'Average Duration'])
     D9=[]
     for index, row in d9.iterrows():
        channel_title = row['ChannelTitle']
        average_duration = row['Average Duration']
        average_duration_str = str(average_duration)
        D9.append({"Channel Title": channel_title ,  "Average Duration": average_duration_str})
     st.write(pd.DataFrame(D9))

elif question=='10. videos with highest number of comments':
     query10='''select channel_name as channelname,vid_name as videoname,vid_comments as comments from video
                where vid_comments is not null order by vid_comments desc;'''
     cursor1.execute(query10)
     mydb1.commit()
     d10=cursor1.fetchall()
     st.write(pd.DataFrame(d10,columns=['channel name','video name','no of comments']))