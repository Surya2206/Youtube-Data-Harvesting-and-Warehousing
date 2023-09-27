import streamlit as st
import googleapiclient.discovery 
from pprint import pprint
import pandas as pd
import mysql.connector
from pymongo import MongoClient

API_key = 'AIzaSyC4NguNP1px184DGIWFccuqPY9IK-diIHs'

st.header('YOUTUBE DATA HARVESTING AND WAREHOUSING', divider='rainbow')

api_service_name = "youtube"
api_version = "v3"
DEVELOPER_KEY = "AIzaSyC4NguNP1px184DGIWFccuqPY9IK-diIHs"


youtube = googleapiclient.discovery.build(
        api_service_name, api_version, developerKey = DEVELOPER_KEY)


def get_channel_details(youtube,channel_id):
    all_data = []
    request = youtube.channels().list(
        part = "snippet,contentDetails, statistics",
        id = channel_id
    )
    response = request. execute()
    for i in response['items']:
        channels = dict(channel_id             = i['id'], 
                        channel_name           = i['snippet']['title'], 
                        channel_link           = i['snippet']['customUrl'], 
                        channel_publish        = str((i['snippet']['publishedAt']).split("T")[0]),  
                        channel_description    = i['snippet']['description'], 
                        channel_view           = i['statistics']['viewCount'], 
                        channel_subscriber     = i['statistics']['subscriberCount'], 
                        channel_video          = i['statistics']['videoCount'],
                        channel_uploads        = i['contentDetails']['relatedPlaylists']['uploads'])
        all_data.append(channels)
    return all_data

def get_playlist_details (youtube, channel_id):
    
    playlist_details = []
    
    request = youtube.playlists().list( 
        part='snippet,contentDetails', 
        channelId = channel_id,
        maxResults = 50
    )
        
    response = request.execute()
    
    for playlist in response['items']: 
        playlist_info = dict(playlist_id = playlist['id'],
                             channel_id = playlist['snippet']['channelId'],
                             playlist_name = playlist['snippet']['title']
                                )
                        
        playlist_details.append(playlist_info)
        
        
    next_page_token = response.get('nextPageToken')
    while next_page_token is not None:
        
        request = youtube.playlists().list( 
            part='snippet,contentDetails', 
            channelId = channel_id,
            maxResults = 50,
            pageToken = next_page_token
        )
        response = request.execute()
        
        for playlist in response['items']: 
            playlist_info = dict(playlist_id = playlist['id'],
                             channel_id = playlist['snippet']['channelId'],
                             playlist_name = playlist['snippet']['title']
                                )
            playlist_details.append(playlist_info)
            
        next_page_token = response.get('nextPageToken')
    
    return playlist_details

    # ******************************************************************************************************************************

def get_video_ids (youtube, channel_uploads):
    video_ids = []
        
    request = youtube.playlistItems().list(
        part='snippet,contentDetails',
        playlistId=channel_uploads,
        maxResults = 50
    )
    response = request.execute()
        
    for i in response['items']:
        video_ids.append(i['contentDetails']['videoId'])
            
    next_page_token = response.get('nextPageToken')
    while next_page_token is not None:
            
        request = youtube.playlistItems().list(
                part='snippet,contentDetails',
                playlistId = channel_uploads ,
                maxResults = 50,
                pageToken = next_page_token
            )
        response = request.execute() 
                
        for i in response['items']:
            video_ids.append(i['contentDetails']['videoId'])
                    
        next_page_token = response.get('nextPageToken')         
        
    return video_ids

def get_video_details (youtube, video_ids,channel_uploads):
    all_video_stats = []
    for i in range (0, len(video_ids), 50):
            
            request = youtube.videos().list(
                    part='snippet,statistics,contentDetails',
                    id=','.join(video_ids[i:i+50])
            )

            response = request.execute()
                
                
            def time_str_to_seconds(t):
                def time_duration(t):
                    a = pd.Timedelta(t)
                    b = str(a).split()[-1]
                    return b
                time_str = time_duration(t)
                    
                hours, minutes, seconds = map(int, time_str.split(':'))
                total_seconds = (hours * 3600) + (minutes * 60) + seconds
                    
                return total_seconds

                
            for i in response['items']:
                video_stats = dict(video_id            = i['id'],
                                    playlist_id         = channel_uploads,
                                    channel_id          = i['snippet']['channelId'],
                                    video_name          = i['snippet']['title'],
                                    video_description   = i['snippet']['description'],
                                    published_date      = str((i['snippet']['publishedAt']).split("T")[0]), 
                                    views               = i['statistics']['viewCount'],
                                    likes               = i['statistics']['likeCount'],
                                    favorite            = i['statistics']['favoriteCount'],
                                    comments            = i['statistics']['commentCount'],
                                    duration            = str(time_str_to_seconds(i['contentDetails']['duration'])),
                                    thumbnail           = i['snippet']['thumbnails']['default']['url'],
                                    caption_status      = i['contentDetails']['caption']
                                    )     

                all_video_stats.append(video_stats) 
    return all_video_stats


    # ***********************************************************************************************************************************

def get_comments_details(youtube, video_ids):
    all_comments_status = []
    for i in video_ids:
        try:
            
            request = youtube.commentThreads().list(
                    part='snippet,replies',
                    videoId=i,
                    maxResults=100
            )
            response = request.execute()
        #         pprint(response)
            for item in response['items']:
                comment_info = dict(comment_id              =item['id'],  
                                    video_id                =item['snippet']['videoId'],
                                    comment_text            =item['snippet']['topLevelComment']['snippet']['textDisplay'],
                                    comment_author          =item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                                    comment_published_date  =str((item['snippet']['topLevelComment']['snippet']['publishedAt']).split("T")[0])
                                )

                all_comments_status.append(comment_info)
        except:
            pass
    return all_comments_status


def main(channel_id):
    channel_data  = get_channel_details(youtube,channel_id)
    playlist_data = get_playlist_details(youtube,channel_id)
    v_ids         = get_video_ids (youtube, channel_data[0]['channel_uploads'])
    video_data    = get_video_details (youtube,v_ids,channel_data[0]['channel_uploads'])
    comments_data = get_comments_details(youtube, v_ids)

        
    data = {
        'channel_details': channel_data,
        'playlist_details': playlist_data,
        'video_details': video_data,
        'comment_details': comments_data,
    }
        
    return data


    # ******************************************************************************************************************************************

mongo_uri = "mongodb://localhost:27017/" 
database_name = "project"       
collection_name = "rise"   

client = MongoClient(mongo_uri)
db = client[database_name]
collection = db[collection_name]

data = {}
for i in collection.find({},{"_id":0,'channel_details':1,'playlist_details':1,'video_details':1, 'comment_details':1}):
    data.update(i)


def create_database_and_tables(channel_id):


    connection = mysql.connector.connect(
        host="127.0.0.1",
        user="root",
        password="admin@123"
        )

    cursor = connection.cursor()

    cursor.execute("CREATE DATABASE IF NOT EXISTS youtube")
    cursor.execute("use youtube")


    cursor.execute('''CREATE TABLE IF NOT EXISTS channel (
                                channel_id VARCHAR(100),
                                channel_name VARCHAR(100),
                                channel_link VARCHAR(100),
                                channel_publish date,
                                channel_description TEXT,
                                channel_view VARCHAR(100),
                                channel_subscriber INT(64),
                                channel_video INT(64),
                                channel_uploads VARCHAR(100)
                            )''')

    cursor.execute('''CREATE TABLE IF NOT EXISTS playlist (
                                playlist_id VARCHAR(100),
                                channel_id VARCHAR(100),
                                playlist_name VARCHAR(100)
                            )''')
            
    cursor.execute('''create table if not exists video (
                                video_id varchar(250),
                                playlist_id varchar(250), 
                                channel_id varchar(250), 
                                video_name varchar(250), 
                                video_description text, 
                                published_date date, 
                                views int(64), 
                                likes int(64), 
                                favorite int(64), 
                                comments int(64), 
                                duration int(64), 
                                thumbnail varchar(250), 
                                caption_status varchar(250)
                            )''')

    cursor.execute('''create table if not exists comment (
                                comment_id varchar(250), 
                                video_id varchar(250), 
                                comment_text text, 
                                comment_author varchar(250), 
                                comment_published_date date
                            )''')


    connection.commit()
    cursor.close()
    connection.close()


def insert_data_into_tables(x,channel_id):
    connection = mysql.connector.connect(
        host="127.0.0.1",
        user="root",
        password="admin@123",
        database="youtube" 
    )

    cursor = connection.cursor()

    channel_details =data['channel_details']
    df = pd.DataFrame(channel_details)
    sql = "insert into channel (channel_id,channel_name,channel_link,channel_publish,channel_description,channel_view,channel_subscriber,channel_video,channel_uploads) values (%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    for i in range(0,len(df)):
        cursor.execute(sql,tuple(df.iloc[i]))
        connection.commit()
    for i in range(0,len(df)):
        print(tuple(df.iloc[i]))


    playlist_details = data['playlist_details']
    df1 = pd.DataFrame(playlist_details)
    sql = "insert into playlist (playlist_id, channel_id, playlist_name) values (%s,%s,%s)"
    for i in range(0,len(df1)):
        cursor.execute(sql,tuple(df1.iloc[i]))
        connection.commit()
    for i in range(0,len(df1)):
        print(tuple(df1.iloc[i]))
            
            
    video_details = data['video_details']
    df2 = pd.DataFrame(video_details)
    sql = "insert into video (video_id, playlist_id, channel_id, video_name, video_description, published_date, views,  likes, favorite, comments, duration, thumbnail, caption_status) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    for i in range(0,len(df2)):
        cursor.execute(sql,tuple(df2.iloc[i]))
        connection.commit()
    for i in range(0,len(df2)):
        print(tuple(df2.iloc[i]))


    comment_details = data['comment_details']
    df3 = pd.DataFrame(comment_details)
    sql = "insert into comment (comment_id, video_id, comment_text, comment_author, comment_published_date) values (%s,%s,%s,%s,%s)"
    for i in range(0,len(df3)):
        cursor.execute(sql,tuple(df3.iloc[i]))
        connection.commit()
    for i in range(0,len(df3)):
        print(tuple(df3.iloc[i]))


    connection.commit()
    cursor.close()
    connection.close()



channel_id = st.sidebar.text_input("Enter the channel id")
if channel_id and st.sidebar.button('Show Details'):
    a = main(channel_id)
    st.write(a)


choice =st.sidebar.selectbox('Migration',('store in Mongodb','Migrate to Sql'))
if choice == "store in Mongodb":
    if choice and st.sidebar.button("store"):
        b=collection.insert_one(main(channel_id))

else:
    if choice and st.sidebar.button("Migrate"):
            c = create_database_and_tables(channel_id)
            d = insert_data_into_tables(data,channel_id)

connection = mysql.connector.connect(
host="127.0.0.1",
user="root",
password="admin@123",
database="youtube" ) 

Question = st.sidebar.selectbox('Select the Query',(None,'What are the names of all the videos and their corresponding channels?',
                'Which channels have the most number of videos, and how many videos do they have?',
                'What are the top 10 most viewed videos and their respective channels?',
                'How many comments were made on each video, and what are their corresponding video names?',
                'Which videos have the highest number of likes, and what are their corresponding channel names?',
                'What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
                'What is the total number of views for each channel, and what are their corresponding channel names?',
                'What are the names of all the channels that have published videos in the year 2022?',
                'What is the average duration of all videos in each channel, and what are their corresponding channel names?',
                'Which videos have the highest number of comments, and what are their corresponding channel names?'))

if Question == None:
        pass
elif Question == 'What are the names of all the videos and their corresponding channels?': 
            if choice and st.sidebar.button("execute"):
                df= pd.read_sql_query('select v.video_name , c.channel_name from video as v join channel as c on v.channel_id = c.channel_id',connection)
                st.write(df)

elif Question == 'Which channels have the most number of videos, and how many videos do they have?':
            if choice and st.sidebar.button("execute"):
                df= pd.read_sql_query('select channel_name, channel_video from channel order by channel_video desc limit 10',connection)
                st.write(df)

elif Question == 'What are the top 10 most viewed videos and their respective channels?':
            if choice and st.sidebar.button("execute"):
                df= pd.read_sql_query('select v.video_id, v.video_name, v.views, c.channel_name from video as v join channel as c on v.channel_id = c.channel_id order by views desc limit 10',connection)
                st.write(df)

elif Question == 'How many comments were made on each video, and what are their corresponding video names?':
            if choice and st.sidebar.button("execute"):
                df= pd.read_sql_query('select video_id, video_name, comments from video',connection)
                st.write(df)

elif Question == 'Which videos have the highest number of likes, and what are their corresponding channel names?':
            if choice and st.sidebar.button("execute"):
                df= pd.read_sql_query('select v.video_id, v.video_name, v.likes, c.channel_name from video as v join channel as c on v.channel_id = c.channel_id order by v.likes desc limit 10',connection)
                st.write(df)

elif Question == 'What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
            if choice and st.sidebar.button("execute"):
                df= pd.read_sql_query('select video_id, video_name, likes from video',connection)
                st.write(df)

elif Question == 'What is the total number of views for each channel, and what are their corresponding channel names?':
            if choice and st.sidebar.button("execute"):
                df= pd.read_sql_query('select channel_view, channel_name from channel',connection)
                st.write(df)

elif Question == 'What are the names of all the channels that have published videos in the year 2022?':
            if choice and st.sidebar.button("execute"):
                df= pd.read_sql_query("Select c.channel_name FROM channel as c join video as v on c.channel_id = v.channel_id  WHERE v.published_date LIKE '2022%' GROUP BY channel_name ORDER BY channel_name desc",connection)
                st.write(df)

elif Question == 'What is the average duration of all videos in each channel, and what are their corresponding channel names?':
            if choice and st.sidebar.button("execute"):
                df= pd.read_sql_query('SELECT c.channel_name, AVG(v.duration) AS average_duration FROM channel as c join video as v ON c.channel_id = v.channel_id GROUP BY c.channel_name',connection)
                st.write(df)

elif Question == 'Which videos have the highest number of comments, and what are their corresponding channel names?':
            if choice and st.sidebar.button("execute"):
                df= pd.read_sql_query('select v.video_id, v.comments, c.channel_name from video as v join channel as c on v.channel_id = c.channel_id order by v.comments desc limit 10',connection)
                st.write(df)


            