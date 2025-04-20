import streamlit as st
import googleapiclient
import pandas as pd
from  googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import mysql.connector as sql
from pymongo import MongoClient
from sqlalchemy import create_engine
from pprint import pprint
from datetime import datetime
import json

#api key connection
def api_connect():
    apikey    = 'AIzaSyAx2pRgb4-oQeNvyWHRHxE6EiqFHPyXG-E'
    api_service_name = "youtube"
    api_version = "v3"
    
    youtube   =  build(api_service_name,api_version, developerKey= apikey)
    return youtube

youtube = api_connect()


def getmongoconnection(collectionname):
#Mongodb connection
    client = MongoClient("mongodb://localhost:27017/") 
    mongo_client = MongoClient("mongodb://localhost:27017/")
        # Access or create a database
    db = client["yth"]
    mongo_db = mongo_client["yth"]
        # Access or create a collection
    collection = db[collectionname]
    return collection


# for playlist in response.get("items",[]):
#     response = youtube.playlists().list(
#     part="snippet,contentDetails",
#     channelId= channel_id,
#     maxResults=50
# ).execute()

def getmysqlconnection():  
    # Connect to MySQL
    mysql_connection = sql.connect(
        host="local",
        user="root",
        password="root",
        database="youtubeharvesting"
    )
    return mysql_connection

mysql_cursor = mysql_connection.cursor()

#get channel information 
def channeldetails(channelid):
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id  = channelid
    )
    response = request.execute()
    #print(response)
  
    for i in range (0,len(response["items"])):         

        z= dict( 
                Channel_Id = response["items"][0]["id"],
                Channel_Name = response["items"][0]["snippet"]["title"],
                Type=response['items'][0]['kind'],
                viewCount=response['items'][0]['statistics']['viewCount'],
                videoCount=response['items'][0]['statistics']['videoCount'],
                Channel_Description = response["items"][0]["snippet"]["description"],
                Playlist_Id = response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"] )
        return z

#get playlistdetails
def playlistdetails(Pchannelid):
    All_data = []
    next_page_token = None
    next_page = True
    while next_page:

        request = youtube.playlists().list(
            part="snippet,contentDetails",
            channelId=Pchannelid,
            maxResults=50,
            pageToken=next_page_token
            )
        response = request.execute()

        for item in response['items']: 
            data={'PlaylistId':item['id'],
                    'Title':item['snippet']['title'],
                    'ChannelId':item['snippet']['channelId'],
                    'ChannelName':item['snippet']['channelTitle'],
                    'PublishedAt':item['snippet']['publishedAt'],
                    'VideoCount':item['contentDetails']['itemCount']}
            All_data.append(data)
        next_page_token = response.get('nextPageToken')
        if next_page_token is None:
            next_page=False


    return All_data

    # Pl =[]

    # request = youtube.playlists().list(
    #     part="snippet, contentDetails", channelId= Pchannelid,  maxResults=5
    # )
    
    # response1= request.execute()

    # print(response1)

    # for item in response1['items']: 
    #     P1 = dict(
    #                     PlaylistID = item['id'],
    #                     channelid= item['snippet']['channelId'],
    #                     PlaylistTitle= item['snippet']['title'],                                            
    #                     NumberofVideos= item['contentDetails']['itemCount']
    #     )
    #     Pl.append(P1)
    # return Pl
      
# Get video IDs from a YouTube channel
def get_channel_videoids(channel_id):
    try:
        request = youtube.search().list(
            part='snippet',  # Gets metadata like title, description
            channelId=channel_id,
            maxResults=10,
            type='video'  # Ensures only videos (not playlists or channels) are returned
    )
        response = request.execute()


        video_ids = [item['id']['videoId'] for item in response['items'] if item['id'].get('videoId')]
        return video_ids
    except HttpError as e:
        st.error(f"YouTube Data API Error: {e}")
        return []
    except Exception as e:
        st.error(f"An unexpected error occurred: {e}")
        return []
    
#get comment information

def commentdetails(video_ids):
    Comment_Info = []
    Comment_information = []


    for video_id in video_ids:


            request = youtube.commentThreads().list(
                part="snippet",
                videoId  = video_id,
                maxResults = 5
                )
            response2=request.execute()
            pprint(response2)   
            for item in response2["items"]:
                        Comment_Info = dict(
                            Comment_Id = item["snippet"]["topLevelComment"]["id"],
                            Video_Id = item["snippet"]["videoId"],
                            Comment_Text = item["snippet"]["topLevelComment"]["snippet"]["textOriginal"],
                            Comment_Author = item["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                            Comment_Published = item["snippet"]["topLevelComment"]["snippet"]["publishedAt"])

                        Comment_information.append(Comment_Info)

                
    return (Comment_information)

    # # get Uploads playlist id
    # res = youtube.channels().list(id=channel_id, 
    #                               part='contentDetails').execute()
    # playlist_id = res['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    # next_page_token = None
    
    # while True:
    #     res = youtube.playlistItems().list( 
    #                                         part = 'snippet',
    #                                         playlistId = playlist_id, 
    #                                         maxResults = 50,
    #                                         pageToken = next_page_token).execute()
      
    #     for i in range(len(res['items'])):
    #         video_ids.append(res['items'][i]['snippet']['resourceId']['videoId'])
    #     next_page_token = res.get('nextPageToken')
        
    #     if next_page_token is None:
    #       break
    #   # return video_ids

#get video information
def get_video_info(video_ids):
    video_data = []


    for video_id in video_ids:
        request = youtube.videos().list(
                    part="snippet,contentDetails,statistics",
                     id= video_id)
        
        response3 = request.execute()

        for item in response3["items"]:
            data = dict(
                    Video_Id = item['id'],
                    Title = item['snippet']['title'],
                    Definition = item['contentDetails']['definition'],
                    Description = item['snippet']['description'],
                    Published_Date = item['snippet']['publishedAt'],
                    Views = item['statistics']['viewCount'],
                    Likes = item['statistics'].get('likeCount'),
                    Favorite_Count = item['statistics']['favoriteCount'],
                    Comments = item['statistics'].get('commentCount'),
                    Duration = item['contentDetails']['duration'],
                    Thumbnail = item['snippet']['thumbnails']['default']['url'],
                    Caption_Status = item['contentDetails']['caption']                 
                    )
            video_data.append(data)

    return video_data

#upload to mongodatabase
def putchannelinmongo(channelid):

    # Connect to MongoDB
    # Access or create a collection
    collection = getmongoconnection ("ytChannels")
    data1 = [channeldetails(channelid)]

    # Insert the data into the collection
    result1 = collection.insert_many(data1)

def putplaylistinmongo(channelid):

    # Access or create a collection
    collection =  getmongoconnection ("ytPlaylist")
    data1 = playlistdetails(channelid)

    # Insert the data into the collection
    result1 = collection.insert_many(data1)    

def putcommentinmongo(video_ids):

    # Access or create a collection
    collection =  getmongoconnection ("ytComment")
    data1 = commentdetails(video_ids)

    # Insert the data into the collection
    result1 = collection.insert_many(data1)  

def putvideoidinmongo(channelid):

    # Access or create a collection
    collection =  getmongoconnection ("ytVideoid")
    data1 = get_channel_videoids(channelid)

    json_data = json.dumps([{"value": v} for v in data1])

    jdata=[]

    for item in data1:
        data = dict(
                Video_Id = item)
        jdata.append(data)

    # Insert the data into the collection
    result1 = collection.insert_many(jdata)    

def putvideoinfoinmongo(video_ids):
    # Access or create a collection
    collection =  getmongoconnection ("ytVideoinfo")
    data1 = get_video_info(video_ids)

    # Insert the data into the collection
    result1 = collection.insert_many(data1) 

def printchannelmongo():
    collection = getmongoconnection ("ytChannels")
    # Fetch data and load into DataFrame
    cursor = collection.find({})
    df = pd.DataFrame(list(cursor))
    return df 

def printplaylistmongo():
    collection =  getmongoconnection ("ytPlaylist")
    # Fetch data and load into DataFrame
    cursor = collection.find({})
    df = pd.DataFrame(list(cursor))
    return df 

def printcommentmongo():
    collection =  getmongoconnection ("ytComment")
    # Fetch data and load into DataFrame
    cursor = collection.find({})
    df = pd.DataFrame(list(cursor))
    return df 

def printvideoidmongo():
    collection =  getmongoconnection ("ytVideoid")
    # Fetch data and load into DataFrame
    cursor = collection.find({})
    df = pd.DataFrame(list(cursor))
    return df 

def printvideoinfomongo():
    collection =  getmongoconnection ("ytVideoinfo")
    # Fetch data and load into DataFrame
    cursor = collection.find({})
    df = pd.DataFrame(list(cursor))
    return df
    
    
def getMySQLResult(query):
 
   # Database connection (Modify with your credentials)
    engine = create_engine("mysql+pymysql://root:root@localhost/yth")

    # Execute the query and fetch the result
    with engine.connect() as connection:
        result = pd.read_sql(query, connection)

    return result

# Connect to MongoDB
mongo_client = MongoClient("mongodb://localhost:27017/")
mongo_db = mongo_client["yth"]

# Connect to MySQL
mysql_conn = sql.connect(
    host="localhost",
    user="root",
    password="root",
    database="youtubeharvesting"
)
mysql_cursor = mysql_conn.cursor()

# Function to migrate data from MongoDB to MySQL
def migrate_collection(mongo_collection_name, table_name, field_mapping):
    try:
        mongo_collection = mongo_db[mongo_collection_name]
        data = list(mongo_collection.find({}, {"_id": 0}))  # Exclude MongoDB _id

        if not data:
            print(f"No data found in MongoDB collection: {mongo_collection_name}")
            return

        # Construct MySQL query
        insert_query = f"INSERT INTO {table_name} ({', '.join(field_mapping.values())}) VALUES ({', '.join(['%s'] * len(field_mapping))})"
        print(insert_query)
        
        # Prepare values for bulk insertion
        values_list = []
        tup = ()

      # Comma is necessary to create a tuple
        for record in data:
            #values = tuple(record.get(mongo_field, None) for mongo_field in field_mapping.keys())
            for mongo_field in field_mapping.keys():
                if mongo_field == "PublishedAt":
                    tup += (datetime.strptime(record.get(mongo_field, None), "%Y-%m-%dT%H:%M:%S.%fZ").strftime("%Y-%m-%d %H:%M:%S"),)
                elif mongo_field =="Comment_Published":
                    tup += (datetime.strptime(record.get(mongo_field, None), "%Y-%m-%dT%H:%M:%SZ").strftime("%Y-%m-%d %H:%M:%S"),)
                elif mongo_field =="Published_Date":
                    tup += (datetime.strptime(record.get(mongo_field, None), "%Y-%m-%dT%H:%M:%SZ").strftime("%Y-%m-%d %H:%M:%S"),)    
                #datetime.strptime(iso_date, "%Y-%m-%dT%H:%M:%SZ").strftime("%Y-%m-%d %H:%M:%S")
                else:
                    tup += (record.get(mongo_field, None),)   
            
            print(tup)
            values_list.append(tup)
            tup = ()

        #print(values_list)
        # Perform batch insert
        mysql_cursor.executemany(insert_query, values_list)
        mysql_conn.commit()

        print(f"Successfully migrated {len(data)} records from {mongo_collection_name} to {table_name}")

    except Exception as e:
        print(f"Error migrating collection {mongo_collection_name}: {e}")

# Define collection-to-table mappings
collections_to_migrate = {
    "ytChannels": {    
        "table_name": "youtubechannel",
        "field_mapping": {
            "Channel_Id":"Channel_Id",
            "Channel_Name":"Channel_Name",
            "Type": "Type",
            "viewCount": "viewCount",
            "videoCount": "videoCount",
            "Channel_Description" :"Channel_Description",
            "Playlist_Id": "Playlist_Id"            
        }
    },
    "ytPlaylist": {
        "table_name": "youtubeplaylist",
        "field_mapping": {
            "PlaylistID": "PlaylistID",
            "Title": "Title",
             "ChannelId": "ChannelId",
             "ChannelName":"ChannelName",
             "PublishedAt":"PublishedAt",
             "VideoCount" : "VideoCount"
                  
        }
    },   
   
    "ytComment": {
        "table_name": "youtubecomment",
        "field_mapping": {
                "Comment_Id": "Comment_Id",
                "Video_Id": "Video_Id",
                "Comment_Text": "Comment_Text",
                "Comment_Author":"Comment_Author",
                "Comment_Published":"Comment_Published"             
        }
    },
     "ytVideoid": {
        "table_name": "youtubevideoid",
        "field_mapping": {
                "Video_Id": "Video_Id"                           
        }
    },
    "ytVideoinfo": {
        "table_name": "youtubevideoinfo",
        "field_mapping": {
                "Video_Id": "Video_Id",
                "Title": "Title",
                "Definition": "Definition",
                "Description":"Description",
                "Published_Date":"Published_Date",
                "Views": "Views",
                "Likes":"Likes",
                "Favorite_Count":"Favorite_Count",
                "Comments": "Comments",
                "Duration": "Duration",
                "Thumbnail": "Thumbnail",
                "Caption_Status": "Caption_Status"
        } 
    },
}              
#pprint(collections_to_migrate.items())
# Migrate each collection
for mongo_collection, config in collections_to_migrate.items():
    #pprint(mongo_collection)
    #pprint(config)
    migrate_collection(mongo_collection, config["table_name"], config["field_mapping"])

# Close connections
mysql_cursor.close()
mysql_conn.close()

print("Data migration from MongoDB to MySQL completed successfully!")


# Streamlit page start 

st.title("Youtube Harvest App")

# Two text input boxes
channal_ID = st.text_input("Enter Channal ID:")

if st.button("Harvest"):
    try:
        putchannelinmongo(channal_ID)
        putplaylistinmongo(channal_ID)
        video_ids = get_channel_videoids(channal_ID)  # Call function to get video IDs
        putvideoinfoinmongo(video_ids)
        putvideoidinmongo(channal_ID)
        putcommentinmongo(video_ids)


        st.success(f"The successfully inserted channel {channal_ID} data to database")
    except ValueError:
        st.error("Please enter valid channel.")

if st.button("Print channel"):
    try:
        df = printchannelmongo()
        #st.success(f"{df}")
        st.table(df)
    except ValueError:
        st.error("Please enter valid channel.")

if st.button("Print playlist"):
    try:
        df = printplaylistmongo()
        #st.success(f"{df}")
        st.table(df)
    except ValueError:
        st.error("Please enter valid channel.")


if st.button("Print videoinformation"):
    try:
        df = printvideoinfomongo()
        #st.success(f"{df}")
        st.table(df)
    except ValueError:
        st.error("Please enter valid channel.")


if st.button("Print videoid"):
    try:
        df = printvideoidmongo()
        #st.success(f"{df}")
        st.table(df)
    except ValueError:
        st.error("Please enter valid channel.")     

if st.button("Print comment"):
    try:
        df = printcommentmongo()
        #st.success(f"{df}")
        st.table(df)
    except ValueError:
        st.error("Please enter valid channel.")             

if st.button("Q1.What is the total number of videos under the title 'Diwali sweets snacks' in the ytplaylist table"):
    try:
            # Define the SQL query
        query1 = """
            SELECT SUM(NumberofVideos) AS video_count  
            FROM yth.ytPlaylist  
            WHERE PlaylistTitle = 'Diwali sweets snacks';
        """
        result = getMySQLResult(query1)

        # Display result
        if not result.empty and result["video_count"][0] is not None:
            st.write(f"### Total Videos: {int(result['video_count'][0])}")
        else:
            st.write("### No videos found for the given playlist.")

    except ValueError:
        st.error("Please enter valid channel.")
if st.button("Q2. Can you tell me the details of the maximum number of videos viewed?"):
    try:
            # Define the SQL query
        query2 = """
            SELECT P.channelid , C.customUrl, P.PlaylistTitle, P.NumberofVideos,C.videoCount,C.viewCount
            FROM ytPlaylist AS P JOIN ytchannels AS C on (P.channelid) = (C.channelid)
            where NumberofVideos = (select MAX(CAST( NumberofVideos as UNSIGNED)) from ytplaylist)
        """
        result = getMySQLResult(query2)
        st.table(result)

        # # Display result
        # if not result.empty and result["NumberofVideos"][0] is not None:
        #     st.write(f"### Maximum Number of Videos: {int(result['NumberofVideos'][0])}")
        # else:
        #     st.write("### No videos found for the given playlist.")

    except ValueError:
        st.error("Please enter valid channel.")
if st.button("Q3. Can you tell me the details of the minimum number of videos viewed?"):
    try:
            # Define the SQL query
        query3 = """
            SELECT P.channelid , C.customUrl, P.PlaylistTitle, P.NumberofVideos,C.videoCount,C.viewCount
            FROM ytPlaylist AS P JOIN ytchannels AS C on (P.channelid) = (C.channelid)
            where NumberofVideos = (select MIN(CAST( NumberofVideos as UNSIGNED)) from ytplaylist)
        """
        result = getMySQLResult(query3)
        st.table(result)

        # # Display result
        # if not result.empty and result["NumberofVideos"][0] is not None:
        #     st.write(f"### Minimum Number of Videos: {int(result['NumberofVideos'][0])}")
        # else:
        #     st.write("### No videos found for the given playlist.")

    except ValueError:
        st.error("Please enter valid channel.")     
if st.button("Q4.Can you tell me the details about the videos which is greater than or equal to  AVERAGE number of videos which is viewed?"):
    try:
            # Define the SQL query
        query4 = """
            SELECT P.channelid , C.customUrl, P.PlaylistTitle, P.NumberofVideos,C.videoCount,C.viewCount
            FROM ytPlaylist AS P JOIN ytchannels AS C on (P.channelid) = (C.channelid)
            where NumberofVideos >= (select AVG(CAST( NumberofVideos as UNSIGNED)) from ytplaylist)
        """
        result = getMySQLResult(query4)
        st.table(result)

        # # Display result
        # if not result.empty and result["NumberofVideos"][0] is not None:
        #     st.write(f"### More than the Average Number of Videos: {int(result['NumberofVideos'][0])}")
        # else:
        #     st.write("### No videos found for the given playlist.")

    except ValueError:
        st.error("Please enter valid channel.")        
if st.button("Q5.Can you tell me the details about the videos which is lesser than or equal to  AVERAGE number of videos which is viewed?"):
    try:
            # Define the SQL query
        query5 = """
            SELECT P.channelid , C.customUrl, P.PlaylistTitle, P.NumberofVideos,C.videoCount,C.viewCount
            FROM ytPlaylist AS P JOIN ytchannels AS C on (P.channelid) = (C.channelid)
            where NumberofVideos <= (select AVG(CAST( NumberofVideos as UNSIGNED)) from ytplaylist)
        """
        result = getMySQLResult(query5)
        st.table(result)

        # # Display result
        # if not result.empty and result["NumberofVideos"][0] is not None:
        #     st.write(f"### Less than the average Number of Videos: {int(result['NumberofVideos'][0])}")
        # else:
        #     st.write("### No videos found for the given playlist.")
    except ValueError:
        st.error("Please enter valid channel.")  

if st.button("Q6.Can you tell me the details about total number of videos for each channelid and customerURL"):
    try:
            # Define the SQL query
        query6 = """
            SELECT c.channelid , C.customUrl,
            sum(P.NumberofVideos)
            FROM ytPlaylist AS P JOIN ytchannels AS C on (P.channelid) = (C.channelid)
            group by c.channelid , C.customUrl
        """
        result = getMySQLResult(query6)

        st.table(result)

    except ValueError:
        st.error("Please enter valid channel.")  
if st.button("Q7.Can you tell me the details about the maximum number of videos for each channelid and customerURL"):
    try:
            # Define the SQL query
        query7 = """
          SELECT c.channelid , C.customUrl,
          max(P.NumberofVideos)
          FROM ytPlaylist AS P JOIN ytchannels AS C on (P.channelid) = (C.channelid)
          group by c.channelid , C.customUrl
        """
        result = getMySQLResult(query7)

        st.table(result)

    except ValueError:
        st.error("Please enter valid channel.")                         

if st.button("Q8.Can you tell me the details about the minimum number of videos for each channelid and customerURL"):
    try:
            # Define the SQL query
        query8 = """
           SELECT c.channelid , C.customUrl,
           min(P.NumberofVideos)
           FROM ytPlaylist AS P JOIN ytchannels AS C on (P.channelid) = (C.channelid)
           group by c.channelid , C.customUrl
        """
        result = getMySQLResult(query8)

        st.table(result)

    except ValueError:
        st.error("Please enter valid channel.")      
if st.button("Q9. Give me the details of  playlist title name  which has the word tamil"):
    try:
        # Define the SQL query
        query9 = """       
            SELECT PlaylistTitle, PlaylistID,NumberofVideos,channelid
            FROM  ytplaylist
            WHERE TRIM(PlaylistTitle) LIKE '%%tamil%%'
        """
        result = getMySQLResult(query9)
        st.table(result)

    except ValueError as e:
        # e contains the exception object
        print(f"An error occurred: {e}")
                             

if st.button("Q10. Give me the details of  playlist id name  which has the playlist id 'PLsBNiywkm9QTv9wU3Sv9XOUf-qaM6zoyQ'"):
    try:
            # Define the SQL query
        query10 = """
          SELECT PlaylistTitle,PlaylistID,NumberofVideos,channelid
          FROM ytplaylist
          where PlaylistID='PLsBNiywkm9QTv9wU3Sv9XOUf-qaM6zoyQ'
        """
        result = getMySQLResult(query10)

        st.table(result)

    except ValueError as e:
        # e contains the exception object
        print(f"An error occurred: {e}")
                             


