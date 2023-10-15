import psycopg2
import psycopg2.extras
import pandas as pd
import config as c



def connection_string():
    #print(">>>> Connecting to YugabyteDB!")

    # conf = {
    # 'host': 'containers-us-west-81.railway.app',
    # 'port': '6145',
    # 'dbName': 'railway',
    # 'dbUser': c.y_dbuser,
    # 'dbPassword': c.y_dbpwd,
    # #'sslMode': 'require',
    # # 'sslRootCert': 'root.crt'
    # }

    conf = {
    'host': 'dpg-ckl4rurj89us73cbu9d0-a.singapore-postgres.render.com',
    'port': '5432',
    'dbName': 'yt_db_sql',
    'dbUser': c.y_dbuser,
    'dbPassword': c.y_dbpwd,
    #'sslMode': 'require',
    # 'sslRootCert': 'root.crt'
    }


    try:
       
        yb = psycopg2.connect(host=conf['host'], port=conf['port'], database=conf['dbName'],
                                  user=conf['dbUser'], password=conf['dbPassword'],
                                  connect_timeout=10)
    except Exception as e:
        print("Exception while connecting to YugabyteDB")
        print(e)
        exit(1)

    #create_table(yb) # Run only first time and comment it 
    
    return yb, yb.cursor()

    #print(">>>> Successfully connected to YugabyteDB!")

    



def create_table(yb):
    try:
        with yb.cursor() as yb_cursor:
            
            yb_cursor.execute('DROP TABLE IF EXISTS comment	')            
            yb_cursor.execute('DROP TABLE IF EXISTS video')
            yb_cursor.execute('DROP TABLE IF EXISTS playlist')
            yb_cursor.execute('DROP TABLE IF EXISTS channel')

            create_channel_stmt = """
                	CREATE TABLE channel (
                    channel_id varchar(255) NOT NULL,
                    channel_name varchar(255) DEFAULT NULL,
                    channel_type varchar(255) DEFAULT NULL,
                    channel_views varchar(255) DEFAULT NULL,
                    channel_description text DEFAULT NULL,
                    channel_status varchar(255) DEFAULT NULL,
                    PRIMARY KEY (channel_id)
                    ) """
            
            create_playlist_stmt = """
                		CREATE TABLE playlist (
                        playlist_id varchar(255) NOT NULL,
                        channel_id varchar(255) DEFAULT NULL,
                        playlist_name varchar(255) DEFAULT NULL,
                        PRIMARY KEY (playlist_id),
                        CONSTRAINT playlist_ibfk_1 FOREIGN KEY (channel_id) REFERENCES channel(channel_id)
                        )  """
            
            create_video_stmt = """
                		CREATE TABLE video (
                        video_id varchar(255) NOT NULL,
                        playlist_id varchar(255) DEFAULT NULL,
                        video_name varchar(255) DEFAULT NULL,
                        video_description text DEFAULT NULL,
                        published_date timestamp DEFAULT NULL,
                        view_count numeric DEFAULT NULL,
                        like_count numeric DEFAULT NULL,
                        dislike_count numeric DEFAULT NULL,
                        favorite_count numeric DEFAULT NULL,
                        comment_count numeric DEFAULT NULL,
                        duration numeric DEFAULT NULL,
                        thumbnail varchar(255) DEFAULT NULL,
                        caption_status varchar(255) DEFAULT NULL,
                        PRIMARY KEY (video_id),                        
                        CONSTRAINT video_ibfk_1 FOREIGN KEY (playlist_id) REFERENCES playlist (playlist_id)
                        )  """
            
            create_comment_stmt = """
                			CREATE TABLE comment (
                            comment_id varchar(255) NOT NULL,
                            video_id varchar(255) DEFAULT NULL,
                            comment_text text DEFAULT NULL,
                            comment_author varchar(255) DEFAULT NULL,
                            comment_published_date timestamp DEFAULT NULL,
                            PRIMARY KEY (comment_id),                            
                            CONSTRAINT comment_ibfk_1 FOREIGN KEY (video_id) REFERENCES video (video_id)
                            )   """

            yb_cursor.execute(create_channel_stmt)
            yb_cursor.execute(create_playlist_stmt)
            yb_cursor.execute(create_video_stmt)
            yb_cursor.execute(create_comment_stmt)

            
        yb.commit()
    except Exception as e:
        print("Exception while creating tables")
        print(e)
        exit(1)

    print(">>>> Successfully created table channel.")

#connection_string()

def insert_channel(mydb, mycursor,channel_id,channel_name,channel_views,channel_description,channel_status):
    mycursor.execute(f"select * from channel where channel_id = '{channel_id}'")
    res = mycursor.fetchall()
    if len(res) > 0:
        update_channel(mydb, mycursor,channel_id,channel_name,channel_views,channel_description,channel_status)
        
    else:
        mycursor.execute(f"insert into channel (channel_id,channel_name,channel_views,channel_description,channel_status) values ('{channel_id}','{channel_name}','{channel_views}','{channel_description}','{channel_status}')")
        
    mydb.commit()

def insert_playlist(mydb, mycursor,playlist_id,channel_id,playlist_name):
    mycursor.execute(f"select * from playlist where playlist_id = '{playlist_id}'")
    res = mycursor.fetchall()
    if len(res) > 0:
        update_playlist(mydb, mycursor,playlist_id,channel_id,playlist_name)
        
    else:
        mycursor.execute(f"insert into playlist (playlist_id,channel_id,playlist_name) values ('{playlist_id}','{channel_id}','{playlist_name}')")
        
    mydb.commit()

def insert_video(mydb, mycursor,video_id,playlist_id,video_name,video_description,published_date,view_count,like_count,dislike_count,favorite_count,comment_count,duration,thumbnail,caption_status):
    mycursor.execute(f"select * from video where video_id = '{video_id}'")
    res = mycursor.fetchall()
    if len(res) > 0:
        update_video(mydb, mycursor,video_id,playlist_id,video_name,video_description,published_date,view_count,like_count,dislike_count,favorite_count,comment_count,duration,thumbnail,caption_status)
        
    else:
        mycursor.execute(f"insert into video (video_id,playlist_id,video_name,video_description,published_date,view_count,like_count,dislike_count,favorite_count,comment_count,duration,thumbnail,caption_status) values ('{video_id}','{playlist_id}','{video_name}','{video_description}','{published_date}','{view_count}','{like_count}','{dislike_count}','{favorite_count}','{comment_count}','{duration}','{thumbnail}','{caption_status}')")
        
    mydb.commit()

def insert_comment(mydb, mycursor,comment_id,video_id,comment_text,comment_author,comment_published_date):
    mycursor.execute(f"select * from comment where comment_id = '{comment_id}'")
    res = mycursor.fetchall()
    if len(res) > 0:
        update_comment(mydb, mycursor,comment_id,video_id,comment_text,comment_author,comment_published_date)
        
    else:
        mycursor.execute(f"insert into comment (comment_id,video_id,comment_text,comment_author,comment_published_date) values ('{comment_id}','{video_id}','{comment_text}','{comment_author}','{comment_published_date}')")
        
    mydb.commit()

def update_channel(mydb, mycursor,channel_id,channel_name,channel_views,channel_description,channel_status):
    mycursor.execute(f"update channel set  channel_name = '{channel_name}', channel_views = '{channel_views}',  channel_description = '{channel_description}', channel_status = '{channel_status}'  where channel_id = '{channel_id}'")                      
    mydb.commit()

def update_playlist(mydb, mycursor,playlist_id,channel_id,playlist_name):
    mycursor.execute(f"update playlist set playlist_name = '{playlist_name}' where playlist_id = '{playlist_id}'")
    mydb.commit()

def update_video(mydb, mycursor,video_id,playlist_id,video_name,video_description,published_date,view_count,like_count,dislike_count,favorite_count,comment_count,duration,thumbnail,caption_status):
    mycursor.execute(f"update video set video_name = '{video_name}', video_description = '{video_description}',published_date = '{published_date}' ,view_count = '{view_count}',like_count = '{like_count}', dislike_count = '{dislike_count}',favorite_count = '{favorite_count}',comment_count = '{comment_count}',duration = '{duration}',thumbnail='{thumbnail}',caption_status = '{caption_status}' where video_id = '{video_id}'")
    mydb.commit()

def update_comment(mydb, mycursor,comment_id,video_id,comment_text,comment_author,comment_published_date):
    mycursor.execute(f"update comment set comment_text = '{comment_text}' ,comment_author = '{comment_author}',comment_published_date = '{comment_published_date}' where  comment_id = '{comment_id}'")
    mydb.commit()


def mySQLResult(mydb, query):
    df = pd.read_sql_query(query, con=mydb)
    return df


