import json
from twarc import Twarc
from neo4j.v1 import GraphDatabase

### NEED TO INSTALL ###
# pip install twarc
# pip install neo4j-driver


# **Authentication:**
uri = "bolt://127.0.0.1:7687"
driver = GraphDatabase.driver(uri, auth=("ne04j_username", "neo4j_password"))

consumer_key = ''
consumer_secret = ''
access_token = ''
access_token_secret = ''

t = Twarc(consumer_key, consumer_secret, access_token, access_token_secret)


# **Uniqueness Constraints:**
session = driver.session()

# Add uniqueness constraints.
session.run( "CREATE CONSTRAINT ON (t:Tweet) ASSERT t.id IS UNIQUE;")
session.run( "CREATE CONSTRAINT ON (u:User) ASSERT u.screen_name IS UNIQUE;")
session.run( "CREATE CONSTRAINT ON (h:Hashtag) ASSERT h.name IS UNIQUE;")
session.run( "CREATE CONSTRAINT ON (l:Link) ASSERT l.url IS UNIQUE;")
session.run( "CREATE CONSTRAINT ON (s:Source) ASSERT s.name IS UNIQUE;")


# **Twitter Follow List:**
list = '16796735,820954235357593602'


for tweets in t.filter(follow=list):

    try:
        query = '''
        UNWIND {tweets} AS t
        WITH t,
             t.entities AS e,
             t.user AS u,
             t.retweeted_status AS retweet
        WHERE t.id is not null
        MERGE (tweet:Tweet {id:t.id_str})
        SET tweet.text = t.text,
            tweet.created = t.created_at,
            tweet.favorites = t.favorite_count
        MERGE (user:User {screen_name:u.screen_name})
        SET user.name = u.name,
            user.id = u.id_str,
            user.location = u.location,
            user.followers = u.followers_count,
            user.following = u.friends_count,
            user.statuses = u.statuses_count,
            user.profile_image_url = u.profile_image_url
        MERGE (user)-[:POSTS]->(tweet)
        FOREACH (h IN e.hashtags |
          MERGE (tag:Hashtag {name:LOWER(h.text)})
          MERGE (tag)<-[:TAGS]-(tweet)
        )
        FOREACH (u IN [u IN e.urls WHERE u.expanded_url IS NOT NULL] |
          MERGE (url:Link {url:u.expanded_url})
          MERGE (tweet)-[:CONTAINS]->(url)
        )
        FOREACH (m IN e.user_mentions |
          MERGE (mentioned:User {screen_name:m.screen_name})
          ON CREATE SET mentioned.name = m.name,
                        mentioned.id = m.id_str
          MERGE (tweet)-[:MENTIONS]->(mentioned)
        )
        FOREACH (r IN [r IN [t.in_reply_to_status_id_str] WHERE r IS NOT NULL] |
          MERGE (reply_tweet:Tweet {id:r})
          SET reply_tweet.name = t.in_reply_to_screen_name
          MERGE (tweet)-[:REPLIES_TO]->(reply_tweet)
        )
        FOREACH (retweet_id IN [x IN [retweet.id_str] WHERE x IS NOT NULL] |
            MERGE (retweet_tweet:Tweet {id:retweet_id})
            SET retweet_tweet.created = retweet.created_at
            MERGE (tweet)-[:RETWEETS]->(retweet_tweet)
        )
        '''

        # Send Cypher query.
        session.run(query,{'tweets':tweets})
        print("Tweets added to graph!")
        #print(query)

    except Exception as e:
        print(e)
        continue
