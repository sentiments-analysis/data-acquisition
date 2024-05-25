import os
from typing import Dict, List
import praw 
from praw.models import Submission
from praw.models import MoreComments
from praw.models import Comment
from  praw.models.comment_forest import CommentForest
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from flask import Request, jsonify
import time
from datetime import datetime as date
import logging

user_agent = "Scrapper 1.0 by /u/Reasonable-Fold-8491"


client_id = os.environ["client_id"]
client_secret = os.environ["client_secret"]

reddit = praw.Reddit(
    client_id=client_id,
    client_secret=client_secret,
    user_agent=user_agent
)

def get_data(subreddit: str, num_results: int):
    headlines = []
    for submission in reddit.subreddit(subreddit).hot(limit=num_results):
        headlines.append(submission)
    print(headlines)
    return headlines

def extract_all_replies(comment: Comment, acc: List[str]) -> List[str]:
    replies: CommentForest | List[Comment] = [] 
    
    
    if isinstance(comment, Comment):
        acc.append(comment.body) 
        replies = comment.replies
        
    if isinstance(comment, MoreComments):
        replies = comment.comments()
            
 
    for reply in replies:
        extract_all_replies(reply, acc)
    return acc
   

def get_all_comments(post: Submission) -> List[str]:
    all_comments = []
    for comment in post.comments:
        extract_all_replies(comment, all_comments)
    return all_comments



def data_acquisition(request):

    request_json = request.get_json()
    subreddits = request_json["subreddits"]
    num_posts = request_json["num_posts"]

    all_posts = []
    for subreddit in subreddits:
        try:
            submissions = get_data(subreddit, num_posts)
        except Exception as e:
            logging.error(f"Error retrieving data from {subreddit}: {e}")
            continue

        for submission in submissions:
            try:
                post = {
                    "reddit": subreddit,
                    "id": submission.id,
                    "title": submission.title,
                    "author_id": submission.author.id,
                    "comments": get_all_comments(submission),
                    "url": submission.url, 
                    "date" : date.now().isoformat()
                }
                print(f"The post {submission.title} has been retrieved")
                all_posts.append(post)
            except Exception as e:
                logging.error(f"Error processing submission {submission.id}: {e}")

            time.sleep(1)  

    
    write_to_db(all_posts)
    return jsonify({"status": "success", "message": "Data acquired and written to BigQuery"}), 200
    
            
        
def write_to_db(posts: List[Dict]):
    client = bigquery.Client()
    dataset_id = 'data'
    table_id = 'posts'
    full_table_id = f"{client.project}.{dataset_id}.{table_id}"
    schema = [
    bigquery.SchemaField("reddit", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("title", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("author_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("comments",  "STRING", mode="REPEATED"),
    bigquery.SchemaField("url", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("date", "TIMESTAMP", mode="REQUIRED")]
   
    table = bigquery.Table(full_table_id, schema=schema)

    try:
        client.get_table(table)  
        print(f"Table {table_id} already exists.")
    except NotFound:
        table = client.create_table(table)  
        print(f"Created table {table_id}.")

    errors = client.insert_rows_json(table, posts)
    if errors == []:
        print("New rows have been added.")
    else:
        print("Errors occurred while inserting rows: ", errors)
    
    
