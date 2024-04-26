import os
from typing import List
import praw 
from praw.models import Submission
from praw.models import MoreComments
from praw.models import Comment
from  praw.models.comment_forest import CommentForest

SUBREDDIT = "formula1"


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




def main():
    all_posts = []
    
    submissions = get_data(SUBREDDIT, 10)
    
    for submission in submissions:
        post = {
            "id": submission.id,
            "title": submission.title,
            "author": submission.author,
            "comments": get_all_comments(submission),
            "url": submission.url
        }
        all_posts.append(post)
    

    titles = [post["title"] for post in all_posts]
    for title in titles:
        print(title)
        
        
def write_to_db(posts):
    pass

main()



