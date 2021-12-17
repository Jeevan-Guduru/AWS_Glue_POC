import boto3
from datetime import datetime
from awsglue.utils import getResolvedOptions
import json
import praw
import sys
import pandas as pd
from io import StringIO
import logging
import logging.config


#logger is configured from 'logger.conf' file in the project directory
logging.config.fileConfig(os.path.dirname(__file__)+r"\logger.conf", disable_existing_loggers=False)
logger = logging.getLogger(__name__)


def build_s3_path(prefix: str, filename: str, dt: datetime) -> str:
    """
    Build a path for destination S3 file.

    Parameters
    ----------
    prefix : str
        Prefix of S3 path before the date part.
    filename : str
        Final part of the filename
    dt : datetime.date
        Date used to organize data in "folders" on S3.

    Returns
    -------
    s3_path : str
        Path of a destination file.
    """
    year, month, day, hour = dt.year, dt.month, dt.day, dt.hour
    s3_path = (
        f"{prefix}/year:{year}/month:{month:02}/day:{day:02}/hour:{hour:02}/{filename}"
    )
    return s3_path


def extract() -> None:
    """
    Extracts data from reddit website.
    Since this is a glue job, it is scheduled with trigger,
    Which runs on hourly basis, thus bring data every hour.

    Fetched data is stored in staging bucket
    Data is partitioned with year,month,day,hour

    Parameters
    ----------
    None

    Returns
    -------
    None
    """
    args = getResolvedOptions(
        sys.argv, ["configuration_file_bucket", "configuration_file_key"]
    )
    s3_client = boto3.client("s3")

    # Configuration info is stored in config.json file in s3 bucket.
    # Below fetches the same and stores into variables.

    configuration_file_bucket = args[
        "configuration_file_bucket"
    ]  # "aws-glue-scripts-229313215368-us-west-2"

    configuration_file_key = args["configuration_file_key"]  # "config.json"

    # fetching config.json from s3 bucket.
    config_json_string = (
        s3_client.get_object(
            Bucket=configuration_file_bucket, Key=configuration_file_key
        )["Body"]
        .read()
        .decode("utf-8")
    )

    # forming config json file.
    config_json = json.loads(config_json_string)
    # variables fetched from config file.
    client_id = config_json["client_id"]
    client_secret = config_json["client_secret"]
    user_agent = config_json["user_agent"]
    staging_bucket = config_json["staging_bucket"]
    subreddit_name = config_json["subreddit_name"]

    # creating an empty dictionary to store reddit data
    posts_dict = {
        "id": [],
        "created": [],
        "url": [],
        "selftext": [],
        "upvote_ratio": [],
        "author": [],
        "author_premium": [],
        "over_18": [],
        "treatment_tags": [],
    }

    # Using reddit's PRAW API to fetch data

    reddit = praw.Reddit(
        client_id=client_id, client_secret=client_secret, user_agent=user_agent,
    )

    # fetching the subreddit (Machinelearning is my choice of subreddit defined in config)
    subreddit = reddit.subreddit(subreddit_name)

    # fetching required fields for top 100 hot posts and storing in dict
    for submission in subreddit.hot(limit=100):
        posts_dict["id"].append(submission.id)
        # coverting posix timestamp in 'created' to regular datetime
        posts_dict["created"].append(
            datetime.datetime.utcfromtimestamp(submission.created).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            )
        )
        posts_dict["url"].append(submission.url)
        posts_dict["selftext"].append(submission.selftext)
        posts_dict["upvote_ratio"].append(submission.upvote_ratio)
        posts_dict["author"].append(submission.author)
        posts_dict["author_premium"].append(submission.author_premium)
        posts_dict["over_18"].append(submission.over_18)
        posts_dict["treatment_tags"].append(submission.treatment_tags)
    # converting dict to DF for easy manipulation of data
    hot_posts = pd.DataFrame(posts_dict)

    # formatting text in selftext column as there are too many blank lines in the text.
    # Hence replacing new lines with string (ctlr).
    # replacing ',' with (comma) for easy readability in s3 which will be,
    # reverted during transform and load to DB
    hot_posts.selftext = hot_posts.selftext.str.replace("\n", "(ctlr)")
    hot_posts.selftext = hot_posts.selftext.str.replace(",", "(comma)")

    # forming s3_path based on current datatime as partitions
    s3_path = build_s3_path(
        prefix=f"reddit/{subreddit_name}",
        filename=f"reddit_{subreddit_name}.csv",
        dt=datetime.now(),
    )

    csv_buffer = StringIO()
    # loading df to CSV buffer
    hot_posts.to_csv(csv_buffer, sep="±", index=False)
    s3_resource = boto3.resource("s3")
    # finally loading CSV buffer to staging bucket
    s3_resource.Object(staging_bucket, s3_path).put(Body=csv_buffer.getvalue())


if __name__ == "__main__":
    extract()
