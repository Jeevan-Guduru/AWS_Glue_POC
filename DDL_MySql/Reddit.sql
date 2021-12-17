--- Two tables are created, one is for posts and other to accomodate tags list data.
--- ETL jobs take care of the relationship between tags and posts.

create table hot_posts(id VARCHAR(40),
    created DATETIME,
    url varchar(1000),
    selftext mediumtext,
    upvote_ratio float,
    author varchar(200),
    author_premium boolean,
    over_18 boolean,
    last_modified DATETIME);
   
 
create table posts_treatment_tags(id VARCHAR(40),
    treatment_tags VARCHAR(600));
