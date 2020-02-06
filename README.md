# BQin

BQin is a BigQuery data importer with AWS S3 and SQS messaging.  
Respected to http://github.com/fujiwara/Rin  

## Architecture

1. (Someone) creates a S3 object.  
2. [S3 event notifications](https://docs.aws.amazon.com/AmazonS3/latest/dev/NotificationHowTo.html) will send to a message to SQS.  
3. BQin will fetch messages from SQS  
4. BQin copy S3 object to Google Cloud Storage [this is temporary bucket], and create BigQuery Load Job  


