# BQin
[![CircleCI](https://circleci.com/gh/kayac/bqin/tree/master.svg?style=svg)](https://circleci.com/gh/kayac/bqin/tree/master)

BQin is a BigQuery data importer with AWS S3 and SQS messaging.  
Respected to http://github.com/fujiwara/Rin  

## Architecture

1. (Someone) creates a S3 object.  
2. [S3 event notifications](https://docs.aws.amazon.com/AmazonS3/latest/dev/NotificationHowTo.html) will send to a message to SQS.  
3. BQin will fetch messages from SQS  
4. BQin copy S3 object to Google Cloud Storage [this is temporary bucket], and create BigQuery Load Job  

## Configuration

[Configuring Amazon S3 Event Notifications](https://docs.aws.amazon.com/AmazonS3/latest/dev/NotificationHowTo.html).

1. Create SQS queue.
2. Attach SQS access policy to the queue. [Example Walkthrough 1:](https://docs.aws.amazon.com/AmazonS3/latest/dev/ways-to-add-notification-config-to-bucket.html)
3. [Enable Event Notifications](http://docs.aws.amazon.com/AmazonS3/latest/UG/SettingBucketNotifications.html) on a S3 bucket.
4. Create a temporary bucket on Google Cloud Storage and create the target dataset on BigQuery.  
5. Run `bqin` process with configuration for using the SQS and S3.

### config.yaml
```
queue_name: my_queue_name    # SQS queue name

cloud:
  aws:
    region: ap-northeast-1

s3:
  bucket: bqin.bucket.test
  region: ap-northeast-1

big_query:
  project_id: bqin-test
  dataset: test

option:
  temporary_bucket: my_bucket_name # GCP temporary bucket
  gzip: true
  source_format: json # [csv, json, parquet] select able
  auto_detect: true # works only csv or json

# define load rule
rules:
  - big_query: # standard rule
      table: user
    s3:
      key_prefix: data/user

  - big_query:  # expand by key_regexp captured value. for date-sharded tables.
      table: $1_$2
    s3:
      key_regexp: data/(.+)/part-([0-9]+).gz

  - big_query: # override default section in this rule
      project_id: hoge
      dataset: bqin_test
      table: role
    s3:
      bucket: bqin.bucket.test
      key_regexp: data/(.+)/part-([0-9]+).csv
    option:
      gzip: false
      source_format: csv
```

A configuration file is parsed by [kayac/go-config](https://github.com/kayac/go-config).

go-config expands environment variables using syntax `{{ env "FOO" }}` or `{{ must_env "FOO" }}` in a configuration file.

#### Credentials

BQin requires some credentials.
- AWS credentials for access to SQS and S3.  
  Refers to credential information like AWS CLI  
  https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html  

- GCP credentials for access to BigQuery and Cloud Storage  
  Reference using GOOGLE_APPLICATION_CREDENTIALS.  
  https://cloud.google.com/docs/authentication/getting-started?hl=en  

 - alternatively, It can be embedded in the config.
  ```yaml
  cloud:
    aws:
      region: ap-northeast-1
      access_key_id: {{ must_env "ACCESSS_KEY_ID" }}
      secret_access_key: {{ must_env "SECRET_ACCESS_KEY" }}
    gcp:
      base64_credential: {{ must_env "GCP_CREDENTIAL_BASE64_JSON" }}
   ```
   Note: For GCP credentials, specify a Base64-encoded string of the contents of the JSON file

## Run

### normally

BQin waits new SQS messages and processing it continually.

```
$ bqin run -config config.yaml [-debug]
```

### batch

BQin receive SQS messages and processing. exit when all messages in the queue have been read.

```
$ bqin batch -config config.yaml -queue <dlq-queue-name> [-debug]
```

## Check Rule

```
$ echo "s3://bucket.example.com/object.txt" | bqin check -config config.yaml
```

# LICENCE  

MIT  

# Author  

KAYAC Inc.  
