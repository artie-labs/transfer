---
description: >-
  We will go over how to gather all the necessary informations to enable
  DynamoDB as a source.
---

# DynamoDB

### Introduction

We will be running [Artie Reader](https://github.com/artie-labs/reader) to fetch the CDC logs from DynamoDB streams.&#x20;

### Finding your DynamoDB settings

* DynamoDB Streams ARN
* AWS Access Key ID
* AWS Secret Access Key

The table and and AWS region can be derived from the Streams ARN.

### Getting DynamoDB Streams ARN

<figure><img src="../.gitbook/assets/image (2) (1) (1).png" alt=""><figcaption></figcaption></figure>

### Generating a service account

Below, you can copy this Terraform script to generate a service account that will have access to DynamoDB streams. Code for this is [available for viewing on GitHub](https://github.com/artie-labs/reader/blob/master/examples/dynamodb/service\_account.tf) as well.

```hcl
provider "aws" {
  region = "us-east-1"
}

resource "aws_iam_role" "dynamodb_streams_role" {
  name = "DynamoDBStreamsRole"
  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Action = "sts:AssumeRole",
        Principal = {
          Service = "ec2.amazonaws.com"
        },
        Effect = "Allow",
        Sid    = ""
      }
    ]
  })
}

resource "aws_iam_policy" "dynamodb_streams_access" {
  name        = "DynamoDBStreamsAccess"
  description = "My policy that grants access to DynamoDB streams"

  policy = jsonencode({
    Version   = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Action = [
          "dynamodb:GetShardIterator",
          "dynamodb:DescribeStream",
          "dynamodb:GetRecords",
          "dynamodb:ListStreams",

          // Stuff only required for export (snapshot)
          "dynamodb:DescribeTable"
        ],
        // Don't want to use "*"? You can specify like this:
        // Resource = [ TABLE_ARN, TABLE_ARN + "/stream/*" ]
        Resource = "*" # Modify this to restrict access to specific streams or resources
      },
      // Export (snapshot) requires access to S3
      {
        "Effect" : "Allow",
        "Action" : [
          "s3:ListBucket"
        ],
        "Resource" : "arn:aws:s3:::artie-transfer-test"
      },
      {
        "Effect" : "Allow",
        "Action" : [
          "s3:GetObject"
        ],
        "Resource" : "arn:aws:s3:::artie-transfer-test/AWSDynamoDB/*"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "dynamodb_streams_role_policy_attachment" {
  role       = aws_iam_role.dynamodb_streams_role.name
  policy_arn = aws_iam_policy.dynamodb_streams_access.arn
}

output "service_role_arn" {
  value = aws_iam_role.dynamodb_streams_role.arn
}

# Create IAM user
resource "aws_iam_user" "dynamodb_streams_user" {
  name = "dynamodb-artie-user"
  path = "/"
}

# Attach policy to IAM user
resource "aws_iam_user_policy_attachment" "user_dynamodb_streams_attachment" {
  user       = aws_iam_user.dynamodb_streams_user.name
  policy_arn = aws_iam_policy.dynamodb_streams_access.arn
}

# Create programmatic access for IAM user
resource "aws_iam_access_key" "dynamodb_streams_user_key" {
  user = aws_iam_user.dynamodb_streams_user.name
}

# Output AWS credentials
output "aws_access_key_id" {
  value     = aws_iam_access_key.dynamodb_streams_user_key.id
  sensitive = true
}

output "aws_secret_access_key" {
  value     = aws_iam_access_key.dynamodb_streams_user_key.secret
  sensitive = true
}
```
