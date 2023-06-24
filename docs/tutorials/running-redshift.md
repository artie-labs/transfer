# \[WIP] Running Redshift

## Pre-requisites:

1. Redshift account
2. S3 bucket

### Setting up S3 bucket

We'll need an S3 bucket so that Artie Transfer can load micro-batches of TSVs such that we are able to invoke the remote `COPY command` for Redshift.

After the MERGE, we will no longer need the files on S3, so it's generally recommended to set up a Lifecycle rule in S3 to have these files be automatically purged.

Here's an example of what the bucket and lifecycle configuration can look like in Terraform.

```hcl
resource "aws_s3_bucket" "transfer_s3_redshift_bucket" {
  bucket = "artie-redshift-logs"  # Set your desired bucket name
}

resource "aws_s3_bucket_lifecycle_configuration" "my_bucket_lifecycle" {
  rule {
    id     = "DeleteOldObjects"
    status = "Enabled"

    expiration {
      # Set however long you'd like it to be.
      days = 1 
    }
  }

  bucket = aws_s3_bucket.transfer_s3_redshift_bucket.id
}
```

