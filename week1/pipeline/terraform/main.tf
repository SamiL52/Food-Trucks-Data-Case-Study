provider "aws" {
    region = "eu-west-2"
}

resource "aws_s3_bucket" "c20-sami-truck-s3" {
    bucket = "c20-sami-truck-s3-bucket"
    force_destroy = true
}

resource "aws_s3_bucket_versioning" "c20-sami-truck-s3-versioning" {
    bucket = aws_s3_bucket.c20-sami-truck-s3.id
    versioning_configuration {
        status = "Disabled"
    }
}
