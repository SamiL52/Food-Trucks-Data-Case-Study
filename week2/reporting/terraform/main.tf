provider "aws" {
    region = "eu-west-2"
}

resource "aws_iam_role" "lambda_role" {
    name = "c20-sami-truck-lambda-role"

    assume_role_policy = jsonencode({
        Version = "2012-10-17"
        Statement = [{
            "Effect" = "Allow",
            "Principal" = { Service = "lambda.amazonaws.com" },
            "Action" = "sts:AssumeRole"
        }]
    })
}

resource "aws_iam_role_policy_attachment" "lambda-execution" {
    role = aws_iam_role.lambda_role.name
    policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy_attachment" "athena-access" {
    role = aws_iam_role.lambda_role.name
    policy_arn = "arn:aws:iam::aws:policy/AmazonAthenaFullAccess"
}

resource "aws_iam_role_policy_attachment" "s3-access" {
    role = aws_iam_role.lambda_role.name
    policy_arn = "arn:aws:iam::aws:policy/AmazonS3FullAccess"
}

resource "aws_lambda_function" "c20-sami-truck-lambda" {
    function_name = "c20-sami-truck-lambda"
    package_type = "Image"
    image_uri = "129033205317.dkr.ecr.eu-west-2.amazonaws.com/c20-sami-truck-report-lambda-ecr:latest"
    role = aws_iam_role.lambda_role.arn

    environment {
      variables = var.environment_variables
    }

    timeout = 30
    memory_size = 512
}
