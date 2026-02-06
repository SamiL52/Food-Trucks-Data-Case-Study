provider "aws" {
    region = "eu-west-2"
}

data "aws_iam_role" "ecs_task_execution_role" {
    name = "ecsTaskExecutionRole"
}

resource "aws_ecs_task_definition" "c20-sami-truck-etl-td" {
    family = "c20-sami-truck-td-family"
    network_mode = "awsvpc"
    requires_compatibilities = ["FARGATE"]
    cpu = "256"
    memory = "512"
    execution_role_arn = data.aws_iam_role.ecs_task_execution_role.arn
    task_role_arn = data.aws_iam_role.ecs_task_execution_role.arn

    container_definitions = jsonencode([
        {
            name = "c20-sami-truck-etl-ecr"
            image = "129033205317.dkr.ecr.eu-west-2.amazonaws.com/c20-sami-truck-etl-ecr:latest"
            essential = true

            environment = [
                for key, value in var.environment_variables : {
                    name = key
                    value = value
                }
            ]
        }
    ])
}