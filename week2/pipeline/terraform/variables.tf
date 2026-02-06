variable "environment_variables" {
    description = "Environment variables for the ecs container"
    type = map(string)
    default = {}
    sensitive = true
}