# creates an infrastructure consisting of t2.micro aws instance in specified region
provider "aws" {
  region = "ap-south-1"
}

resource "aws_instance" "ec2_server" {
    ami           = "ami-0f8ca728008ff5af4"
    instance_type = "t2.micro"

    root_block_device {
        volume_size = 10 
    }

}