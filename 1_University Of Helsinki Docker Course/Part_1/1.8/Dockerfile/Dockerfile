# Start from ubuntu image
FROM ubuntu:18.04

WORKDIR /usr/src/app

COPY code.sh .

RUN apt update && apt install curl -y

CMD ["bash", "code.sh"]
