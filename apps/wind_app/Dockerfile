FROM ubuntu:latest

WORKDIR /usr/src/app

RUN apt-get update && apt-get install -y \
    python3-pip

COPY requirements.txt ./

RUN pip3 install --no-cache-dir -r requirements.txt
#RUN conda install -r requirements.txt

COPY . .

ENV IMPALA_HOST='ec2-54-66-248-84.ap-southeast-2.compute.amazonaws.com'
ENV IMPALA_PORT=21050
ENV WEBHDFS_HOST='ec2-54-66-248-84.ap-southeast-2.compute.amazonaws.com'
ENV WEBHDFS_PORT=9870

CMD [ "python3", "./app.py" ]

EXPOSE 8050