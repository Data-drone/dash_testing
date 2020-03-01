FROM ubuntu:latest

WORKDIR /usr/src/app

RUN apt-get update && apt-get install -y \
    python3-pip

COPY requirements.txt ./

RUN pip3 install --no-cache-dir -r requirements.txt
#RUN conda install -r requirements.txt

COPY . .

CMD [ "python3", "./app.py" ]

EXPOSE 8050