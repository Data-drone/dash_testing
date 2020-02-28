FROM python:3

WORKDIR /usr/src/app

COPY requirements.txt ./

RUN pip install --no-cache-dir -r requirements.txt
#RUN conda install -r requirements.txt

COPY . .

CMD [ "python", "./app.py" ]

EXPOSE 8050