FROM nickgryg/alpine-pandas:latest

RUN mkdir -p /home

WORKDIR /home

RUN apk update && \
    apk add --virtual build-deps gcc python-dev musl-dev && \
    apk add postgresql-dev

ADD requirements.txt ./requirements.txt
RUN python -m pip install pip

RUN python -m pip install --upgrade pip
# Installing packages
RUN pip install -r ./requirements.txt

# Copying over necessary files
COPY src ./src
COPY settings.py ./settings.py
COPY summarizer.py ./app.py
RUN mkdir /tmp/logs
RUN touch /tmp/logs/summarizer.log

CMD tail -f /tmp/logs/summarizer.log
