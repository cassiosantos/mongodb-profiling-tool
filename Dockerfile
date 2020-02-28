FROM python:2.7-alpine

WORKDIR /usr/src/app

COPY profile.py .


RUN pip install pymongo


ENTRYPOINT ["python","profile.py"]
