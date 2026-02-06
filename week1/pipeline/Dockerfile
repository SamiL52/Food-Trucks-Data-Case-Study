FROM python:3.13-alpine

COPY requirements.txt .
RUN pip3 install -r requirements.txt

RUN mkdir data

COPY extract.py .
COPY transform.py .
COPY load.py .

CMD python3 extract.py && python3 transform.py && python3 load.py