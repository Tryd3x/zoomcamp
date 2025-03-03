FROM python:3.9

RUN pip install pandas

RUN mkdir /python-files

WORKDIR /python-files

COPY test.py .

ENTRYPOINT ["bash"] 
