ARG IMAGE_VARIANT=buster
ARG PYTHON_VERSION=3.8.12

FROM python:${PYTHON_VERSION}-${IMAGE_VARIANT} AS py3

RUN apt-get update -y
RUN apt install default-jre -y
RUN apt install openjdk-11-jre-headless -y

RUN pip3 install --upgrade pip
RUN pip3 install pyspark==3.2.0
RUN pip3 install pandas==1.3.4 pytest==6.2.4 jsonargparse==3.19.4

COPY . yum-de
WORKDIR yum-de

RUN pip3 install -e .

