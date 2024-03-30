FROM apache/airflow:latest

USER root

RUN apt-get update && \
    apt-get -y install git && \
    apt-get -y install wget && \
    apt-get clean

USER airflow

