FROM python:3.10

RUN mkdir -p /var/lib
RUN mkdir -p /var/lib/data_folder
RUN mkdir -p /var/lib/data_destination
WORKDIR /var/lib


CMD ["python", "main.py"]