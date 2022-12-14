FROM python:3.10

RUN mkdir -p /var/lib
RUN mkdir -p /var/lib/data_folder
RUN mkdir -p /var/lib/data_destination
WORKDIR /var/lib

COPY . /var/lib
RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "main.py"]