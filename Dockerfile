# FROM python:3.7-buster
FROM wradlib/wradlib-docker:master-min

WORKDIR /home/wradlib

COPY requirements.txt .
RUN conda install -y --file requirements.txt

COPY . .
ENTRYPOINT ["./entrypoint.sh"]
CMD [ "python", "-u", "./main.py"]
