# FROM python:3.7-buster
FROM wradlib/wradlib-docker:master-min

WORKDIR /home/wradlib

COPY conda-requirements.txt .
RUN conda install -y --file conda-requirements.txt

COPY pip-requirements.txt .
RUN pip install -r pip-requirements.txt

COPY . .
ENTRYPOINT ["./entrypoint.sh"]
CMD [ "python", "-u", "./main.py"]
