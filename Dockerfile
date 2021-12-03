FROM continuumio/miniconda3:4.9.2-alpine

WORKDIR /home/anaconda

ENV PATH /opt/conda/bin:$PATH
RUN conda create -n env python=3.9
RUN echo "source activate env" > ~/.bashrc

COPY conda-requirements.txt .
RUN conda install -c conda-forge -y --file conda-requirements.txt

COPY pip-requirements.txt .
RUN pip install -r pip-requirements.txt

COPY . .
LABEL org.opencontainers.image.source https://github.com/SENERGY-Platform/import-radolan
CMD [ "python", "-u", "./main.py"]
