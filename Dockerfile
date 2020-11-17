FROM continuumio/miniconda3:4.8.2-alpine

WORKDIR /home/wradlib

ENV PATH /opt/conda/bin:$PATH
RUN conda create -n env python=3.7
RUN echo "source activate env" > ~/.bashrc

COPY conda-requirements.txt .
RUN conda install -c conda-forge -y --file conda-requirements.txt

COPY pip-requirements.txt .
RUN pip install -r pip-requirements.txt

COPY . .
CMD [ "python", "-u", "./main.py"]
