FROM mambaorg/micromamba:2

WORKDIR /app

COPY conda-lock.yml .
RUN micromamba create -y -n app -f conda-lock.yml && \
    micromamba clean --all --yes

COPY . .
LABEL org.opencontainers.image.source=https://github.com/SENERGY-Platform/import-radolan
CMD [ "micromamba", "run", "-n", "app", "python", "-u", "./main.py"]
