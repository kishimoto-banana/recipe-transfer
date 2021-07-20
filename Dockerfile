FROM python:3.9

WORKDIR /app

COPY pyproject.toml poetry.lock ./
RUN pip --disable-pip-version-check --no-cache-dir install poetry==1.1.4 && \
    poetry config virtualenvs.create false && \
    poetry install --no-ansi

COPY Makefile ./
COPY upload_master.py ./
COPY main.py ./
