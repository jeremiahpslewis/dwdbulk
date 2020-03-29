FROM python:3.8-buster

WORKDIR /app

COPY requirements-dev.txt .

RUN pip install --upgrade pip setuptools wheel \
    && pip install --no-cache-dir -r requirements-dev.txt

COPY . /app

RUN python setup.py sdist bdist_wheel \
    && pip install --user --no-cache-dir . \
    && rm -rf dwdbulk/
