FROM python:3.11-bookworm

COPY /src/requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt

COPY /src /src

WORKDIR /src

CMD ["python", "main.py"]