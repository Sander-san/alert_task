FROM python:3.8

RUN mkdir -p /usr/src/app/
WORKDIR /usr/src/app/

COPY ./data /usr/src/app/
COPY ./script.py /usr/src/app/
COPY ./requirements.txt /usr/src/app/

RUN pip install --no-cashe-dir -r requirements.txt

CMD ["python", "script.py"]
