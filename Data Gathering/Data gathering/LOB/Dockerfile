FROM python:3.7

WORKDIR /LOB

COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

COPY . .

CMD [ "python3","./LOB.py"]