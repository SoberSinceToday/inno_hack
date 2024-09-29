FROM python:3.11

COPY . .

RUN python -m pip install --upgrade pip
RUN pip install -r ./requirements.txt
CMD ["python", "./main.py"]
