FROM python:3.9

WORKDIR /app

COPY requirements.txt ./
RUN pip install -r requirements.txt

COPY analyzer.py ./

CMD ["python", "analyzer.py"]