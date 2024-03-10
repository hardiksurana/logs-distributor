FROM python:3.9

WORKDIR /app

COPY requirements.txt ./
RUN pip install -r requirements.txt

COPY distributor.py app.py ./

EXPOSE 3000

CMD ["python", "app.py"]