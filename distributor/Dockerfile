FROM python:3.9

WORKDIR /app

COPY requirements.txt ./
RUN pip install -r requirements.txt

COPY distributor.py app.py ./

EXPOSE 3000

# CMD ["gunicorn", "app:app", "--worker-class", "gevent", "-w", "4", "-b", "0.0.0.0:3000"]
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "3000"]
