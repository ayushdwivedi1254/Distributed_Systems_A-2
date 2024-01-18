# Dockerfile
FROM python:3.8-slim

WORKDIR /app

COPY . /app

RUN pip install --trusted-host pypi.python.org flask requests

EXPOSE 5001

CMD ["python", "load_balancer.py"]
