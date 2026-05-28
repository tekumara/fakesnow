FROM python:3.10-slim

WORKDIR /app

COPY . .

RUN pip install --no-cache-dir ".[server]"

EXPOSE 8080

CMD ["fakesnow", "-s", "--host", "0.0.0.0", "--port", "8080"]
