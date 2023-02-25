FROM python:3.9-alpine
RUN apk --no-cache add g++ wget
RUN pip install pandas sqlalchemy psycopg2-binary
WORKDIR /app
COPY data-ingest.py data-ingest.py
ENTRYPOINT [ "python","data-ingest.py"]