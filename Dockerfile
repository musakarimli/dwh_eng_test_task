FROM datamechanics/spark:3.2.1-hadoop-3.3.1-java-8-scala-2.12-python-3.8-dm17

USER root

WORKDIR /opt/spark

RUN pip install --upgrade pip

COPY  requirements.txt .
COPY  input/test.csv .
COPY  main.py .
COPY  etl_test.py .
RUN pip3 install -r requirements.txt

CMD jupyter-lab --allow-root --no-browser --ip=0.0.0.0
