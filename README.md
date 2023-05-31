# dwh_eng_test_task

Running mini ETL in PySpark

##### For some reason I couldn't export data to csv, so used persist() method while creating DataFrame, there are similar issues in the stackoverflow
https://stackoverflow.com/questions/45963507/spark-dataframes-are-getting-created-successfully-but-not-able-to-write-into-the

#### Running

1. Install the docker

2. Run below commands:
    docker build . -t sparkhome

3. If you want only run test
    docker run --name spark_container sparkhome /bin/bash -c "cd /opt/spark/ && pytest etl_test.py" 

4. If you want run main script
    docker run --name spark_container sparkhome /bin/bash -c "cd /opt/spark/ && python main.py" 

5. This command will transfer exported file from container to the local directory     
    docker cp spark_container:opt/spark/output/test_transformed test_transformed