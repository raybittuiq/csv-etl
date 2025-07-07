# csv-etl



#### Install Dependencies

To install the dependencies, go to the root directory (in csv-etl directory) and run the below command

```bash
pip install -r advanced_infra/requirements.txt
```



#### Execute The Code

To execute the code, run the command as mentioned below from the root directory (in csv-etl directory)

```bash
 python -m advanced_infra.main --input_csv csv_file_path --chunk_size 100 --db_path transaction.db
```

for example if the csv file name is sample.csv and it's location is /home/ubuntu/sample.csv, then run the below command

```bash
python -m advanced_infra.main --input_csv /home/ubuntu/sample.csv
```



#### Run The TestCase

To execute the testcases, run the command as mentioned below from the root directory (in csv-etl directory)

```bash
 pytest advanced_infra/tests.py
```



I have provided a sample csv file (sample_transactions.csv) in the root directory for your reference
