## Dag for daily data update.

Variables:
* max_available_date - max date of data that's already exist.

Steps:
* First of all - check max available date and set it to yesterday if not present.
* Second - parallel generation of terminals and customer tables.
* Third - generation of transaction and frauds.
* Last - saving new partition of data on HDFS (partitioned by date).

Tables generated on each step also saving on HDFS as temporaly artifacts. Dag could be configurated by changing value in `config.py` (by default, you can edit n_customers, n_terminal, output_prefix (for HDFS) and radius).

DAG:

![изображение](https://user-images.githubusercontent.com/64536258/188732460-1bc2aeb2-3d9f-4a37-a815-cc2b125520f1.png)

Variables:

![изображение](https://user-images.githubusercontent.com/64536258/188732498-83b0c045-3fee-43b9-bf34-8dbdd2a7f39b.png)

## Dag for daily feature calculations.

In daily data update added task, that trigger feature preparation dag via TriggerDagRunOperator. Data preparation dag has only one task, that calc features and save them to hdfs partitioned by date.

DAG:

![изображение](https://user-images.githubusercontent.com/64536258/189437637-e52fd396-d359-4fcc-8aee-08d6a18547e8.png)
![изображение](https://user-images.githubusercontent.com/64536258/189437560-402e7343-2936-4d9d-9362-da19d8236f3c.png)

Notebook with feature engineering ![link](https://github.com/slavkostrov/fraud_detection/blob/6d3ec2b1c162b9fd2414d193c84d6c5aa2804795/notebooks/practice_4.ipynb)

## Dag for model train and eval + Mlflow.
Dag runs mlflow experiment, calculates metrics and save model as artifacrts to s3. Examples:

![изображение](https://user-images.githubusercontent.com/64536258/190929109-87fb9814-94bc-42e0-b967-14d818d23a57.png)

![изображение](https://user-images.githubusercontent.com/64536258/190929118-acde74f8-4807-411e-b925-e71f7ae0ad7f.png)

![изображение](https://user-images.githubusercontent.com/64536258/190929120-39c6325c-3be7-4d05-92ef-757681ae97ea.png)

![изображение](https://user-images.githubusercontent.com/64536258/190929124-60d88d9c-aa3e-4f49-9fb8-4b661bf9b37c.png)

![изображение](https://user-images.githubusercontent.com/64536258/190929131-f43a096d-30dc-46e9-93cf-bf9908fb66da.png)




