## fraud-detection

Repository for solution of fraud detection problem with modern machine learning algorithms that will provide high quality and best values of business/ML metrics.

### Main Purposes:
* Main purpose, as said earlier, is to solve fraud detection problem using ML algorithms.
* So final ML model must answer to all of constraints and acceptance criterias.
* In project must be used modern libraries and technologies related to Data Science subject.

### Metrics of project: 

Accord to problem description, the primary goal to return precise alerts, as investigators might ignore further alerts when too many false alarms are reported. So first metric wille `Precision` of Fraud class, which can be calculated from values of confusion matrix. With this metric we aim to minimize false alarms of our model.

But only with Precision metric our model may be bad. So we introduce second metric - `Recall` of Fraud class. With this metric we aim to find all of Fraud class examples.

To combine first two metrics we can use F measure with beta parametr, which we can use to add weight to any of metrics.

But these metrics are threshold based, so we need to add another metric without dependance from threshold. Two main variants are `ROC AUC` and `PR AUC`. These metrics are based on areas under curves. Axes - combinations of values in confusion matrix. In case of `PR AUC` axes are `Recall` and `Precision`, and each point show them with particular threshold.

### Tasks (S.M.A.R.T.)

###### Pre-modeling part

* Explore articles on fraud detection topic to understand which solution is better than others and prepare presentation for investors of project (4 days);
* Explore data that we already have and data that we need to have for reach expected result (1 week);
* Understand who we need and recruit specialists for out project (2 weeks).

###### Data preparing part

* Prepare cluster for parallel data processing, for faster response (2 weeks);
* Set up process of continuous and efficient data acquisition for fast response (3 weeks).

##### Modeling

* Select validation strategy that will swill display real world metrics (1 week);
* Select and prepare most import features from our data for robust ML model (2 weeks);
* Learn baseline model on selected features for start point of model's quality (1 week);
* Improve baseline model for reaching our metric goals (4 weeks). 

##### MLOps

* Define format of model answer which suits everyone (1 weeks);
* Carry out load testing of the model in order to understand that the model is ready for production (2 week);
