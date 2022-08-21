## fraud-detection

Repository for solution of fraud detection problem with modern machine learning algorithms that will provide high quality and best values of buisness/ML metrics.

### Main Purposes:
* Main purpose, as said earlier, is to solve fraud detection problem using ML algorithms.
* So final ML model must answer to all of constraints and acceptance criterias.
* In project must be used modern libraries and technologies related to Data Science subject.

### Metrics of project: 

Accord to problem description, the primary goal to return precise alerts, as investigators might ignore further alerts when too many false alarms are reported. So first metric wille `Precision` of Fraud class, which can be calculated from values of confusion matrix. With this metric we aim to minimize false alarms of our model.

But only with Precision metric our model may be bad. So we introduce second metric - `Recall` of Fraud class. With this metric we aim to find all of Fraud class examples.

To combine first two metrics we can use F measure with beta parametr, which we can use to add weight to any of metrics.

But these metrics are threshold based, so we need to add another metric without dependance from threshold. Two main variants are `ROC AUC` and `PR AUC`. These metrics are based on areas under curves. Axes - combinations of values in confusion matrix. In case of `PR AUC` axes are `Recall` and `Precision`, and each point show them with particular threshold.
