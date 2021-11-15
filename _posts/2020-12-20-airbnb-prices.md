---
layout: post
title: Traveling in the 21st Century
subtitle: Predicting the Price of an Airbnb with Math and Python
author: Jacob Torres
gh-repo: jacob-torres/predictive-modeling-airbnb-prices/blob/main/airbnb_ds_u2.ipynb
gh-badge: [follow]
tags: [data-science, data-analysis, travel, airbnb, housing, housing-price, machine-learning, predictive-modeling]
comments: true
---

When traveling around the United States, an increasing number of people are looking to the popular service [Airbnb](https://airbnb.com/) as an alternative to more traditional options like hotels. There are several main reasons for this trend, including the flexibility of location, housing type, and affordability. It's important for a person interested in listing their home for rent on Airbnb to understand what determines a fair price.

The following dataset was found on [Kaggle's database,](https://kaggle.com/datasets/) and comprises nearly a quarter of a million Airbnb listings around the United States in 2020.

### Tools

I programmed this project in Python 3.8. Some of the key tools I used are:

* Jupyter notebook, Pandas, Numpy, and MatPlotLib
* Category_encoders:
    - [One-Hot encoder](https://contrib.scikit-learn.org/category_encoders/onehot.html)
    - [OrdinalEncoder](https://contrib.scikit-learn.org/category_encoders/ordinal.html)
* SciKit-Learn:
    - [SimpleImputer](https://scikit-learn.org/stable/modules/generated/sklearn.impute.SimpleImputer.html)
    - [StandardScaler](https://scikit-learn.org/stable/modules/generated/sklearn.preprocessing.StandardScaler.html)
    - [LinearRegression](https://scikit-learn.org/stable/modules/generated/sklearn.linear_model.LinearRegression.html)
* XGBoost:
    - [XGBoost Regressor](https://xgboost.readthedocs.io/en/latest/python/python_api.html)
* Tensorflow:
    - [Keras Sequential Model](https://keras.io/api/models/sequential/)

---

## Exploratory Data Analysis

Let's take a look at the data before determining a hypothesis or model:
 
 
| |id|name|host_id|host_name|neighbourhood_group|neighbourhood|latitude|longitude|room_type|price|minimum_nights|number_of_reviews|last_review|reviews_per_month|calculated_host_listings_count|availability_365|city|
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|0|38585|Charming Victorian home - twin beds + breakfast|165529|Evelyne|NaN|28804|35.65146|-82.62792|Private room|60|1|138|16/02/20|1.14|1|0|Asheville|
|1|80905|French Chic Loft|427027|Celeste|NaN|28801|35.59779|-82.55540|Entire home/apt|470|1|114|07/09/20|1.03|11|288|Asheville|
|2|108061|Walk to stores/parks/downtown. Fenced yard/Pet...|320564|Lisa|NaN|28801|35.60670|-82.55563|Entire home/apt|75|30|89|30/11/190.81|2|298|Asheville|
|3|155305|Cottage! BonPaul + Sharky's Hostel|746673|BonPaul|NaN|28806|35.57864|-82.59578|Entire home/apt|90|1|267|22/09/20|2.39|5|0|Asheville|
|4|160594|Historic Grove Park|769252|Elizabeth|NaN|28801|35.61442|-82.54127|Private room|125|30|58|19/10/15|0.52|1|0|Asheville|

The dataset has 226,030 observations, and 17 features.

To get a sense of the overall trends in the data, or if there are outliers or other anomalies, I use Pandas' dataframe "describe" function to get a table of basic stats:

 
| |id|host_id|latitude|longitude|price|minimum_nights|number_of_reviews|reviews_per_month|calculated_host_listings_count|availability_365|
|---|---|---|---|---|---|---|---|---|---|---|
|count|2.260300e+05|2.260300e+05|226030.000000|226030.000000|226030.000000|2.260300e+05|226030.000000|177428.00000|226030.000000|226030.000000|
|mean|2.547176e+07|9.352385e+07|35.662829|-103.220662|219.716529|4.525490e+02|34.506530|1.43145|16.698562|159.314856|
|std|1.317814e+07|9.827422e+07|6.849855|26.222091|570.353609|2.103376e+05|63.602914|1.68321|51.068966|140.179628|
|min|1.090000e+02|2.300000e+01|18.920990|-159.714900|0.000000|1.000000e+00|0.000000|0.01000|1.000000|0.000000|
|25%|1.515890e+07|1.399275e+07|32.761783|-118.598115|75.000000|1.000000e+00|1.000000|0.23000|1.000000|0.000000|
|50%|2.590916e+07|5.138266e+07|37.261125|-97.817200|121.000000|2.000000e+00|8.000000|0.81000|2.000000|140.000000|
|75%|3.772624e+07|1.497179e+08|40.724038|-76.919322|201.000000|7.000000e+00|39.000000|2.06000|6.000000|311.000000|
|max|4.556085e+07|3.679176e+08|47.734620|-70.995950|24999.000000|1.000000e+08|966.000000|44.0600|593.000000|365.000000|

The table above shows that the data has at least one outlier noticeable at a glance: the maximum value of "minimum_nights" is 100 trillion. Clearly this is either a mistake or a joke. Regardless, I removed the listing with the outlier from this dataset to decrease the skew.

## Data Wrangling

I wrote a function to wrangle the dataset into a clean, pre-processed feature matrix and target vector. This is where SKLearn especially came in handy; in order to get this table into a format that the model will be able to digest, it needs to be transformed into scaled numeric values.

In short: I wrote code to turn this beautiful table into some ugly arrays of numbers that the computer can understand. I'll be using this transformed data to fit and train my models.

---

## Data Modeling

### Linear Model: Linear Regression

Our goal is to build a predictive model using Python which most accurately predicts the price of an airbnb. This is a regression problem, meaning the variable we're targeting (price) could be an infinite number of values. First, we'll fitt a standard linear regression model from Scikit-Learn, without expecting a super accurate result.

A linear regression model uses all the algebraic mind gymnastics we learned in high school, like slope and y-intercept, to fit a line to the input data (features of the airbnb listings.) It then applies the equation of that line to predict the price of future listings.

We'll test this model using two accuracy scoring methods:

* R-squared score (R2): Measures the proportion of the predicted outcome whose variance was accurately replicated by the linear model.
* Mean-squared error (MSE): Measures the average of the squared differences between the predicted and actual values.

Here are the results when we fit and test a basic LinearRegression model:

 
>r2 score = 0.05261821654425727
>MSE = -92099.40925130853


For context, these are horrific scores!

The R2 score is a value between 0 and 1, representing the percentage of variance that was accurately predicted by the model. An R2 score of 0.05 means that the model was about 5% accurate ... so not very.

Since we scored the model using a parameter of the validation function, rather than through the MSE package from SKLearn, the MSE is represented here as a negative number. This is a quirk of the scorer that is used. The MSE represents the average squared distance between the predicted and actual values, and thus must be a non-negative number.

The above MSE indicates that the squared variance in predicted and actual data sums up to about 92 thousand, which represents quite a large gap between what the model "thought" the price was and the actual price of the listings.

The linear regression is often called the "Hello World" of machine learning models, meaning it's the model that is the most basic and simple to explain to beginners. It doesn't have many parameters to tune, and it seems likely that the data would be better suited to a different algorithm.

---

### Tree-Based Model: XGBoost Regressor

A tree-based model, as opposed to a linear model, uses a decision tree as its base strategy. The XGBoost regressor is an ensemble model which boosts the correct decisions of each previous estimator to optimize its final decision.

When we fit the XGBoost regressor to the data, our accuracy increases slightly:


>r2 score = 0.15548775785353622


15% accuracy is certainly an improvement from 5%, but it's still not great. It's time to bring in the big guns.

---

### Neural Network: Keras Sequential Model

In short, a neural network is a machine-learning model which has multiple "hidden" layers through which the data is transformed before the final output is computed. A hidden layer can perform any number of computations to improve the given metric. There are many neural network architectures, but perhaps the simplest (and the one we'll be using on this data) is the feed-forward model. In this model, data flows predictably from the input layer, through each successive layer until the output layer.

Depending on the results of the computations in each neuron of a layer, the output may or may not contribute to the final output. This is the process of determining whether or not a neuron will "fire." The ["sigmoid" function](https://towardsdatascience.com/sigmoid-neuron-deep-neural-networks-a4cd35b629d7) is an example of a common activation function which determines whether or not a hidden layer passes its output data to the next layer of the network.

Putting the Airbnb data through a Keras neural network is a last-ditch effort to get more accurate results from the model. All 226,030 entries and 17 features can be synthesized more vigorously than is possible with a traditional method.

We'll be evaluating loss as the metric for this model. Loss is the penalty for incorrect predictions, expressed by the sum of all error. If the prediction is perfect, the loss will equal 0. The larger the loss, the less accurate the results are. For this test, we'll be using the mean absolute error (MAE) metric as the loss function.

* Mean absolute error (MAE): The magnitude of difference between the expected result and the prediction. This metric indicates the amount of error in the prediction, and is equal to 0 if the actual result was the same as the prediction. Again, the larger this value, the less accurate the results.
    - Note: The mean squared error metric (MSE,) used earlier in this post, is an exaggerated version of the MAE. By squaring the differences between the actual and predicted values, the MSE gives a more obvious picture of the inaccuracy of the results.

For this test, we must set a baseline output against which to compare the prediction results. Using the MAE metric, we'll determine whether or not the model is performing well by comparing the predicted error with the final error computed by the model.

Because our target variable is the price of an Airbnb rental, we'll find the average price in the dataset. The average (mean) price is only the predicted value though, or in other words, it is the value against which we want to compare all other price values. Therefore, the metric of mean absolute error is the mean of the predicted price value subtracted from each entry's price.

Here are the results of that computation:


>Mean Absolute Error for Baseline is 1368.8777720114826


This value is the baseline MAE, and will be used after fitting the model to determine how accurate the results are.

After constructing a standard feed-forward neural network with five hidden layers (not including the output layer,) it's time to fit the data to the model. The results come in the form of loss and mean absolute error values for each epoch. (And epoch being a single pass of the model over the data.)

The four epochs which were run on the data have produced the following output:


>    Epoch 1/4: 80723/80723 [==============================] - 72s 885us/step - loss: 614.7093 - mean_absolute_error: 614.7093 - val_loss: 568.1913 - val_mean_absolute_error: 568.191
>    Epoch 2/4: 80723/80723 [==============================] - 70s 868us/step - loss: 570.3614 - mean_absolute_error: 570.3614 - val_loss: 573.1775 - val_mean_absolute_error: 573.177
>    Epoch 3/4: 80723/80723 [==============================] - 71s 882us/step - loss: 555.7084 - mean_absolute_error: 555.7084 - val_loss: 533.4962 - val_mean_absolute_error: 533.496 - ETA: 32s - loss: 562.5844 - mean_absolute_error: 562.58
>    Epoch 4/4: 80723/80723 [==============================] - 72s 889us/step - loss: 546.1714 - mean_absolute_error: 546.1714 - val_loss: 517.6982 - val_mean_absolute_error: 517.69


This demonstrates a distinct improvement in the training of the model; the initial predicted error was much larger than the error of the final epoch:


>The baseline Mean Absolute Error calculated initially was : 1368.8777720114826
>The Model's Mean Absolute Error calculated initially is : 517.6982421875
>The improvement demonstrated by model using Mean Absolute Error as metrics is : 62.0


---

## Conclusions

The above modeling is meant to demonstrate a number of training and evaluation techniques on a real-world dataset. It is clear that all dimensions of the data can be used in a neural network to determine an accurate price for any given Airbnb listing, with reasonably low error.

To experience first-hand what this model can do, check it out [here](https://ptptairbnb2.herokuapp.com/).

Note: The webpage takes about a half a minute to load; it's a hefty model! Feel free to imagine you are renting out your place on Airbnb, and would like to find out the ideal price. Maybe you are. Enter in your details, and have fun!
