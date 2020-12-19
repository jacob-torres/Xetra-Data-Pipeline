---
layout: post
title: 21st Century Traveling
subtitle: Predicting the Price of an Airbnb with Math and Python
author: Jacob A. Torres
gh-repo: jacob-torres/predictive-modeling-airbnb-prices/blob/main/airbnb_ds_u2.ipynb
gh-badge: [follow]
tags: [data-science, data-analysis, travel, airbnb, housing, housing-price, machine-learning, predictive-modeling]
comments: true
---

When traveling around the United States, an increasing number of people are looking to the popular service Airbnb as an alternative to more traditional options like hotels. There are several main reasons for this trend, including the flexibility of location, housing type, and affordability.
It's important for the modern traveler to understand what determines the price of an airbnb. A good question to ask might be "Does the minimum-night limit of a listing predict the cost?" Or maybe "What are the characteristics that affordable listings have in common?"
The following dataset was found on Kaggle's database, and comprises nearly a quarter of a million Airbnb listings around the United States in 2020. The primary question I plan to ask of this dataset is: "What are the best predicters of the Price of an Airbnb in the U.S.?"
Let's take a look at the data before determining a hypothesis or model.
 
Exploratory Data Analysis
In [1]:
%matplotlib inline
import warnings
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import plotly.express as px
import eli5
from pandas_profiling import ProfileReport
from category_encoders.ordinal import OrdinalEncoder
from xgboost import XGBRegressor
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.feature_selection import SelectKBest, f_regression, chi2
from sklearn.inspection import permutation_importance
from sklearn.pipeline import make_pipeline
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import StandardScaler
from sklearn.impute import SimpleImputer
from sklearn.metrics import r2_score, mean_squared_error, roc_auc_score

# Ignore ugly warning messages
warnings.filterwarnings('ignore')
c:\program files\python38\lib\site-packages\sklearn\utils\deprecation.py:143: FutureWarning: The sklearn.metrics.scorer module is  deprecated in version 0.22 and will be removed in version 0.24. The corresponding classes / functions should instead be imported from sklearn.metrics. Anything that cannot be imported from sklearn.metrics is now part of the private API.
  warnings.warn(message, FutureWarning)
c:\program files\python38\lib\site-packages\sklearn\utils\deprecation.py:143: FutureWarning: The sklearn.feature_selection.base module is  deprecated in version 0.22 and will be removed in version 0.24. The corresponding classes / functions should instead be imported from sklearn.feature_selection. Anything that cannot be imported from sklearn.feature_selection is now part of the private API.
  warnings.warn(message, FutureWarning)
In [2]:
data_url = './data/ab_us_2020.csv'
df = pd.read_csv(data_url)

print(f"Dataset: {df.shape}")
df.head()
Dataset: (226030, 17)
Out[2]:
 
id
name
host_id
host_name
neighbourhood_group
neighbourhood
latitude
longitude
room_type
price
minimum_nights
number_of_reviews
last_review
reviews_per_month
calculated_host_listings_count
availability_365
city
0
38585
Charming Victorian home - twin beds + breakfast
165529
Evelyne
NaN
28804
35.65146
-82.62792
Private room
60
1
138
16/02/20
1.14
1
0
Asheville
1
80905
French Chic Loft
427027
Celeste
NaN
28801
35.59779
-82.55540
Entire home/apt
470
1
114
07/09/20
1.03
11
288
Asheville
2
108061
Walk to stores/parks/downtown. Fenced yard/Pet...
320564
Lisa
NaN
28801
35.60670
-82.55563
Entire home/apt
75
30
89
30/11/19
0.81
2
298
Asheville
3
155305
Cottage! BonPaul + Sharky's Hostel
746673
BonPaul
NaN
28806
35.57864
-82.59578
Entire home/apt
90
1
267
22/09/20
2.39
5
0
Asheville
4
160594
Historic Grove Park
769252
Elizabeth
NaN
28801
35.61442
-82.54127
Private room
125
30
58
19/10/15
0.52
1
0
Asheville
In [3]:
ProfileReport(df)
HBox(children=(FloatProgress(value=0.0, description='Summarize dataset', max=31.0, style=ProgressStyle(descrip…
HBox(children=(FloatProgress(value=0.0, description='Generate report structure', max=1.0, style=ProgressStyle(…
HBox(children=(FloatProgress(value=0.0, description='Render HTML', max=1.0, style=ProgressStyle(description_wi…
Out[3]:
In [4]:
df.describe()
Out[4]:
 
id
host_id
latitude
longitude
price
minimum_nights
number_of_reviews
reviews_per_month
calculated_host_listings_count
availability_365
count
2.260300e+05
2.260300e+05
226030.000000
226030.000000
226030.000000
2.260300e+05
226030.000000
177428.00000
226030.000000
226030.000000
mean
2.547176e+07
9.352385e+07
35.662829
-103.220662
219.716529
4.525490e+02
34.506530
1.43145
16.698562
159.314856
std
1.317814e+07
9.827422e+07
6.849855
26.222091
570.353609
2.103376e+05
63.602914
1.68321
51.068966
140.179628
min
1.090000e+02
2.300000e+01
18.920990
-159.714900
0.000000
1.000000e+00
0.000000
0.01000
1.000000
0.000000
25%
1.515890e+07
1.399275e+07
32.761783
-118.598115
75.000000
1.000000e+00
1.000000
0.23000
1.000000
0.000000
50%
2.590916e+07
5.138266e+07
37.261125
-97.817200
121.000000
2.000000e+00
8.000000
0.81000
2.000000
140.000000
75%
3.772624e+07
1.497179e+08
40.724038
-76.919322
201.000000
7.000000e+00
39.000000
2.06000
6.000000
311.000000
max
4.556085e+07
3.679176e+08
47.734620
-70.995950
24999.000000
1.000000e+08
966.000000
44.06000
593.000000
365.000000
 
Data Wrangling
In [5]:
# Data wrangling function
def wrangle(df):
  df = df.copy()

  # Drop irrelevant features
  df = df.drop(
    columns=['id', 'name', 'host_name', 'neighbourhood_group']
  )

  # Convert last_review to datetime,
  # and replace it with only the month
  df['last_review'] = pd.to_datetime(
    df['last_review'], infer_datetime_format=True
  )
  df['last_review'] = df['last_review'].dt.month

  # Split data iinto feature and target matrices
  target = 'price'
  X = df.drop(target, axis=1)
  y = df[target]

  # Build a preprocessing pipeline to prep features for modeling
  cat_features = [col for col in X.columns if X[col].dtype == 'object']
  num_features = [col for col in X.columns if col not in cat_features]

  cat_pipe = make_pipeline(
    SimpleImputer(strategy='constant', fill_value='missing'), OrdinalEncoder(handle_unknown='return_nan')
  )

  num_pipe = make_pipeline(
    SimpleImputer(strategy='median'), StandardScaler()
  )

  preprocessor = ColumnTransformer(transformers=[
    ('category transformer', cat_pipe, cat_features),
    ('numeric transformer', num_pipe, num_features)
  ])

# Transform the features
  X = preprocessor.fit_transform(X)

# Split the data into training and testing sets
  X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2
  )

  # Return the transformed feature matrices and target vectors
  return X_train, X_test, y_train, y_test
In [6]:
# Drop outlier (minimum_nights = 100000000000000)
df.drop(df['minimum_nights'].argmax(), inplace=True)
print(f"Dataset: {df.shape}")
Dataset: (226029, 17)
In [7]:
# Wrangle the data
X_train, X_test, y_train, y_test = wrangle(df)

print(f"""
Training set: {len(X_train) / len(df) *100}%
Testing set: {len(X_test) / len(df) *100}%
""")
Training set: 79.99991151577895%
Testing set: 20.000088484221052%

Initial Modeling
Linear Regression
My goal is to build a predictive model using Python which most accurately predicts the price of an airbnb. This is a regression problem, meaning the variable I'm targeting (price) could be an infinite number of values. First, I'll fitt a standard linear regression model from Scikit-Learn.
A linear regression model uses all the algebraic mind gymnastics we learned in high school, like slope and y-intercept, to fit a line to the input data (features of the airbnb listings.) It then applies the equation of that line to predict the price of future listings.
I want to test this model using two accuracy scoring methods:
• 
R-squared score (R2): Measures the proportion of the predicted outcome whose variance was accurately replicated by the linear model.
• 
Mean-squared error (MSE): Measures the square of the average difference between the predicted and actual values.
Let's fit and test a linear regression model:
 