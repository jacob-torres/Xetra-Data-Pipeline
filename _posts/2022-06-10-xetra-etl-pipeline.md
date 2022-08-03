---
layout: post
title: Xetra ETL Pipeline
subtitle: Building a Simple and Efficient pipeline to Report on Stock Exchange Data
author: Jake Torres
gh-repo: jacob-torres/xetra-data-pipeline/blob/main
gh-badge: [follow]
tags: [data-engineering, ETL, AWS, S3, pandas, docker, jupyter-notebook, financial-data, report]
comments: false
---

One of my favorite passtimes is building pipelines. Whether its a bare-bones application to process raw data into a clean, transformed tabular dataset, or an orchestrated workflow for streamlining the increasingly rapid development process, I love using engineering to make a bunch of code sing together like a choir.

Over the last few months, I've been developing a pipeline with a simple goal: make it easy to spot trends in the stock market every day.

I chose the public dataset formerly maintained on AWS S3 by the [Deutsche Boerse Xetra stock exchange network.](https://www.xetra.com/xetra-en/)
 Though the dataset is no longer being updated on AWS, I believe the project is still a good illustration of the process of building and deploying an ETL pipeline designed to do a job.

ETL (or in other cases ELT,) is a straight-forward method of transferring data from one place to another, with transformation logic in the middle or end that can be as simple or as complex as you can imagine. In the case of the Xetra dataset, a standard ETL job will extract CSV files from one S3 bucket, perform some aggrigation and calculations to create a useful report for our purposes, and drop that newly-formatted data into another S3 bucket in Parquet format.
 Once these tasks are successfully performed, a meta file will also be updated with the date of the data being extracted, and the date-time of the ETL job.

## Tech Stack

* Python 3.95
* Pandas
* Boto3
* Jupyter notebook
* AWS S3
* Docker

## First Steps

On AWS, the S3 service (also known as Simple Storage Service) allows users to create buckets which store objects. An object can be of any number of formats, including Parquet, CSV, and JSON.

To begin, we navigate to S3 and create a new bucket to store our daily report files. This will be the "load" destination of our ETL pipeline.

Once the bucket is created, we spawn a new virtual environment and open up a Jupyter notebook to start playing around with the Python libraries we'll be using:

* Boto3 will be critical for accessing the S3 buckets through a SSL connection. It has an easy API for interacting with AWS services.
* We'll be using Pandas for analyzing, cleaning, and transforming the raw data from the source bucket. The data is in CSV format, and Pandas is great for working with CSV.
* The PyYaml library makes it convenient to configure the application with YAML.
* A few choice builtin Python libraries, such as logging and argparse, will also come in handy once we're past the exploration phase.

## Functional Programming

Once the initial exploration of the data is complete, it's best to structure the first draft of our pipeline code in a series of functions. It gives us a good idea of what parameters each step of the pipeline will need, and how to most efficiently tie the three components (extract, transform, load) together.

In doing so, we need to separate the underlying adaptation layer from the application itself. In other words: if we need to perform many steps in order to extract the data, we need to split those tasks into their own functions and place them in a layer beneath the extract function. This layer is where the nitty-gritty logic is done.
 This is where we write the code to connect to the source S3 bucket, search for the object key of the data we want (in this case a string containing the current date, suffix, and a .csv extension,) and load that object into a Pandas dataframe. This is also where we use Pandas to perform mutations on the dataframe, convert it to a Parquet object, and drop it into the target S3 bucket.

## Object-Oriented Programming

In some cases, I prefer to leave my code in functions. It depends on each situation, and there is no one-size-fits-all. But in the case of a simple pipeline like the one we have, object-oriented structures work quite nicely.
 The pipeline itself is an object with some specific properties and abilities. The extract, transform, and load methods are combined to form the report method, which runs the ETL pipeline.

The S3 buckets are also objects, and in this project I named the class S3BucketConnector to emphasize that this class contains the logic for connecting to an existing bucket. It also has methods which can search for objects by prefix (date,) read CSV data to a Pandas dataframe, and write a Pandas dataframe to a Parquet object and load it into an S3 bucket. The meta file process is its own class and performs that meta file updates.

Once the code is organized into packages and modules with classes, the real fun begins: testing!

## Testing

It's important to begin with unit testing each individual method for flaws. This part of the process can sometimes take even longer than the coding, because lots of unanticipated errors *will* come up.
 I like using good old Unittest for Python testing, and it also supports integration testing. I wrote tests with the help of a mock package which mocks S3 and other AWS services.

## Deployment

Once unit and integration testing is complete, it's time to deploy. I used Docker to containerize the project, and Cron to create a job configuration. In the near future, an automated workflow with live on Kubernetes.

## Conclusions

I learned a lot during this project, not least of all the shere volume of complexity that goes into such a simple, straight-forward data pipeline. I'm excited to move onto more involved systems and more diverse data store technology. Orchestration, data warehouses/lakes/lakehouses, and so many more tools are bringing more value to people and businesses through data. The sky is the limit.
