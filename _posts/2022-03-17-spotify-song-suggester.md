---
layout: post
title: Spotify Song Suggester
subtitle: Using a Nearest Neighbors Model to Suggest the Best Music for Anyone
author: Jake Torres
gh-repo: jacob-torres/spotify-song-suggester/blob/main/airbnb_ds_u2.ipynb
gh-badge: [follow]
tags: [data-science, data-analysis, machine-learning, spotify, music, recommendations, nearest-neighbors, predictive-modeling, jupyter-notebook]
comments: false
---

[Spotify](https://spotify.com/) is one of the most popular music streaming services in the world. For one of my most interesting projects yet, I was tasked with writing and training a machine learning model to suggest songs to a Spotify user based on their liked songs.

Using a dataset of over half a million songs, I built a nearest-neighbors model with [Scikit-learn](https://scikit-learn.org/stable/)
 to generate 10 recommended songs from the dataset which most closely matched the attributes of the input songs. This model is an unsupervised classification problem, meaning the model has no "target variable" to learn from.

### Tools

* Python 3.8
* Pandas
* Numpy
* Scikit-Learn:
  - [StandardScaler](https://scikit-learn.org/stable/modules/generated/sklearn.preprocessing.StandardScaler.html)
  - [PCA](https://scikit-learn.org/stable/modules/generated/sklearn.decomposition.PCA.html)
  - [NearestNeighbors](https://scikit-learn.org/stable/modules/generated/sklearn.neighbors.NearestNeighbors.html)

---

## Exploratory Data Analysis

First, we'll look at the head of the dataframe:

 ```python
# Load data
DATA_PATH = '../data/raw/tracks.csv.zip'
df = pd.read_csv(DATA_PATH)

# Display data
print(df.shape)
df.head()
```


    (586672, 20)



<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>id</th>
      <th>name</th>
      <th>popularity</th>
      <th>duration_ms</th>
      <th>explicit</th>
      <th>artists</th>
      <th>id_artists</th>
      <th>release_date</th>
      <th>danceability</th>
      <th>energy</th>
      <th>key</th>
      <th>loudness</th>
      <th>mode</th>
      <th>speechiness</th>
      <th>acousticness</th>
      <th>instrumentalness</th>
      <th>liveness</th>
      <th>valence</th>
      <th>tempo</th>
      <th>time_signature</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>35iwgR4jXetI318WEWsa1Q</td>
      <td>Carve</td>
      <td>6</td>
      <td>126903</td>
      <td>0</td>
      <td>['Uli']</td>
      <td>['45tIt06XoI0Iio4LBEVpls']</td>
      <td>1922-02-22</td>
      <td>0.645</td>
      <td>0.4450</td>
      <td>0</td>
      <td>-13.338</td>
      <td>1</td>
      <td>0.4510</td>
      <td>0.674</td>
      <td>0.7440</td>
      <td>0.151</td>
      <td>0.127</td>
      <td>104.851</td>
      <td>3</td>
    </tr>
    <tr>
      <th>1</th>
      <td>021ht4sdgPcrDgSk7JTbKY</td>
      <td>Capítulo 2.16 - Banquero Anarquista</td>
      <td>0</td>
      <td>98200</td>
      <td>0</td>
      <td>['Fernando Pessoa']</td>
      <td>['14jtPCOoNZwquk5wd9DxrY']</td>
      <td>1922-06-01</td>
      <td>0.695</td>
      <td>0.2630</td>
      <td>0</td>
      <td>-22.136</td>
      <td>1</td>
      <td>0.9570</td>
      <td>0.797</td>
      <td>0.0000</td>
      <td>0.148</td>
      <td>0.655</td>
      <td>102.009</td>
      <td>1</td>
    </tr>
    <tr>
      <th>2</th>
      <td>07A5yehtSnoedViJAZkNnc</td>
      <td>Vivo para Quererte - Remasterizado</td>
      <td>0</td>
      <td>181640</td>
      <td>0</td>
      <td>['Ignacio Corsini']</td>
      <td>['5LiOoJbxVSAMkBS2fUm3X2']</td>
      <td>1922-03-21</td>
      <td>0.434</td>
      <td>0.1770</td>
      <td>1</td>
      <td>-21.180</td>
      <td>1</td>
      <td>0.0512</td>
      <td>0.994</td>
      <td>0.0218</td>
      <td>0.212</td>
      <td>0.457</td>
      <td>130.418</td>
      <td>5</td>
    </tr>
    <tr>
      <th>3</th>
      <td>08FmqUhxtyLTn6pAh6bk45</td>
      <td>El Prisionero - Remasterizado</td>
      <td>0</td>
      <td>176907</td>
      <td>0</td>
      <td>['Ignacio Corsini']</td>
      <td>['5LiOoJbxVSAMkBS2fUm3X2']</td>
      <td>1922-03-21</td>
      <td>0.321</td>
      <td>0.0946</td>
      <td>7</td>
      <td>-27.961</td>
      <td>1</td>
      <td>0.0504</td>
      <td>0.995</td>
      <td>0.9180</td>
      <td>0.104</td>
      <td>0.397</td>
      <td>169.980</td>
      <td>3</td>
    </tr>
    <tr>
      <th>4</th>
      <td>08y9GfoqCWfOGsKdwojr5e</td>
      <td>Lady of the Evening</td>
      <td>0</td>
      <td>163080</td>
      <td>0</td>
      <td>['Dick Haymes']</td>
      <td>['3BiJGZsyX9sJchTqcSA7Su']</td>
      <td>1922</td>
      <td>0.402</td>
      <td>0.1580</td>
      <td>3</td>
      <td>-16.900</td>
      <td>0</td>
      <td>0.0390</td>
      <td>0.989</td>
      <td>0.1300</td>
      <td>0.311</td>
      <td>0.196</td>
      <td>103.220</td>
      <td>4</td>
    </tr>
  </tbody>
</table>
</div>


## Data Cleaning and Preprocessing

This structured data looks ideal for the most part. A few things stand out though, mostly the datatype of the "release_date" feature, which is a string.
 To make this feature model-friendly, we'll cut the first 4 digits (the year) from the string and convert it into an integer.
 For now the year should be adequate. In later versions we might parse these dates using the [Pandas datetime object](https://pandas.pydata.org/docs/reference/api/pandas.to_datetime.html),
 possibly giving us more information.

Now that all of the data is numeric, let's move on to preparing it for modeling.

For a preprocessing pipeline, we can use a number of methods. In my first solution, I went with the StandardScaler and principle component analysis (PCA). We can do any number of components when using PCA, and I went with 5. Once these steps are taken, we can start modeling.

---

## Nearest Neighbors Model

We'll go with the standard NearestNeighbors model, an unsupervised learner which can be instantiated with a few different algorithms and a specified number of results to compute.
 This is not the same as the K-NearestNeighbors classifier, which is a supervised learner. The NearestNeighbors model can make predictions with unlabeled data input.
 Since this dataset doesn't have a target (which in this case would be "liked" or "not liked,") it can only benefit from an unsupervised solution.

 The model will calculate the similarity of the results by measuring the distance between the songs, represented by vectors in virtual space. This is what makes the PCA a good algorithm for analyzing the components of song data. It's a much more concise solution for synthesizing the data and extracting meaning from it.

Below we'll instantiate the model and set it to find 10 recommendations based on a sample song. Then we'll print the results and use our intuition on the model's accuracy.


```python
# NearestNeighbors model
model = NearestNeighbors(n_neighbors=10, algorithm='kd_tree')
neighbors = model.fit(X)

# Find nearest neighbors to test track "My Girl"
my_girl_track = X[38498].reshape(1, -1)
distances, indices = neighbors.kneighbors(my_girl_track)
[df.iloc[ind].name for ind in indices]
```




    [38498                                               My Girl
     119126                                          Hello Walls
     343116                            True Love Will Never Fade
     130192    Kiss the Girl - From "The Little Mermaid"/ Sou...
     358146                                        Никой не може
     435397                                                Sampa
     74376                          Somebody That I Used To Know
     68876                                    The Cuppycake Song
     489947                   Redemption Song - B Is For Bob Mix
     68345                                   Parece Que Fue Ayer
     Name: name, dtype: object]

---

## Conclusion

 While this model is still a work in progress, it demonstrates the power of the nearest-neighbors method in determining the most appropriate recommendations to make based on unlabeled data. My next questions are which metrics are best to test the accuracy of the model, and how might I design an [autoencoder neural network](https://www.tensorflow.org/tutorials/generative/autoencoder) to process unlabeled song data and perhaps output more accurate results.

If you'd like to check out the entire jupyter notebook I used to build and test this model, [click here.](https://jacobtorres.net/2022-03-17-spotify-nearest-neighbors-model-notebook/)
