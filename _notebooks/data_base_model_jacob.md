# Spotify Song Suggester

This application suggests a personalized playlist of tracks based on a user's song preference data.

---

## Data Cleaning



```python
# Imports
import numpy as np
import pandas as pd
import pickle
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
from sklearn.neighbors import NearestNeighbors
```


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




```python
# Transform release_date feature to integer years
df.release_date = df.release_date.apply(lambda x: int(x[:4]))
df.head()
```




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
      <td>1922</td>
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
      <td>1922</td>
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
      <td>1922</td>
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
      <td>1922</td>
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




```python
# Create modeling data
features = [
    'popularity',
    'duration_ms',
    'explicit',
    'release_date',
    'danceability',
    'energy',
    'key',
    'loudness',
    'mode',
    'speechiness',
    'acousticness',
    'instrumentalness',
    'liveness',
    'valence',
    'tempo',
    'time_signature'
]

X = df[features]

print(X.shape)
X.head()
```

    (586672, 16)





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
      <th>popularity</th>
      <th>duration_ms</th>
      <th>explicit</th>
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
      <td>6</td>
      <td>126903</td>
      <td>0</td>
      <td>1922</td>
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
      <td>0</td>
      <td>98200</td>
      <td>0</td>
      <td>1922</td>
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
      <td>0</td>
      <td>181640</td>
      <td>0</td>
      <td>1922</td>
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
      <td>0</td>
      <td>176907</td>
      <td>0</td>
      <td>1922</td>
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
      <td>0</td>
      <td>163080</td>
      <td>0</td>
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




```python
# Scale data
scaler = StandardScaler()
pca = PCA(n_components=5)

X = scaler.fit_transform(X)
X = pca.fit_transform(X)
X.shape
```




    (586672, 5)




```python
# NearestNeighbors model
model = NearestNeighbors(n_neighbors=10, algorithm='kd_tree')
neighbors = model.fit(X)
```


```python
# Find nearest neighbors to test track "My Girl"
my_girl_track = X[38498].reshape(1, -1)
distances, indices = neighbors.kneighbors(my_girl_track)
indices
```




    array([[ 38498, 119126, 343116, 130192, 358146, 435397,  74376,  68876,
            489947,  68345]], dtype=int64)




```python
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



## Save Base Model


```python
# Save using pickle
pickle.dump(model, open('../models/base_nn.sav', mode='wb'))
```
