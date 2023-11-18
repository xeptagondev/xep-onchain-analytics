import os

import sklearn
from sklearn.preprocessing import MinMaxScaler, QuantileTransformer
from sklearn.cluster import KMeans
from sklearn.manifold import TSNE
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import IsolationForest 

import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib as mpl
import matplotlib.pyplot as plt
from scipy.stats.mstats import zscore
from kneed import KneeLocator

import datetime as dt
from datetime import datetime, tzinfo

import scipy, json, csv, time, pytz
from pytz import timezone
from functools import reduce

from pyod.models.knn import KNN
from pyod.models.auto_encoder import AutoEncoder

import psycopg2
from sqlalchemy import create_engine

# Database configurations
with open('../../config.json') as config_file:
    config = json.load(config_file)

# Connecting to Database
engine = create_engine(config['postgre']['engine'])
conn = psycopg2.connect(database = config['postgre_extract']['database'],
                            host = config['postgre_extract']['host'],
                            user = config['postgre_extract']['user'],
                            password = config['postgre_extract']['password'],
                            port = config['postgre_extract']['port'])

cursor = conn.cursor()

# Read tsv files
df_transaction_volume = pd.read_parquet('/home/ec2-user/test/data/basic_metrics/transaction_volume_usd.parquet')
df_transaction_volume = df_transaction_volume.rename(columns={"sum(Output total – Blocks (USD)) – Bitcoin" : "Transaction Volume"})

df_transaction_count = pd.read_parquet('/home/ec2-user/test/data/basic_metrics/transaction_count.parquet')
df_transaction_count = df_transaction_count.rename(columns={"sum(Transaction count – Blocks) – Bitcoin" : "Transaction Count"})

df_cdd = pd.read_parquet('/home/ec2-user/test/data/basic_metrics/cdd.parquet')
df_cdd = df_cdd.rename(columns={"sum(Coindays destroyed – Blocks) – Bitcoin" : "Coin-Days-Destroyed (CDD)"})

df_supply = pd.read_parquet('/home/ec2-user/test/data/basic_metrics/circulating_supply.parquet')
df_supply = df_supply.rename(columns={"runningsum(Generated value – Blocks (BTC)) – Bitcoin" : "Circulating Supply"})
df_supply["Circulating Supply"] = df_supply["Circulating Supply"] / 100000000

df_price = pd.read_parquet('/home/ec2-user/test/data/basic_metrics/price.parquet')
df_price = df_price.rename(columns={"Price (BTC/USD) – Bitcoin" : "Price ($)"})

# Create df with respective set of metrics
# Final set of metrics - Total Transaction Value + Total Transaction Count + CDD + Circulating Supply + Price
dfs = [df_transaction_volume, df_transaction_count, df_cdd, df_supply, df_price]
df_final = reduce(lambda left,right: pd.merge(left, right, on='Time', how='inner'), dfs)
df_isoForest = df_final.copy()
df_autoEncoder = df_final[['Transaction Volume', 'Transaction Count', 
                           'Coin-Days-Destroyed (CDD)', 'Circulating Supply', 'Price ($)']]

data_kMeans = df_final[['Transaction Volume', 'Transaction Count']]

outliers_fraction=0.05

# Isolation Forest
scaler = StandardScaler()
np_scaled = scaler.fit_transform(df_isoForest[['Transaction Volume', 'Transaction Count', 
                                               'Coin-Days-Destroyed (CDD)', 'Circulating Supply', 'Price ($)']])
data_isoForest = pd.DataFrame(np_scaled)

# Train isolation forest
model =  IsolationForest(contamination=outliers_fraction, random_state=111)
model.fit(data_isoForest)

df_isoForest['anomaly'] = pd.Series(model.predict(data_isoForest))
df_isoForest['anomaly'] = df_isoForest['anomaly'].apply(lambda x: x == -1)
df_isoForest['anomaly'] = df_isoForest['anomaly'].astype(int)
print(df_isoForest['anomaly'].value_counts())

fig, ax = plt.subplots(figsize=(10,6))
abnormaly = df_isoForest.loc[df_isoForest['anomaly'] == 1]
ax.plot(df_isoForest['Transaction Volume'], color='black', label = 'Normal', linewidth=1.5)
ax.scatter(abnormaly.index ,abnormaly['Transaction Volume'], color='red', label = 'Anomaly', s=16)
plt.legend()
plt.title("Anomaly Detection Using Isolation Forest")
plt.xlabel('Date')
plt.ylabel('Total Transaction Value')
plt.show()

fig, ax = plt.subplots(figsize=(10,6))
abnormaly = df_isoForest.loc[df_isoForest['anomaly'] == 1]
ax.plot(df_isoForest['Transaction Count'], color='black', label = 'Normal', linewidth=1.5)
ax.scatter(abnormaly.index, abnormaly['Transaction Count'], color='red', label = 'Anomaly', s=16)
plt.legend()
plt.title("Anomaly Detection Using Isolation Forest")
plt.xlabel('Date')
plt.ylabel('Total Transaction Count')
plt.show()

fig, ax = plt.subplots(figsize=(10,6))
abnormaly = df_isoForest.loc[df_isoForest['anomaly'] == 1]
ax.plot(df_isoForest['Coin-Days-Destroyed (CDD)'], color='black', label = 'Normal', linewidth=1.5)
ax.scatter(abnormaly.index ,abnormaly['Coin-Days-Destroyed (CDD)'], color='red', label = 'Anomaly', s=16)
plt.legend()
plt.title("Anomaly Detection Using Isolation Forest")
plt.xlabel('Date')
plt.ylabel('Coin-Days-Destroyed (CDD)')
plt.show()

fig, ax = plt.subplots(figsize=(10,6))
abnormaly = df_isoForest.loc[df_isoForest['anomaly'] == 1]
ax.plot(df_isoForest['Circulating Supply'], color='black', label = 'Normal', linewidth=1.5)
ax.scatter(abnormaly.index, abnormaly['Circulating Supply'], color='red', label = 'Anomaly', s=16)
plt.legend()
plt.title("Anomaly Detection Using Isolation Forest")
plt.xlabel('Date')
plt.ylabel('Circulating Supply')
plt.show()

fig, ax = plt.subplots(figsize=(10,6))
abnormaly = df_isoForest.loc[df_isoForest['anomaly'] == 1]
ax.plot(df_isoForest['Price ($)'], color='black', label = 'Normal', linewidth=1.5)
ax.scatter(abnormaly.index, abnormaly['Price ($)'], color='red', label = 'Anomaly', s=16)
plt.legend()
plt.title("Anomaly Detection Using Isolation Forest")
plt.xlabel('Date')
plt.ylabel('Price ($)')
plt.show()

print(df_isoForest.loc[df_isoForest['anomaly'] == 1].head())

# Save IsoForest Results to PostgreSQL
df_isoForest['Time'] = pd.to_datetime(df_isoForest['Time'], dayfirst = True).dt.date
df_isoForest.rename(columns = {'Time': 'Date'}, inplace=True)
df_isoForest.to_sql('isoForest_outliers', con=engine, if_exists = 'replace')

print("Isolation Forest Completed")

# Auto-Encoder
scaler = StandardScaler() 
scaler.fit(df_autoEncoder)    
df_autoEncoder_scaled = scaler.transform(df_autoEncoder) 
df_autoEncoder_scaled = pd.DataFrame(df_autoEncoder_scaled)

# Train Auto-Encoder
clf = AutoEncoder(contamination = outliers_fraction, hidden_neurons =[25, 2, 2, 25])
clf.fit(df_autoEncoder_scaled)

anomaly_scores = clf.decision_scores_ 
anomaly_pred = clf.predict(df_autoEncoder_scaled)

anomaly_scores = pd.Series(anomaly_scores)
anomaly_pred = pd.Series(anomaly_pred)

df_autoEncoder['score'] = anomaly_scores
df_autoEncoder['anomaly'] = anomaly_pred
df_autoEncoder['anomaly'].value_counts()

df_autoEncoder.groupby('anomaly').mean()
df_autoEncoder = pd.concat([df_final[['Time']], df_autoEncoder], axis = 1)

# Save AutoEncoder Results to PostgreSQL
df_autoEncoder['Time'] = pd.to_datetime(df_autoEncoder['Time'], dayfirst = True).dt.date
df_autoEncoder.rename(columns = {'Time': 'Date'}, inplace=True)
df_autoEncoder.to_sql('autoEncoder_outliers', con=engine, if_exists = 'replace')

print("Outlier Detection Completed")

# K-Means Clustering
def pca_results(good_data, pca):
    # Dimension indexing
    dimensions = dimensions = ['Dimension {}'.format(i) for i in range(1,len(pca.components_)+1)]

    # PCA components
    components = pd.DataFrame(np.round(pca.components_, 4), columns = good_data.keys())
    components.index = dimensions

    # PCA explained variance
    ratios = pca.explained_variance_ratio_.reshape(len(pca.components_), 1)
    variance_ratios = pd.DataFrame(np.round(ratios, 4), columns = ['Explained Variance'])
    variance_ratios.index = dimensions

    # Create a bar plot visualization
    fig, ax = plt.subplots(figsize = (10,10))

    # Plot the feature weights as a function of the components
    components.plot(ax = ax, kind = 'bar');
    ax.set_ylabel("Feature Weights")
    ax.set_xticklabels(dimensions, rotation=0)

    # Display the explained variance ratios
    for i, ev in enumerate(pca.explained_variance_ratio_):
        ax.text(i-0.40, ax.get_ylim()[1] + 0.05, "Explained Variance\n %.4f"%(ev))

    # Return a concatenated DataFrame
    return pd.concat([variance_ratios, components], axis = 1)

# Plot Elbow Method
data_kMeans[:] = MinMaxScaler().fit_transform(data_kMeans[:])
pca = PCA(n_components=2) # we have selected 2 components in PCA for simplicity
pca.fit(data_kMeans)
reduced_data = pca.transform(data_kMeans)
reduced_data = pd.DataFrame(reduced_data)

num_clusters = range(1, 20)
seed = 4103

kmeans = [KMeans(n_clusters=i, random_state=seed).fit(reduced_data) for i in num_clusters]
scores = [kmeans[i].score(reduced_data) for i in range(len(kmeans))]

fig, ax = plt.subplots(figsize=(8,6))
ax.plot(num_clusters, scores, linewidth = 4)
plt.xticks(num_clusters)
plt.xlabel('Number of Clusters')
plt.ylabel('Score')
plt.title('Elbow Curve')
plt.show()

# Finding point of inflection
kl = KneeLocator(range(1, 20), scores, curve="concave", direction="increasing")
kl.elbow

# Choosing the three clusters based on the elbow curve
best_num_cluster_ = 3
km_ = KMeans(n_clusters=best_num_cluster_, random_state=seed)
km_.fit(reduced_data)
km_.predict(reduced_data)
labels_3 = km_.labels_

fig = plt.figure(1, figsize=(7,7))
plt.scatter(reduced_data.iloc[:,0], reduced_data.iloc[:,1], 
            c=labels_3.astype(np.float), edgecolor="k", s=16)
plt.xlabel('Principal Component 1')
plt.ylabel('Principal Component 2')
plt.title('Clusters based on K means: 3 clusters')

# Choosing the four clusters based on the elbow curve
best_num_cluster_ = 4
km_ = KMeans(n_clusters=best_num_cluster_, random_state=seed)
km_.fit(reduced_data)
km_.predict(reduced_data)
labels_4 = km_.labels_

fig = plt.figure(1, figsize=(7,7))
plt.scatter(reduced_data.iloc[:,0], reduced_data.iloc[:,1], 
            c=labels_4.astype(np.float), edgecolor="k", s=16)
plt.xlabel('Principal Component 1')
plt.ylabel('Principal Component 2')
plt.title('Clusters based on K means: 4 clusters')

# Choosing the five clusters based on the elbow curve
best_num_cluster_ = 5
km_ = KMeans(n_clusters=best_num_cluster_, random_state=seed)
km_.fit(reduced_data)
km_.predict(reduced_data)
labels_5 = km_.labels_

fig = plt.figure(1, figsize=(7,7))
plt.scatter(reduced_data.iloc[:,0], reduced_data.iloc[:,1], 
            c=labels_5.astype(np.float), edgecolor="k", s=16)
plt.xlabel('Principal Component 1')
plt.ylabel('Principal Component 2')
plt.title('Clusters based on K means: 5 clusters')

# Using 4 clusters as it produced the best results
best_cluster = 4
reduced_data.loc[0]
mod = kmeans[best_cluster - 1] # -1 account for 0 index
mod.cluster_centers_

reduced_data['Principal Component 1'] = reduced_data[0]
reduced_data['Principal Component 2'] = reduced_data[1]
reduced_data.drop(columns = [0, 1], inplace=True)
reduced_data.head()

def getDistanceByPoint(data, model):
    distance = []
    for i in range(0,len(data)):
        Xa = np.array(data.loc[i])
        Xb = model.cluster_centers_[model.labels_[i]-1]
        distance.append(np.linalg.norm(Xa-Xb))
    return distance
    
distance = getDistanceByPoint(reduced_data, kmeans[best_cluster - 1])
distance = pd.Series(distance)
number_of_outliers = int(outliers_fraction*len(distance))
threshold = distance.nlargest(number_of_outliers).min()

data_kMeans['anomaly'] = (distance >= threshold).astype(int)

# Visualisation of anomaly with cluster view
colors = {0:'blue', 1:'red'}
plt.figure(figsize=(7,7))
plt.scatter(reduced_data.iloc[:,0], reduced_data.iloc[:,1], 
            c=data_kMeans["anomaly"].apply(lambda x: colors[x]), s=25)
plt.xlabel('Principal Feature 1')
plt.ylabel('Principal Feature 2')
plt.title('Anomaly prediction using KMeans: Red represents Anomaly')

cluster_labels = pd.DataFrame(labels_4, columns=['cluster']) 
data_kMeans = pd.concat([df_final[['Time']], reduced_data, data_kMeans, cluster_labels], axis = 1)
print(data_kMeans['anomaly'].value_counts())

# Save K-Means Results to PostgreSQL
data_kMeans['Time'] = pd.to_datetime(data_kMeans['Time'], dayfirst = True).dt.date
data_kMeans.rename(columns = {'Time': 'Date'}, inplace=True)
data_kMeans.to_sql('kmeans_outliers', con=engine, if_exists = 'replace')

print("K-Means Completed")

conn.close()
cursor.close()