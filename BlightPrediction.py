import GetTrainingData as TrainingData
import numpy as np
import pandas as pd
import QueryBuildings as QB
from pprint import pprint
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import RandomizedSearchCV, cross_val_score
from sklearn import svm, tree, metrics
from sklearn.metrics import roc_auc_score
from scipy.stats import randint as sp_randint
import sys
# output the model accuracy results to a file
sys.stdout = open("Data/ModelAccuracy.txt", "w")

# Utility function to report best scores
def report(results, n_top=1):
    for i in range(1, n_top + 1):
        candidates = np.flatnonzero(results['rank_test_score'] == i)
        for candidate in candidates:
            print("Model")
            print("Mean validation score: {0:.3f} (std: {1:.3f})".format(
                  results['mean_test_score'][candidate],
                  results['std_test_score'][candidate]))
            print("Parameters: {0}".format(results['params'][candidate]))

# Get the training data and labels
(X, Y) = TrainingData.GetData()
X = TrainingData.TransformData(X)
# Our random forest classifer
clf = RandomForestClassifier(n_estimators=100, min_samples_split=5, random_state=0, max_features=None, max_depth=20)

# parameter search space for our randomizedsearchcv meta parameter tuning
param_dist = {"max_depth": [3, 6, 9, 12, None],
              "max_features": sp_randint(1, 11),
              "min_samples_split": sp_randint(2, 11),
              "bootstrap": [True, False],
              "criterion": ["gini", "entropy"]}

n_iter_search = 1
random_search = RandomizedSearchCV(clf, param_distributions=param_dist, n_iter=n_iter_search, cv=5, scoring="roc_auc")
random_search.fit(X, Y)
report(random_search.cv_results_)
feature_importances = pd.DataFrame(random_search.best_estimator_.feature_importances_, index = X.columns, columns=['importance']).sort_values('importance', ascending=False).reset_index()
feature_importances = feature_importances.rename(columns={'index':'variable'})
feature_importances.to_csv("data/FeatureImportances.csv")

(XAll,YAll) = TrainingData.GetAllData()
XTransform = TrainingData.TransformData(XAll)[X.columns]
probabilities = random_search.predict(XTransform)
XAll = pd.concat([XAll, pd.DataFrame(probabilities, columns=['ProbT'])], axis=1)
XAll['area_name'] = XAll.apply(lambda x: QB.GetNHoodName(x["nhood_id"]), axis=1)
XAll[['lat', 'lon']] = XAll.apply(lambda x: QB.GetLatLon(x["parcelnum"]), axis=1)
XAll.to_csv("data/AllPredictions.csv")

XNhoodG = XAll.groupby(['nhood_id'])['ProbT'].mean().reset_index()
XNhoodG.columns = ["_id", "prob"]
XNhoodG['area_name'] = XNhoodG.apply(lambda x: QB.GetNHoodName(x["_id"]), axis=1)
XNhoodG.to_csv("data/Nhood_BlightProb.csv")