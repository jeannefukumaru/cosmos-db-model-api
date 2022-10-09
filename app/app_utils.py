from threading import local
from dotenv import load_dotenv
from mlflow.tracking import MlflowClient
import mlflow
import os
import pickle
import pandas as pd
from yaml import load, Loader

load_dotenv()

def load_model(model_artifact_path: str):
    """Loads model artifact from the specified path."""

    if (os.path.exists(model_artifact_path)):
        with open(model_artifact_path, 'rb') as file:
            model_artifact = pickle.load(file)
        return model_artifact
    else:
        raise FileNotFoundError("The specified path ({model_artifact_path})\
            does not exist.")

def download_mlflow_artifacts(run_id, remote_dir, local_dir):
    """download model artifacts from mlflow registry"""
    client = MlflowClient("databricks://jeanne_az")
    if not os.path.isdir(local_dir):
        os.mkdir(local_dir)
    print(f"local_dir: {local_dir}")
    print(f"local_dir_full_path: {os.path.abspath(local_dir)}")
    download_path = client.download_artifacts(run_id, remote_dir, local_dir)
    return download_path

# download_path = download_mlflow_artifacts("ab37c0022b1346b283876ea77991e225", "model", "model_artifacts")
# print(download_path)

def parse_feature_spec(spec_path):
    """
    parse feature spec to return dataframe of 
    (source, table_name, feature_name, lookup_key and output_name)
    """
    # load feature_spec.yaml
    with open(spec_path, "r") as f:
        features = load(f, Loader)
    feat_df = pd.DataFrame()
    for i in range(len(features["input_columns"])):
        for k, v in features["input_columns"][i].items():
             feat_df = pd.concat([feat_df, pd.DataFrame(v, index=[0])])
    return feat_df.reset_index(drop=True)
