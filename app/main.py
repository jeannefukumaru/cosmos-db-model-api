from ast import parse
from turtle import down
from typing import ItemsView
from fastapi import FastAPI
from dotenv import load_dotenv
from app_utils import load_model, parse_feature_spec, download_mlflow_artifacts
from cosmos_utils import query_cosmos
import logging
import json
from fastapi.encoders import jsonable_encoder
import uuid
from classes import *
import pandas as pd 
import configparser
import asyncio

load_dotenv()
config = configparser.ConfigParser()
config.read("../config.ini")

# Initialize the FastAPI app
SERVICE_NAME = "cosmos_online_model"
app = FastAPI(title=config["MODEL"]["SERVICE_NAME"], docs_url="/")

# Configure logger
logger = logging.getLogger("uvicorn")
logger.setLevel(logging.INFO)


@app.on_event("startup")
async def startup_load_model():
    global MODEL
    MODEL = load_model(config["MODEL"]["MODEL_ARTIFACT_PATH"])

@app.get("/test")
async def test_endpoint():
    """
    Test that the endpoint is working
    """
    return {"response": "endpoint reached"}

@app.post("/predict")
async def predict(item: Item):
    """Prediction endpoint.
    1. This should be a post request!
    2. Make sure to post the right data.
    """

    response_payload = None

    try:
        # Parse data
        # TODO: why is parsing response so complicated
        logger.info(f"Input: {str(item)}")
        wine_id = pd.Series(item.wine_id, name="wine_id")
        alcohol = pd.Series(item.alcohol, name="alcohol")
        frame = {"wine_id": wine_id, "alcohol": alcohol}
        input_df = pd.DataFrame(frame)

        ids = input_df["wine_id"]  # get lookup ids
        ids = [str([i]) for i in ids]  # convert int to "[id]" format in online store
        
        lookups = await query_cosmos(ids)  # lookup online features from online store

        # get list of features used by model
        feat_df = parse_feature_spec(config["MODEL"]["FEATURE_SPEC_PATH"])
        features = feat_df[feat_df["source"]=="feature_store"]["output_name"].to_list()
        print(f"features: {features}")

        # join with input data 
        lookups = lookups[features]
        inputs_and_features = input_df.join(lookups).drop("wine_id", axis=1)

        # Define UUID for the request
        request_id = uuid.uuid4().hex

        # Log input data
        logger.info(json.dumps({
            "service_name": config["MODEL"]["SERVICE_NAME"],
            "type": "InputData",
            "request_id": request_id,
            "data": input_df.to_json(orient='records'),
        }))

        # Make predictions and log
        model_output = MODEL.predict(inputs_and_features).tolist()

        # Log output data
        logger.info(json.dumps({
            "service_name": config["MODEL"]["SERVICE_NAME"],
            "type": "OutputData",
            "request_id": request_id,
            "data": model_output
        }))

        # Make response payload
        response_payload = jsonable_encoder(model_output)
    except Exception as e:
        response_payload = {
            "error": str(e)
        }

    return response_payload