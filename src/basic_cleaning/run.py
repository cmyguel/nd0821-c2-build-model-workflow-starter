#!/usr/bin/env python
"""
Download from W&B the raw dataset and apply some basic data cleaning, exporting the result to a new artifact
"""
import argparse
import logging
import wandb
#----------------------
import pandas as pd
# import tempfile
# from pathlib import Path


logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):

    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)

    ######################
    # YOUR CODE HERE     #
    
    # Move to a temporary directory
    # with tempfile.TemporaryDirectory() as temp_dir:
        
    # Download input artifact. This will also log that this script is using this
    # particular version of the artifact
    logger.info("Downloading input artifact")
    artifact_local_path = run.use_artifact(args.input_artifact).file()

    logger.info("Reading input artifact")
    df = pd.read_csv(artifact_local_path)
    
    logger.info("Dropping outlier prices")
    idx = df['price'].between(args.min_price, args.max_price)
    df = df[idx].copy()
    
    logger.info("Filtering latitudes and longitudes")
    idx = df['longitude'].between(-74.25, -73.50) & df['latitude'].between(40.5, 41.2)
    df = df[idx].copy()
    
    logger.info("Saving to .csv file")
    df.to_csv("clean_sample.csv", index=False)
    
    logger.info("Uploading to W&B")
    artifact = wandb.Artifact(
         args.output_artifact,
         type=args.output_type,
         description=args.output_description,
         )
    artifact.add_file("clean_sample.csv")
    run.log_artifact(artifact)   
    
    logger.info("Cleaning finished")
    
    ######################


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="A very basic data cleaning")


    parser.add_argument(
        "--input_artifact", 
        type=str,
        help="Input CSV file to be cleaned",
        required=True
    )

    parser.add_argument(
        "--output_artifact", 
        type=str,
        help="Output CSV file with clean data",
        required=True
    )

    parser.add_argument(
        "--output_type", 
        type=str,
        help="type of output artifact",
        required=True
    )

    parser.add_argument(
        "--output_description", 
        type=str,
        help="description of output artifact",
        required=True
    )

    parser.add_argument(
        "--min_price", 
        type=float,
        help="min range for dropping range",
        required=True
    )

    parser.add_argument(
        "--max_price", 
        type=float,
        help="max range for dropping range",
        required=True
    )


    args = parser.parse_args()

    go(args)
