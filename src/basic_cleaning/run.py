#!/usr/bin/env python
"""
Download from W&B the raw dataset and apply some basic data cleaning,
exporting the result to a new artifact

Author: Wonseok Oh
Date: May, 2023
"""
import argparse
import logging
import pandas as pd
import wandb


logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def clean(args):
    '''
    clean: conduct data cleaning
    Input : args (Namespace) -- arguments from argparse
    Output: None
    '''
    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)

    # Download input artifact. This will also log that this script is using this
    # particular version of the artifact
    logger.info("Downloading artifact")
    artifact_local_path = run.use_artifact(args.input_artifact).file()

    d_f = pd.read_csv(artifact_local_path)

    # Drop the duplicates
    num_rows = len(d_f)
    d_f = d_f[(d_f['price'] >= args.min_price) &
              (d_f['price'] <= args.max_price)]
    d_f.dropna(subset = ['price'], inplace=True)
    logger.info(
        "Dropped price outliers that fall out of the range [%f, %f] or null: %d rows dropped",
        args.min_price,
        args.max_price,
        num_rows -
        len(d_f))

    d_f.to_csv("clean_sample.csv", index=False)

    artifact = wandb.Artifact(
        args.output_artifact,
        type=args.output_type,
        description=args.output_description,
    )

    artifact.add_file("clean_sample.csv")

    logger.info("Logging artifact")
    run.log_artifact(artifact)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="A very basic data cleaning")

    parser.add_argument(
        "--input_artifact",
        type=str,
        help="the input artifact",
        required=True
    )

    parser.add_argument(
        "--output_artifact",
        type=str,
        help="the name for the output artifact",
        required=True
    )

    parser.add_argument(
        "--output_type",
        type=str,
        help="the type for the output artifact",
        required=True
    )

    parser.add_argument(
        "--output_description",
        type=str,
        help="a description for the output artifact",
        required=True
    )

    parser.add_argument(
        "--min_price",
        type=float,
        help="the minimum price to consider",
        required=True
    )

    parser.add_argument(
        "--max_price",
        type=float,
        help="the maximum price to consider",
        required=True
    )

    ARGs = parser.parse_args()

    clean(ARGs)
