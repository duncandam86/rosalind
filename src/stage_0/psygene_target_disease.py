import pyarrow as pa
import pyarrow.compute as pc
import pandas as pd

from ..utils import utils


def psygene_target_disease(psygene_path: str):
    psygene_target_disease = utils.read_csv(
        psygene_path, delimiter="\t", compression="gzip", read_mode="pandas"
    )

    # select columns
    gene_id = psygene_target_disease.column("psygenet_v02.txt")
    disease_id = psygene_target_disease.column("Disease_Id")
    psychiatric_disorder = psygene_target_disease.column("PsychiatricDisorder")
    evidence = psygene_target_disease.column("Number_of_Abstracts")
    validated_evidence = psygene_target_disease.column("Number_of_AbstractsValidated")
    score = psygene_target_disease.column("Score")

    # create new table
    psygene_target_disease = pa.table(
        [
            gene_id,
            disease_id,
            psychiatric_disorder,
            evidence,
            validated_evidence,
            score,
        ],
        names=[
            "gene_id",
            "disease_id",
            "psychiatric_disorder",
            "evidence",
            "validated_evidence",
            "score",
        ],
    )

    return psygene_target_disease


def run():
    psygene_path = "downloads/target-disease/all_GeneDiseaseAssociations.tar.gz"

    # create table
    target_disease = psygene_target_disease(psygene_path)

    # write to S3
    output = "stage-0/psygene_target_disease.parquet"

    utils.write_parquet(target_disease, output)
