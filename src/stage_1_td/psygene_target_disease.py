import pyarrow as pa
import pyarrow.compute as pc
import pandas as pd

from ..utils import utils


def psygene_target_disease(psygene_path: str):
    psygene_target_disease = (
        utils.read_csv(
            psygene_path, delimiter="\t", compression="gzip", read_mode="pandas"
        )
        .select(
            [
                "psygenet_v02.txt",
                "Disease_Id",
                "PsychiatricDisorder",
                "Number_of_Abstracts",
                "Number_of_AbstractsValidated",
                "Score",
            ]
        )
        .rename_columns(
            [
                "gene_id",
                "disease_id",
                "psychiatric_disorder",
                "evidence",
                "validated_evidence",
                "score",
            ]
        )
    )

    # format disease_id
    disease_id = pc.replace_substring(
        psygene_target_disease.column("disease_id"), "umls:", "UMLS_"
    )

    # replace column
    psygene_target_disease = psygene_target_disease.drop(["disease_id"]).append_column(
        "disease_id", disease_id
    )

    return psygene_target_disease


def run():
    psygene_path = "downloads/target-disease/all_GeneDiseaseAssociations.tar.gz"

    # create table
    target_disease = psygene_target_disease(psygene_path)

    # write to S3
    output = "stage-1/psygene_target_disease.parquet"

    utils.write_parquet(target_disease, output)
