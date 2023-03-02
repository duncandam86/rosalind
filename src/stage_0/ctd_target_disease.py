import pyarrow as pa
import pyarrow.compute as pc
from pyarrow import fs

from ..utils import utils


def ctd_target_disease(
    ctd_path: str,
):
    ctd_gene_disease = utils.read_csv(
        path=ctd_path,
        compression="gzip",
        delimiter="\t",
        skip_rows=27,
        skip_rows_after_names=1,
        bucket="./resources",
        fs_type="local",
    )

    # filter direct evidence is not empty
    ctd_gene_disease_direct = (
        ctd_gene_disease.filter(pc.not_equal(pc.field("DirectEvidence"), ""))
        .select(["GeneID", "DiseaseID", "DirectEvidence", "PubMedIDs"])
        .rename_columns(["gene_id", "disease_id", "evidence", "pubmed_evidence"])
    )

    # reformat disease_id
    disease_id = pc.replace_substring(
        ctd_gene_disease_direct.column("disease_id"), ":", "_"
    )

    # append new disease_id column
    ctd_gene_disease_direct.drop(["disease_id"]).append_column("disease_id", disease_id)

    return ctd_gene_disease_direct


def run():
    ctd_path = "downloads/target-disease/CTD_genes_diseases.tsv.gz"

    # create table
    target_disease = ctd_target_disease(ctd_path)
    print(target_disease[0:3])
    # write to S3
    output = "stage-0/ctd_target_disease.parquet"

    utils.write_parquet(target_disease, output)
