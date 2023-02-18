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
        # bucket="./resources",
        # filesystem=fs.LocalFileSystem()
    )

    # filter direct evidence is not empty
    ctd_gene_disease_direct = ctd_gene_disease.filter(
        pc.not_equal(pc.field("DirectEvidence"), "")
    )

    # select columns
    gene_id = ctd_gene_disease_direct.column("GeneID")
    disease_id = ctd_gene_disease_direct.column("DiseaseID")
    evidence = ctd_gene_disease_direct.column("DirectEvidence")
    pubmed_evidence = ctd_gene_disease_direct.column("PubMedIDs")

    # create new table
    ctd_gene_disease_direct = pa.table(
        [gene_id, disease_id, evidence, pubmed_evidence],
        names=["gene_id", "disease_id", "evidence", "pubmed_evidence"],
    )

    return ctd_gene_disease_direct


def run():
    ctd_path = "downloads/target-disease/CTD_genes_diseases.tsv.gz"

    # create table
    target_disease = ctd_target_disease(ctd_path)

    # write to S3
    output = "stage-0/ctd_target_disease.parquet"

    utils.write_parquet(target_disease, output)
