from ..utils.log_utils import logger
from ..utils import utils

import pyarrow as pa
import pyarrow.compute as pc


def get_uniprot(uniprot_path: str) -> pa.table:
    """
    Function to load uniprot file from S3
    Args:
      uniprot_path: S3 path to uniprot file in
    Return:
      PyArrow table for uniprot
    """

    table = utils.read_csv(path=uniprot_path, delimiter="\t")

    # get selected column
    uniprot_id = table.column("Entry")
    uniprot_entry_name = table.column("Entry Name")
    protein_name = table.column("Protein names")
    reviewed = table.column("Reviewed")
    length = table.column("Length")
    gene_symbol = table.column("Gene Names (primary)")
    pathway = table.column("Pathway")

    # create uniprot dataframe
    df_uniprot = pa.table(
        [
            uniprot_id,
            uniprot_entry_name,
            protein_name,
            reviewed,
            length,
            gene_symbol,
            pathway,
        ],
        names=[
            "uniprot_id",
            "uniprot_entry_name",
            "protein_name",
            "reviewed",
            "length",
            "gene_symbol",
            "pathway",
        ],
    )

    return df_uniprot


def run():
    uniprot_path = "rosalind-pipeline/downloads/uniprot/uniprot_human.tsv"

    uniprot = get_uniprot(uniprot_path)

    # write file to s3
    otuput_path = "rosalind-pipeline/stage-0/proteins.parquet"

    utils.write_parquet(uniprot, output_path)
