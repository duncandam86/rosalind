import pyarrow as pa
import pyarrow.compute as pc
from prefect import task

from ..utils import utils

def get_uniprot(uniprot_path: str) -> pa.table:
    """
    Function to load uniprot file from S3
    Args:
      uniprot_path: S3 path to uniprot file in
    Return:
      PyArrow table for uniprot
    """

    df_uniprot = (
        utils.read_csv(path=uniprot_path, delimiter="\t")
        .select(
            [
                "Entry",
                "Entry Name",
                "Protein names",
                "Reviewed",
                "Length",
                "Gene Names (primary)",
                "Pathway",
            ]
        )
        .rename_columns(
            [
                "uniprot_id",
                "uniprot_entry_name",
                "protein_name",
                "reviewed",
                "length",
                "gene_symbol",
                "pathway",
            ]
        )
    )

    return df_uniprot

@task(name="uniprot_proteins")
def run():
    uniprot_path = "downloads/uniprot/uniprot_human.tsv"

    # load uniprot
    uniprot = get_uniprot(uniprot_path)

    # write file to s3
    output_path = "stage-0/uniprot_proteins.parquet"

    utils.write_parquet(uniprot, output_path)
