from ..utils.log_utils import logger
from ..utils import utils

import pyarrow as pa
import pyarrow.compute as pc

def get_uniprot(
  uniprot_path:str
) -> pa.table:
    """
    Function to load uniprot file from S3
    Args:
      uniprot_path: S3 path to uniprot file in
    Return:
      PyArrow table for uniprot
    """

    table = utils.read_csv(path=uniprot_path, delimiter="\t")

    #get selected column 
    gene_symbol = table.column("Gene Names (primary)")
    protein_name = table.column("Protein names")
    uniprot_entry_name = table.column("Entry Name")
    uniprot_id = table.column("Entry")

    #create uniprot dataframe 
    df_uniprot = pa.table(
      [gene_symbol, protein_name, uniprot_entry_name, uniprot_id],
      names = ["gene_symbol", "protein_name", "uniprot_entry_name", "uniprot_id"]
    )

    return df_uniprot

def main():
    uniprot_path = "rosalind-pipeline/downloads/uniprot/uniprot_human.tsv"

    uniprot = get_uniprot(uniprot_path)

    #write file to s3
    s3_path = "rosalind-pipeline/stage-0/proteins.parquet"

    utils.write_parquet(uniprot, s3_path)
