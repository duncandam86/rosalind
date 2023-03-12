from ..utils.log_utils import logger
from ..utils import utils

import pyarrow as pa
import pyarrow.compute as pc
from prefect import task

def get_gene_info(
    entrez_gene_info_path: str,
) -> pa.table:
    """
    Function to wrangle gene_info table from entrez
    Args:
      entrez_gene_info_path: S3 path to entrez's gene_info file
    Return:
      PyArrow table with gene_info information
    """
    table = utils.read_csv(
        path=entrez_gene_info_path, compression="gzip", delimiter="\t"
    )

    # getting columns for new gene_info
    gene_id = table.column("GeneID")
    gene_name = table.column("description")
    gene_symbol = table.column("Symbol")
    chromosome = table.column("chromosome")
    map_location = table.column("map_location")
    type_of_gene = table.column("type_of_gene")
    alt_symbols = pc.replace_substring(table.column("Synonyms"), "|", ";")
    synonyms = pc.binary_join_element_wise(
        pc.cast(table["Full_name_from_nomenclature_authority"], pa.string()),
        pc.cast(table["Other_designations"], pa.string()),
        "|",
    )
    dbXrefs = pc.split_pattern(table.column("dbXrefs"), "|")

    # create new gene_info table
    gene_info = pa.table(
        [
            gene_id,
            gene_name,
            gene_symbol,
            chromosome,
            map_location,
            type_of_gene,
            alt_symbols,
            synonyms,
            dbXrefs,
        ],
        names=[
            "gene_id",
            "gene_name",
            "gene_symbol",
            "chromosome",
            "map_location",
            "type_of_gene",
            "alternative_symbols",
            "synonym",
            "ensembl_id",
        ],
    )

    # unpack array column contains ensembl_id
    gene_info = utils.explode(table=gene_info, column="ensembl_id")

    # filter for for only ensembl_id value
    gene_info = gene_info.filter(
        pc.starts_with(gene_info["ensembl_id"], pattern="ensembl", ignore_case=True)
    )

    # process all columns, replacing "-" with Null value
    array_columns = []
    for col in gene_info.column_names:
        array_col = pa.array(
            [v if v != "-" else None for v in gene_info.column(col).to_pylist()]
        )
        array_columns.append(array_col)

    # create final gene_info table
    gene_info = pa.table(array_columns, gene_info.column_names)

    return gene_info


def get_hgnc(hgnc_path: str) -> pa.table:
    """
    Function to load HGNC file from S3 and wrangle it
    Args:
      hgnc_path: path to HGNC file in S3
    Return
      PyArrow table contains HGNC data
    """
    table = utils.read_csv(path=hgnc_path, delimiter="\t")

    # select hgnc_id and symbol columns from HGNC
    hgnc_id = table.column("hgnc_id")
    gene_symbol = table.column("symbol")

    # load selected column HGNC table
    hgnc = pa.table([hgnc_id, gene_symbol], names=["hgnc_id", "gene_symbol"])

    return hgnc


def get_gene_accession(entrez_accession_path: str):
    gene_accession = utils.read_csv(
        path=entrez_accession_path,
        compression="gzip",
        delimiter="\t",
        bucket="./resources",
        fs_type="local",
    )

    # filter for human
    gene_accession = (
        gene_accession.filter(
            (pc.field("#tax_id") == 9606)
            & (pc.field("protein_accession.version") != "-")
        )
        .select(["GeneID", "protein_accession.version"])
        .rename_columns(["gene_id", "protein_accession"])
    ).to_pandas()

    # using pandas to group by column to get a list of protein accession for each gene
    gene_accession = (
        gene_accession.groupby("gene_id")["protein_accession"]
        .unique()
        .reset_index(name="protein_accession")
    )
    # recreate pyarrow table
    gene_accession = pa.table(gene_accession)
    # process protein_accession pyarrow column into string seperated by |
    protein_accession = pa.array(
        ["|".join(l) for l in gene_accession.column("protein_accession").to_pylist()]
    )
    # add new protein_accession column back to pyarrow table
    gene_accession = gene_accession.drop(["protein_accession"]).append_column(
        "protein_accession", protein_accession
    )

    return gene_accession

@task(name = "entrez_genes")
def run():
    entrez_gene_info_path = "downloads/entrez/Homo_sapiens.gene_info.gz"
    hgnc_path = "downloads/hgnc/gene_with_protein_product.txt"
    entrez_accession_path = "downloads/entrez/gene2accession.gz"

    # load tables
    df_gene_info = get_gene_info(entrez_gene_info_path)
    df_hgnc = get_hgnc(hgnc_path)
    df_accession = get_gene_accession(entrez_accession_path)

    # final gene table
    df_gene = df_gene_info.join(
        right_table=df_hgnc, keys="gene_symbol", use_threads=True
    ).join(right_table=df_accession, keys="gene_id", use_threads=True)

    # write file to s3
    s3_path = "stage-0/entrez_genes.parquet"

    utils.write_parquet(df_gene, s3_path)
