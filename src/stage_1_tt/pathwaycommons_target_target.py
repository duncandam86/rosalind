import pyarrow as pa
import pyarrow.compute as pc
from prefect import task

from ..utils import utils


def get_pathwaycommons(pathwaycommons_path: str, gene_path: str):
    # load pathwaycommons table
    pathwaycommons = (
        utils.read_csv(
            pathwaycommons_path,
            compression="gzip",
            delimiter="\t",
            fs_type="local",
            bucket="./resources",
            read_mode="pandas",
        )
        .select(
            [
                "PARTICIPANT_A",
                "INTERACTION_TYPE",
                "PARTICIPANT_B",
                "INTERACTION_DATA_SOURCE",
                "INTERACTION_PUBMED_ID",
            ]
        )
        .rename_columns(
            [
                "gene_symbol_1",
                "association_type",
                "gene_symbol_2",
                "source",
                "pubmed_evidence",
            ]
        )
    )

    genes = utils.read_parquet(gene_path).select(["gene_symbol", "gene_id"])

    pathwaycommons = (
        pathwaycommons.join(
            right_table=genes.rename_columns(["gene_symbol_1", "gene_id_1"]),
            keys="gene_symbol_1",
            join_type="inner",
        )
        .join(
            right_table=genes.rename_columns(["gene_symbol_2", "gene_id_2"]),
            keys="gene_symbol_2",
            join_type="inner",
        )
        .select(
            ["gene_id_1", "gene_id_2", "association_type", "source", "pubmed_evidence"]
        )
    )

    return pathwaycommons

@task(name="pathwaycommons_target_target")
def run():
    pathwaycommons_path = "downloads/target-target/PathwayCommons12.All.hgnc.txt.gz"
    gene_path = "stage-0/entrez_genes.parquet"

    pathwaycommons = get_pathwaycommons(pathwaycommons_path, gene_path)

    output_path = "stage-1/pathwaycommons_target_target.parquet"

    utils.write_parquet(pathwaycommons, output_path)
