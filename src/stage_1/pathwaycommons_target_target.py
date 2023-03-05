import pyarrow as pa
import pyarrow.compute as pc

from ..utils import utils


def get_pathwaycommons(pathwaycommons_path: str):
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
                "gene_name_1",
                "interaction_type",
                "gene_name_2",
                "source",
                "pubmed_evidence",
            ]
        )
    )

    return pathwaycommons


def run():
    pathwaycommons_path = "downloads/target-target/PathwayCommons12.All.hgnc.txt.gz"

    pathwaycommons = get_pathwaycommons(pathwaycommons_path)

    print(pathwaycommons[0:3])
