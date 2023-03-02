import pyarrow as pa
from ..utils import utils


def get_string(string_protein_path: str):
    # load string_protein
    string_protein = (
        utils.read_csv(string_protein_path, delimiter="\t", compression="gzip")
        .select(["#string_protein_id", "preferred_name"])
        .rename_columns(["string_id", "gene_symbol"])
    )

    string_id = pa.array(
        [p.split(".")[-1] for p in string_protein.column("string_id").to_pylist()]
    )

    string_protein = string_protein.drop(["string_id"]).append_column(
        "string_id", string_id
    )

    return string_protein


def run():
    string_path = "downloads/string/9606.protein.info.v11.5.txt.gz"
    # load string protein
    string_protein = get_string(string_path)
    # output_path
    output_path = "stage-0/string_proteins.parquet"

    # write out string proteins
    utils.write_parquet(string_protein, output_path)
