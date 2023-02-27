import pyarrow as pa
import pyarrow.compute as pc

from ..utils import utils


def get_string_target_target(string_target_target_path: str, entrez_gene_path: str, string_protein_path:str):
    # load string target-target and rename
    string_target = (
        utils.read_csv(string_target_target_path, delimiter=" ", compression="gzip")
        .select(["protein1", "protein2", "combined_score"])
        .rename_columns(["string_id_1", "string_id_2", "combined_score"])
    )

    # reformat string_id
    string_id_1 = pa.array(
        [p.split(".")[-1] for p in string_target.column("string_id_1").to_pylist()]
    )
    string_id_2 = pa.array(
        [p.split(".")[-1] for p in string_target.column("string_id_2").to_pylist()]
    )

    # add new columns
    string_target_target = (
        string_target.drop(["string_id_1", "string_id_2"])
        .append_column("string_id_1", string_id_1)
        .append_column("string_id_2", string_id_2)
    )

    # get mapping of string's protein id and entrez gene's id
    string_protein = utils.read_parquet(entrez_gene_path).select(["gene_symbol", "gene_id"]).join(
        right_table=utils.read_parquet(string_protein_path),
        keys=["gene_symbol"],
        join_type="inner"
    ).select(["gene_id", "string_id"])

    #create target_target 
    target_target = string_target_target.join(
        right_table=string_protein.rename_columns(["gene_id_1", "string_id_1"]),
        keys="string_id_1",
        join_type="inner"
    ).join(
        right_table=string_protein.rename_columns(["gene_id_2", "string_id_2"]),
        keys="string_id_2",
        join_type="inner"
    ).select(["gene_id_1", "gene_id_2", "combined_score"])

    return target_target


def run():
    entrez_gene_path = "stage-0/entrez_genes.parquet"
    string_protein_path = "stage-0/string_proteins.parquet"
    string_target_target_path = (
        "downloads/target-target/9606.protein.links.full.v11.5.txt.gz"
    )

    # output path
    output_path = "stage-0/string_target_target.parquet"

    target_target = get_string_target_target(
        string_target_target_path,
        entrez_gene_path,
        string_protein_path
    )

    utils.write_parquet(target_target, output_path)