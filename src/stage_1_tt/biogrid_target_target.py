import pyarrow as pa
import pyarrow.compute as pc
from prefect import task

from ..utils import utils


def get_biogrid_target_target(biogrid_target_target_path: str):

    biogrid_target_target = (
        utils.read_csv(
            biogrid_target_target_path,
            bucket="./resources",
            delimiter="\t",
            fs_type="local",
        )
        .filter(
            (pc.field("Organism Interactor A") == 9606)
            & (pc.field("Organism Interactor B") == 9606)
        )
        .select(
            [
                "Entrez Gene Interactor A",
                "Entrez Gene Interactor B",
                "Qualifications",
                "Pubmed ID",
                "Score",
                "Source Database",
            ]
        )
        .rename_columns(
            [
                "gene_id_1",
                "gene_id_2",
                "phenotypes",
                "pubmed_evidence",
                "score",
                "source",
            ]
        )
    )

    # changing all "-" to null in all columns
    array_columns = []
    for column in biogrid_target_target.column_names:
        new_column = pa.array(
            [
                v if v != "-" else None
                for v in biogrid_target_target.column(column).to_pylist()
            ]
        )
        array_columns.append(new_column)

    # conver to pandas dataframe
    biogrid_target_target = pa.table(
        array_columns, biogrid_target_target.column_names
    ).to_pandas()

    # groupby gene_id_1 and gene_id_2 and rank by highest score
    biogrid_target_target["rank"] = biogrid_target_target.groupby(
        ["gene_id_1", "gene_id_2"]
    )["score"].rank("max")

    # convert dataframe to pyarrow and filter rank = 1
    biogrid_target_target = (
        pa.table(biogrid_target_target).filter(pc.field("rank") == 1).drop(["rank"])
    )

    return biogrid_target_target

@task(name="biogrid_target_target")
def run():
    biogrid_target_target_path = "downloads/target-target/BIOGRID-ALL-4.4.218.tab2.txt"

    biogrid = get_biogrid_target_target(biogrid_target_target_path)

    # output path
    output_path = "stage-1/biogrid_target_target.parquet"

    # write out results
    utils.write_parquet(biogrid, output_path)
