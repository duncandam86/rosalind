import pyarrow as pa
import pyarrow.compute as pc
from typing import List
from pyarrow_ops import drop_duplicates

from ..utils import utils


def get_gene(entrez_gene_path):
    entrez_genes = utils.read_parquet(entrez_gene_path).select(
        ["gene_id", "ensembl_id"]
    )

    ensembl_id = pa.array(
        [
            ensembl.split(":")[-1]
            for ensembl in entrez_genes.column("ensembl_id").to_pylist()
        ]
    )

    entrez_genes = entrez_genes.drop(["ensembl_id"]).append_column(
        "ensembl_id", ensembl_id
    )

    return entrez_genes


def get_human_interactome_target_target(paths: List[str]):
    human_interactome = []

    for path in paths:
        hi = utils.read_csv(
            path, delimiter="\t", column_names=["ensembl_id_1", "ensembl_id_2"]
        )

        human_interactome.append(hi)

    human_interactome_target_target = pa.concat_tables(human_interactome)

    return human_interactome_target_target


def run():
    entrez_gene_path = "stage-0/entrez_genes.parquet"
    human_interactome_hi_05_path = "downloads/target-target/H-I-05.tsv"
    human_interactome_hi_ii_14_path = "downloads/target-target/HI-II-14.tsv"
    human_interactome_hi_union = "downloads/target-target/HI-union.tsv"
    human_interactome_litbm = "downloads/target-target/Lit-BM.tsv"
    human_interactome_venka = "downloads/target-target/Venkatesan-09.tsv"
    human_interactome_yang = "downloads/target-target/Yang-16.tsv"
    human_interactome_yu = "downloads/target-target/Yu-11.tsv"

    # create human-interactome
    human_interactome_target_target = get_human_interactome_target_target(
        [
            human_interactome_hi_05_path,
            human_interactome_hi_ii_14_path,
            human_interactome_hi_union,
            human_interactome_litbm,
            human_interactome_venka,
            human_interactome_yang,
            human_interactome_yu,
        ]
    )

    # deduplicates
    human_interactome_target_target = drop_duplicates(
        human_interactome_target_target,
        on=["ensembl_id_1", "ensembl_id_2"],
        keep="first",
    )

    # load entrez gene
    entrez_gene = get_gene(entrez_gene_path)

    # mapping ensembl id into entrez id
    human_interactome_target_target = (
        human_interactome_target_target.join(
            right_table=entrez_gene.rename_columns(["gene_id_1", "ensembl_id_1"]),
            keys="ensembl_id_1",
            join_type="inner",
        )
        .join(
            right_table=entrez_gene.rename_columns(["gene_id_2", "ensembl_id_2"]),
            keys="ensembl_id_2",
            join_type="inner",
        )
        .select(["gene_id_1", "gene_id_2"])
    )

    # add source column and append
    source = pa.array(["human_interactome"] * human_interactome_target_target.shape[0])

    human_interactome_target_target = human_interactome_target_target.append_column(
        "source", source
    )

    # output path
    output_path = "stage-1/human_interactome_target_target.parquet"

    # write
    utils.write_parquet(human_interactome_target_target, output_path)
