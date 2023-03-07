import pyarrow as pa
import pyarrow.compute as pc

from ..utils import utils


def ttd_target_disease(psygene_path: str):
    # load file in pandas mode to pyarrow table
    ttd_target_disease = utils.read_csv(
        psygene_path,
        delimiter="\t",
        skip_rows=22,
        column_names=["c_0", "c_1", "c_2", "c_3"],
        read_mode="pandas",
    )

    # get target_id
    target_id = (
        ttd_target_disease.filter(pc.field("c_1") == "TARGETID")
        .select(["c_0", "c_2"])
        .rename_columns(["id", "gene_id"])
    )

    # get targe_name
    target_name = (
        ttd_target_disease.filter(pc.field("c_1") == "TARGNAME")
        .select(["c_0", "c_2"])
        .rename_columns(["id", "gene_name"])
    )

    # get phase of study
    target_phase = (
        ttd_target_disease.filter(pc.field("c_1") == "INDICATI")
        .select(["c_0", "c_2"])
        .rename_columns(["id", "phase_evidence"])
    )

    # get disease
    disease = (
        ttd_target_disease.filter(pc.field("c_1") == "INDICATI")
        .select(["c_0", "c_3"])
        .rename_columns(["id", "ttd_disease"])
    )

    # recreate table
    ttd_target_disease = (
        target_id.join(right_table=target_name, keys="id", use_threads=True)
        .join(right_table=target_phase, keys="id", use_threads=True)
        .join(right_table=disease, keys="id", use_threads=True)
    )

    # get disease_id
    ttd_disease = pc.split_pattern(
        ttd_target_disease.column("ttd_disease"), "ICD-11:"
    ).to_pylist()
    ttd_disease_id = pc.utf8_ltrim_whitespace(
        pa.array([a[-1].replace("]", "") for a in ttd_disease])
    )
    ttd_disease_id = pc.split_pattern(ttd_disease_id, "-")
    # get disease_name
    ttd_disease_name = pc.utf8_rtrim_whitespace(
        pa.array([a[0].replace("[", "") for a in ttd_disease])
    )

    # append column
    ttd_target_disease = ttd_target_disease.append_column(
        "disease_id", ttd_disease_id
    ).append_column("disease_name", ttd_disease_name)

    # explode disease_id column
    ttd_target_disease = utils.explode(ttd_target_disease, "disease_id")

    # refomat disease_id
    disease_id = pa.array(
        [f"ICD11_{id}" for id in ttd_target_disease.column("disease_id").to_pylist()]
    )

    # add new columns
    ttd_target_disease = ttd_target_disease.drop(
        ["disease_id", "ttd_disease"]
    ).append_column("disease_id", disease_id)

    return ttd_target_disease


def ttd_target(ttd_target_path: str):
    ttd_target = utils.read_csv(
        ttd_target_path,
        delimiter="\t",
        skip_rows=22,
        column_names=["c_0", "c_1", "c_2"],
        read_mode="pandas",
    )

    # filter for uniprot_id
    ttd_target = (
        ttd_target.filter(pc.field("c_1") == "UNIPROID")
        .select(["c_0", "c_2"])
        .rename_columns(["id", "protein_id"])
    )

    protein_id = pc.split_pattern(ttd_target.column("protein_id"), ";")

    # update new column
    ttd_target = ttd_target.drop(["protein_id"]).append_column("protein_id", protein_id)
    # explode column
    ttd_target = utils.explode(ttd_target, "protein_id")

    return ttd_target


def run():
    ttd_path = "downloads/target-disease/P1-06-Target_disease.txt"
    ttd_target_path = "downloads/ttd/P2-01-TTD_uniprot_all.txt"

    # create table
    target = ttd_target(ttd_target_path)
    target_disease = ttd_target_disease(ttd_path)

    # create final table
    target_disease = target_disease.join(
        right_table=target, keys="id", use_threads=True
    ).select(
        ["protein_id", "gene_name", "disease_id", "disease_name", "phase_evidence"]
    )

    # write to S3
    output = "stage-1/ttd_target_disease.parquet"

    utils.write_parquet(target_disease, output)
