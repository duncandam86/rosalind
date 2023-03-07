from ..utils import utils


def get_disgenet_target_disease(
    disgenet_target_disease_path: str,
    disgenet_disease_attr_path: str,
    disgenet_gene_attr_path: str,
):
    # get disgenet's target disease
    target_disease = utils.read_parquet(disgenet_target_disease_path).select(
        [
            "geneNID",
            "diseaseNID",
            "score",
            "source",
            "associationType",
            "pmid",
            "sentence",
        ]
    )

    disease = utils.read_parquet(disgenet_disease_attr_path).select(
        ["diseaseNID", "diseaseId"]
    )

    gene = utils.read_parquet(disgenet_gene_attr_path).select(["geneNID", "geneId"])

    disgenet_target_disease = (
        target_disease.join(right_table=disease, keys="diseaseNID", use_threads=True)
        .join(right_table=gene, keys="geneNID", use_threads=True)
        .select(
            [
                "geneId",
                "diseaseId",
                "score",
                "source",
                "associationType",
                "pmid",
                "sentence",
            ]
        )
        .rename_columns(
            [
                "gene_id",
                "disease_id",
                "score",
                "source",
                "association_type",
                "pubmed_evidence",
                "sentence_evidence",
            ]
        )
    )

    return disgenet_target_disease


def run():
    # paths to files
    disgenet_target_disease_path = "stage-0/geneDiseaseNetwork.parquet"
    disgenet_disease_attr_path = "stage-0/diseaseAttributes.parquet"
    disgenet_gene_attr_path = "stage-0/geneAttributes.parquet"

    # create disgenet's target_disease association
    target_disease = get_disgenet_target_disease(
        disgenet_target_disease_path,
        disgenet_disease_attr_path,
        disgenet_gene_attr_path,
    )

    # output
    output_path = "stage-1/disgenet_target_disease.parquet"

    # write to s3
    utils.write_parquet(target_disease, output_path)
