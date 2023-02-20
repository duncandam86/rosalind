import pyarrow as pa
import pyarrow.compute as pc

from ..utils import utils


def jensen_knowledge_target_disease(knowledge_target_disease_path: str):
    # load jensenlab knowledge target-disease
    knowledge_target_disease = utils.read_csv(
        knowledge_target_disease_path,
        delimiter="\t",
        column_names=[
            "other_id",
            "gene_symbol",
            "disease_id",
            "disease_name",
            "source",
            "type",
            "confidence",
        ],
    ).select(["gene_symbol", "disease_id", "source", "confidence", "type"])

    return knowledge_target_disease


def jensen_textmining_target_disease(textmining_target_disease_path: str):
    # load jensenlab textmining target-disease
    textmining_target_disease = utils.read_csv(
        textmining_target_disease_path,
        delimiter="\t",
        column_names=[
            "other_id",
            "gene_symbol",
            "disease_id",
            "disease_name",
            "z-score",
            "confidence",
            "source",
        ],
    ).select(["gene_symbol", "disease_id", "confidence", "source"])

    # round up confidence column
    confidence = pc.cast(
        pc.round(textmining_target_disease.column("confidence")), pa.int64()
    )
    textmining_target_disease = textmining_target_disease.drop(
        ["confidence"]
    ).append_column("confidence", confidence)

    # create type column
    n_row = textmining_target_disease.shape[0]
    type = pa.array(["textmining"] * n_row, pa.string())
    textmining_target_disease = textmining_target_disease.append_column("type", type)

    return textmining_target_disease


def jensen_experiment_target_disease(experiment_target_disease_path: str):
    experiment_target_disease = utils.read_csv(
        experiment_target_disease_path,
        delimiter="\t",
        column_names=[
            "other_id",
            "gene_symbol",
            "disease_id",
            "disease_name",
            "source",
            "mean_rank_score",
            "confidence",
        ],
    ).select(["gene_symbol", "disease_id", "source", "confidence"])

    # round up confidence column
    confidence = pc.cast(
        pc.round(experiment_target_disease.column("confidence")), pa.int64()
    )
    experiment_target_disease = experiment_target_disease.drop(
        ["confidence"]
    ).append_column("confidence", confidence)

    # create type column
    n_row = experiment_target_disease.shape[0]
    type = pa.array(["experiment"] * n_row, pa.string())
    experiment_target_disease = experiment_target_disease.append_column("type", type)

    return experiment_target_disease

def get_entrez_gene(
  entrez_gene_path:str
):
    entrez_genes = utils.read_parquet(
      entrez_gene_path
    )

    #gene_id and symbol
    gene_id_symbol = entrez_genes.select(["gene_id", "gene_symbol"])
    #gene_id and alternative symbols
    gene_id_alt_symbol = entrez_genes.select(["gene_id", "alternative_symbols"])
    gene_symbol = pc.split_pattern(gene_id_alt_symbol.column("alternative_symbols"), "|")
    gene_id_alt_symbol = gene_id_alt_symbol.drop(["alternative_symbols"]).append_column("gene_symbol", gene_symbol)
    gene_id_alt_symbol = utils.explode(gene_id_alt_symbol, "gene_symbol")
    #gene_id and accession
    gene_id_accession = entrez_genes.select(["gene_id", "protein_accession"])
    gene_symbol = pc.split_pattern(gene_id_accession.column("protein_accession"), "|")
    gene_id_accession = gene_id_accession.drop(["protein_accession"]).append_column("gene_symbol", gene_symbol)
    gene_id_accession = utils.explode(gene_id_accession, "gene_symbol")
    #gene id mapping
    genes = pa.concat_tables(
      [gene_id_symbol, gene_id_alt_symbol, gene_id_accession]
    )
    
    return genes

def run():
    knowledge_target_disease_path = (
        "downloads/target-disease/human_disease_knowledge_filtered.tsv"
    )

    textmining_target_disease_path = (
        "downloads/target-disease/human_disease_textmining_filtered.tsv"
    )

    experiment_target_disease_path = (
        "downloads/target-disease/human_disease_experiments_filtered.tsv"
    )

    entrez_gene_path = "stage-0/entrez_genes.parquet"

    #load gene mapping
    genes = get_entrez_gene(entrez_gene_path)

    # process all jensenlab target-disease data
    knowledge_target_disease = jensen_knowledge_target_disease(
        knowledge_target_disease_path
    )

    textmining_target_disease = jensen_textmining_target_disease(
        textmining_target_disease_path
    )

    experiment_target_disease = jensen_experiment_target_disease(
        experiment_target_disease_path
    )

    # concat all tables
    jensen_target_disease = pa.concat_tables(
        [knowledge_target_disease, textmining_target_disease, experiment_target_disease]
    )

    # reformat disease_id
    disease_id = pc.replace_substring(
        jensen_target_disease.column("disease_id"), ":", "_"
    )

    jensen_target_disease = jensen_target_disease.drop(["disease_id"]).append_column(
        "disease_id", disease_id
    ).join(
      right_table=genes, keys = "gene_symbol", use_threads=True
    )

    # write file to s3
    s3_path = "stage-0/jensenlab_target_disease.parquet"

    utils.write_parquet(jensen_target_disease, s3_path)