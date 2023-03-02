from typing import List, Dict, Union
import logging
import argparse
import importlib

logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def run_pipeline_stage(stage: str, step: str):
    STAGE_STEP = {
        "0": {
            "entrez_genes": "entrez_genes.run",
            "uniprot_proteins": "uniprot_proteins.run",
            "disgenet":"disgenet.run",
            "ctd_target_disease": "ctd_target_disease.run",
            "psygene_target_disease": "psygene_target_disease.run",
            "ttd_target_disease": "ttd_target_disease.run",
            "jensenlab_target_disease":"jensenlab_target_disease.run",   
            "disgenet_target_disease":"disgenet_target_disease.run",
            "human_interactome_target_target":"human_interactome_target_target.run",
            "string_protein":"string_protein.run",
            "string_target_target":"string_target_target.run",
            "biogrid_target_target":"biogrid_target_target.run"
        }
    }

    stage_mod_name = f"src.{stage}"

    #get stage map
    stage_map = STAGE_STEP[stage]
    if stage_map is None:
      raise ValueError(
         f"Invalid stage specified, valid stages are: {list(STAGE_STEP.keys())}"
      )
    
    #import module.function from stage module
    fn_path = stage_map[step]
    path_splits = fn_path.split(".")
    submod_name, fn_name = path_splits[0], path_splits[-1]

    #construct import name
    import_name = f"src.stage_{stage}.{submod_name}"

    #import specific module
    step_module = importlib.import_module(import_name)
    #get step function
    process_function = getattr(step_module, fn_name)

    if process_function is None:
      raise ValueError(
          f"Invalid step for stage {stage} specified, valid steps are: {list(stage_map.keys())}"
      )

    logger.info(f"Running pipeline stage {stage}: {step}")

    run_fn = process_function()

    logger.info(f"Finish pipeline stage {stage}: {step}")

    return run_fn

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--stage",
        help="Pipeline stage to run.",
        required=True,
        choices=["0", "1", "2", "3"],
    )
    parser.add_argument(
        "--step",
        help="Step of pipeline stage to run.",
        required=True,
    )

    args = parser.parse_args()

    run_pipeline_stage(stage=args.stage, step=args.step)
