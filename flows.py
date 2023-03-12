import asyncio
from prefect import flow

from src import stage_0
from src import stage_1_td
from src import stage_1_tt

@flow(name="stage_0")
def run_stage_0():
  stage_0.disgenet.run.submit()
  stage_0.entrez_genes.run.submit()
  stage_0.string_protein.run.submit()
  stage_0.uniprot_proteins.run.submit()

@flow(name="stage_1_td")
async def run_stage_1td():
  stage_1_td.ctd_target_disease.run.submit()
  stage_1_td.disgenet_target_disease.run.submit()
  stage_1_td.jensenlab_target_disease.run.submit()
  stage_1_td.ttd_target_disease.run.submit()
  await asyncio.sleep(1)

@flow(name="stage_1_tt")
async def run_stage_1tt():
  stage_1_tt.biogrid_target_target.run.submit()
  stage_1_tt.human_interactome_target_target.run.submit()
  stage_1_tt.pathwaycommons_target_target.run.submit()
  stage_1_tt.string_target_target.run.submit()
  await asyncio.sleep(1)


@flow(name="pipeline")
async def run_pipeline():
  #run stage_0 flow
  run_stage_0()

  #run stage_1 flows in parallel
  stage_1_flows = [run_stage_1td(), run_stage_1tt()]
  await asyncio.gather(*stage_1_flows)

  
if __name__ == "__main__":
  asyncio.run(run_pipeline())