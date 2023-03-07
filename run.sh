#! /bin/bash

while getopts "t:" opt; do
  case "$opt" in
  t) TYPE=$OPTARG ;;
  esac
done

#run stage-1
STAGE_TD=1_td
STEPS_TD=("ctd_target_disease" "psygene_target_disease" "ttd_target_disease"
          "jensenlab_target_disease" "disgenet_target_disease")

#run stage_1_tt
STAGE_TT=1_tt
STEPS_TT=("human_interactome_target_target" "string_target_target"
          "biogrid_target_target" "pathwaycommons_target_target")

run!(){
  local STAGE="$1"
  shift 1
  local STEPS=("$@")

  for STEP in ${STEPS[@]}; do
    python3 main.py --stage $STAGE --step $STEP
  done
}



if [[ $TYPE == "1_td" ]]; then
  run! $STAGE_TD ${STEPS_TD[@]}
elif [[ $TYPE == "1_tt" ]]; then
  run! $STAGE_TT ${STEPS_TT[@]}
fi


