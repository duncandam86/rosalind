#!/bin/bash

while getopts "s:a:" opt; do
  case "$opt" in
  s) SOURCE=$OPTARG ;;
  a) ACTION=$OPTARG ;;
  esac
done

if [[ -z $SOURCE ]]; then
  SOURCE="all"
fi

#load .env variables
source .env

INACT=($INACT_AZ_URL $INACT_CANCER_URL $INACT_CORONA_URL $INACT_CROHN_URL
       $INACT_DIABETES_URL $INACT_IBD_URL $INACT_PARKINSON_URL $INACT_HUNTINGTON_URL 
       $INACT_RARE_URL $INACT_NEURODEGEN_URL $INACT_NEURODEVE_URL)
FULL_INT=($PHARMGKB_URL)
TARGET_DISEASE=($TDD_TD_URL $PSYGENE_TD_URL $CTD_TD_URL $JENSEN_TD_TEM_URL $JENSEN_TD_KNO_URL $JENSEN_TD_EXP_URL)
TARGET_COMPOUND=($TTD_TC_URL_MOA $TTD_TC_URL_ACT $DGI_TC_URL
                 $CTD_TC_URL $BINDING_TC_CURATED_URL $BINDING_TC_PATENT_URL
                 $BINDING_TC_UCSD_URL $STICH_TC_URL $INSXGHT_TC_URL)
TARGET_TARGET=($STRING_TT_URL $PATHWAY_COMMON_TT_URL $BIOGRID_TT_URL
               $HI_HURI_URL $HI_LITBM_URL $HI_YANG_URL $HI_HI_II_14_URL
               $HI_YU_11_URL $HI_VEN_09_URL $HI_I_O5_URL)
DISEASE_COMPOUND=($TTD_DC_URL $CTD_DC_URL)
DISEASE_BIOMARKER=($TTD_DB_URL)
PATHWAY_DISEASE=($CTD_PD_URL)
PATHWAY_COMPOUND=($PHARMGKB_PC_URL $CTD_PC_URL $CTD_GO_URL)
PATHWAY_TARGET=($PATHBANK_PT_URL $CTD_PT_URL $REACTOME_PT_URL)
COMPOUND_COMPOUND=($STICH_CC_URL)
LITERATURE=($PHARMGKB_LIT_URL)
ENTREZ=( $ENTREZ_GENE_INFO_URL $ENTREZ_GENE2ENSEMBLE_URL $ENTREZ_GENE2ACCESSION_URL)
UNIPROT=($UNIPROT_HUMAN_URL)
HGNC=($HGNC_URL)
TTD=($TTD_TARGET_URL)
DISGENET=($DISGEN_URL)

download-data() {
  
  local SOURCE="$1"
  local DECOMPRESS="$2"
  shift 2
  local ARRAY_URL=("$@")
  
  OUTPUT_PATH=./resources/downloads

  mkdir -p $OUTPUT_PATH/$SOURCE
  
  for URL in ${ARRAY_URL[@]}; do
    FILE=$(echo $URL | rev | cut -d"/" -f 1 | rev)
    echo "Downloading $FILE"
    if [[ $URL == *"uniprotkb"* ]]; then
      wget $URL -O ./resources/downloads/$SOURCE/uniprot_human.tsv
    elif [[ $URL == *"disgenet"* ]]; then
      wget $URL -O ./resources/downloads/$SOURCE/disgenet_2020.db.gz
      FILE=disgenet_2020.db.gz
    else
      wget $URL -O ./resources/downloads/$SOURCE/$FILE
    fi

    if [[ $DECOMPRESS == "true" ]]; then
      if [[ $FILE == *".zip"* ]]; then
        echo "Unzipping $FILE"
        unzip -o -d $OUTPUT_PATH/$SOURCE $OUTPUT_PATH/$SOURCE/$FILE
        rm $OUTPUT_PATH/$SOURCE/$FILE
        echo "Finished unzipping"
    
      elif [[ $FILE == *".tar.gz"* ]]; then
        echo "Unzipping $FILE"
        tar -xvzf $OUTPUT_PATH/$SOURCE/$FILE -C $OUTPUT_PATH/$SOURCE
        rm $OUTPUT_PATH/$SOURCE/$FILE
        echo "Finished unzipping"

      elif [[ $FILE == *".gz"* ]]; then
        echo "Unzipping $FILE"
        gunzip $OUTPUT_PATH/$SOURCE/$FILE
        rm $OUTPUT_PATH/$SOURCE/$FILE
        echo "Finished unzipping"
      fi
    fi
  done
}

upload-s3() {
  SOURCE=$1
  OUTPUT_PATH=./resources/downloads
  
  echo "Uploading $SOURCE to S3"
  #upload to S3
  aws-vault exec rosalind \
  -- aws s3 sync $OUTPUT_PATH/$SOURCE s3://rosalind-pipeline/downloads/$SOURCE/ --delete
  echo "Finished uploading $SOURCE"
}  

execute!(){
  local SOURCE="$1"
  local DECOMPRESS="$2"
  shift 2
  local ARRAY_URL=("$@")
  
  if [[ -z $ACTION ]]; then
    download-data "$SOURCE" "$DECOMPRESS" "${ARRAY_URL[@]}"
    upload-s3 $SOURCE
  elif [[ $ACTION = "download" ]]; then
    download-data "$SOURCE" "$DECOMPRESS" "${ARRAY_URL[@]}"
  elif [[ $ACTION = "upload" ]]; then
    upload-s3 $SOURCE
  fi
}

if [[ $SOURCE == "inact" ]]; then
  execute! "$SOURCE" true "${INACT[@]}" 
elif [[ $SOURCE == "full-int" ]]; then
  execute! "$SOURCE" true "${FULL_INT[@]}"
elif [[ $SOURCE == "disgenet" ]]; then
  execute! "$SOURCE" true "${DISGENET[@]}"
elif [[ $SOURCE == "target-disease" ]]; then
  execute! "$SOURCE" false "${TARGET_DISEASE[@]}" 
elif [[ $SOURCE == "target-compound" ]]; then
  execute! "$SOURCE" false "${TARGET_COMPOUND[@]}" 
elif [[ $SOURCE == "target-target" ]]; then
  execute! "$SOURCE" false "${TARGET_TARGET[@]}" 
elif [[ $SOURCE == "disease-compound" ]]; then
  execute! "$SOURCE" false "${DISEASE_COMPOUND[@]}" 
elif [[ $SOURCE == "disease-biomarker" ]]; then
  execute! "$SOURCE" false "${DISEASE_BIOMARKER[@]}" 
elif [[ $SOURCE == "pathway-disease" ]]; then
  execute! "$SOURCE" false "${PATHWAY_DISEASE[@]}"
elif [[ $SOURCE == "pathway-compound" ]]; then
  execute! "$SOURCE" false "${PATHWAY_COMPOUND[@]}"
elif [[ $SOURCE == "pathway-target" ]]; then
  execute! "$SOURCE" false "${PATHWAY_TARGET[@]}" 
elif [[ $SOURCE == "compound-compound" ]]; then
  execute! "$SOURCE" false "${COMPOUND_COMPOUND[@]}" 
elif [[ $SOURCE == "literature" ]]; then
  execute! "$SOURCE" false "${LITERATURE[@]}" 
elif [[ $SOURCE == "entrez" ]]; then
  execute! "$SOURCE" false "${ENTREZ[@]}"
elif [[ $SOURCE == "uniprot" ]]; then
  execute! "$SOURCE" false "${UNIPROT[@]}"
elif [[ $SOURCE == "hgnc" ]]; then
  execute! "$SOURCE" false "${HGNC[@]}" 
elif [[ $SOURCE == "ttd" ]]; then
  execute! "$SOURCE" false "${TTD[@]}" 
elif [[ $SOURCE == "all" ]]; then
  execute! "inact" true "${INACT[@]}"
  execute! "full-int" true "${FULL_INT[@]}" 
  execute! "target-disease" false "${TARGET_DISEASE[@]}" 
  execute! "target-compound" false "${TARGET_COMPOUND[@]}" 
  execute! "target-target" false "${TARGET_TARGET[@]}" 
  execute! "disease-compound" false "${DISEASE_COMPOUND[@]}" 
  execute! "disease-biomarker" false "${DISEASE_BIOMARKER[@]}" 
  execute! "pathway-disease" false "${PATHWAY_DISEASE[@]}" 
  execute! "pathway-compound" false "${PATHWAY_COMPOUND[@]}" 
  execute! "pathway-target" false "${PATHWAY_TARGET[@]}" 
  execute! "compound-compound" false "${COMPOUND_COMPOUND[@]}" 
  execute! "literature" false "${LITERATURE[@]}" 
  execute! "entrez" false "${ENTREZ[@]}" 
  execute! "uniprot" false "${UNIPROT[@]}" 
  execute! "hgnc" false "${HGNC[@]}" 
fi
