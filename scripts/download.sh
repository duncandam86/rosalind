#!/bin/zsh
cleanShell() {
  #clean shell
  if [[ ! -z "$ZSH_VERSION" ]]; then
    exec zsh
  elif [[ ! -z "$BASH_VERSION" ]]; then
    exec bash --login
  fi
}

while getopts "s:" opt; do
  case "$opt" in
  s) SOURCE=$OPTARG ;;
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
FULL_INT=( $PHARMGKB_URL $DISGEN_URL)
TARGET_DISEASE=($TDD_TD_URL $PSYGENE_TD_URL $CTD_TD_URL
                $JENSEN_TD_TEM_URL $JENSEN_TD_KNO_URL $JENSEN_TD_EXP_URL)
TARGET_COMPOUND=($TTD_TC_URL_MOA $TTD_TC_URL_ACT $DGI_TC_URL
                 $CTD_TC_URL $BINDING_TC_CURATED_URL $BINDING_TC_PATENT_URL
                 $BINDING_TC_UCSD_URL $STICH_TC_URL $INSXGHT_TC_URL)
TARGET_TARGET=($STRING_TT_URL $PATHWAY_COMMON_TT_URL $BIOGRID_TT_URL)
DISEASE_COMPOUND=($TTD_DC_URL $CTD_DC_URL)
DISEASE_BIOMARKER=($TTD_DB_URL)
PATHWAY_DISEASE=($CTD_PD_URL)
PATHWAY_COMPOUND=($PHARMGKB_PC_URL $CTD_PC_URL $CTD_GO_URL)
PATHWAY_TARGET=($PATHBANK_PT_URL $CTD_PT_URL $REACTOME_PT_URL)
COMPOUND_COMPOUND=($STICH_CC_URL)
LITERATURE=($PHARMGKB_LIT_URL)

download-data() {
  URL=$1
  OUTPUT_PATH=$2
  
  FILE=$(echo $URL | rev | cut -d"/" -f 1 | rev)
  echo "Downloading $FILE"
  wget $URL -O $OUTPUT_PATH/$FILE
  
  if [[ $FILE == *".zip"* ]]; then
    unzip -o -d $OUTPUT_PATH $OUTPUT_PATH/$FILE
    rm $OUTPUT_PATH/$FILE
  
  elif [[ $FILE == *".tar.gz"* ]]; then
    tar -xvzf $OUTPUT_PATH/$FILE -C $OUTPUT_PATH
    rm $OUTPUT_PATH/$FILE

  elif [[ $FILE == *".gz"* ]]; then
    gunzip $OUTPUT_PATH/$FILE
    rm $OUTPUT_PATH/$FILE
  fi
}

create-output(){
  PREFIX=$1

  OUTPUT_PATH=./resources/downloads/$PREFIX
  if [[ ! -d $OUTPUT_PATH ]]; then
    mkdir $OUTPUT_PATH
  fi

  echo $OUTPUT_PATH
}

upload-s3(){
  SOURCE=$1
  aws-vault exec rosalind -- aws s3 mv ./resources/downloads/$SOURCE s3://rosalind-sources/downloads/$SOURCE --recursive
}  

if [[ $SOURCE == "inact" ]]; then
  for URL in $INACT; do
    download-data $URL $(create-output $SOURCE)
  done
  upload-s3 $SOURCE
elif [[ $SOURCE == "full-int" ]]; then
  for URL in $FULL_INT; do
    download-data $URL $(create-output $SOURCE)
  done
  upload-s3 $SOURCE
elif [[ $SOURCE == "target-disease" ]]; then
  for URL in $TARGET_DISEASE; do
    download-data $URL $(create-output $SOURCE)
  done
  upload-s3 $SOURCE
elif [[ $SOURCE == "target-compound" ]]; then
  for URL in $TARGET_COMPOUND; do
    download-data $URL $(create-output $SOURCE)
  done
  upload-s3 $SOURCE
elif [[ $SOURCE == "target-target" ]]; then
  for URL in $TARGET_TARGET; do
    download-data $URL $(create-output $SOURCE)
  done
  upload-s3 $SOURCE
elif [[ $SOURCE == "disease-compound" ]]; then
  for URL in $DISEASE_COMPOUND; do
    download-data $URL $(create-output $SOURCE)
  done
  upload-s3 $SOURCE
elif [[ $SOURCE == "disease-biomarker" ]]; then
  for URL in $DISEASE_BIOMARKER; do
    download-data $URL $(create-output $SOURCE)
  done
  upload-s3 $SOURCE
elif [[ $SOURCE == "pathway-disease" ]]; then
  for URL in $PATHWAY_DISEASE; do
    download-data $URL $(create-output $SOURCE)
  done
  upload-s3 $SOURCE
elif [[ $SOURCE == "pathway-compound" ]]; then
  for URL in $PATHWAY_COMPOUND; do
    download-data $URL $(create-output $SOURCE)
  done
  upload-s3 $SOURCE
elif [[ $SOURCE == "pathway-target" ]]; then
  for URL in $PATHWAY_TARGET; do
    download-data $URL $(create-output $SOURCE)
  done
  upload-s3 $SOURCE
elif [[ $SOURCE == "compound-compound" ]]; then
  for URL in $COMPOUND_COMPOUND; do
    download-data $URL $(create-output $SOURCE)
  done
  upload-s3 $SOURCE
elif [[ $SOURCE == "literature" ]]; then
  for URL in $LITERATURE; do
    download-data $URL $(create-output $SOURCE)
  done
  upload-s3 $SOURCE
elif [[ $SOURCE == "all" ]]; then
  for URL in $INACT; do
    download-data $URL $(create-output "inact")
  done
  for URL in $FULL_INT; do
    download-data $URL $(create-output "full-int")
  done
  for URL in $TARGET_DISEASE; do
    download-data $URL $(create-output "target-disease")
  done
  for URL in $TARGET_COMPOUND; do
    download-data $URL $(create-output "target-compound")
  done
  for URL in $TARGET_TARGET; do
    download-data $URL $(create-output "target-target")
  done
  for URL in $DISEASE_COMPOUND; do
    download-data $URL $(create-output "disease-compound")
  done
  for URL in $DISEASE_BIOMARKER; do
    download-data $URL $(create-output "disease-biomarker")
  done
  for URL in $PATHWAY_DISEASE; do
    download-data $URL $(create-output "pathway-disease")
  done
  for URL in $PATHWAY_COMPOUND; do
    download-data $URL $(create-output "pathway-compound")
  done
  for URL in $PATHWAY_TARGET; do
    download-data $URL $(create-output "pathway-target")
  done
  for URL in $COMPOUND_COMPOUND; do
    download-data $URL $(create-output "compound-compound")
  done
  for URL in $LITERATURE; do
    download-data $URL $(create-output "literature")
  done

fi

