#!/bin/bash

JAR_DIR=$(realpath $(dirname "${BASH_SOURCE[0]}"))/jars
INPUT_FILE="$1"
BLUE="\033[1;34m"
NC="\033[0m"
RED="\033[1;31m"
GREEN="\033[1;32m"

__help()
{
    printf "\n\tUsage: ./runner.sh <Input File (edgelist.txt-like file)>\n\n"
}

if [[ $# == 0 ]];
then 
    printf "${RED}Please pass in the Input Edgelist file!${NC}\n"
    __help
    exit 1
fi


if [[ $1 == "--run-on-hadoop" ]];
then
    __copy_jars_to_hdfs
    exit 1
fi
    
__python_start_step()
{
    printf "\n${GREEN} ---> [PYTHON STEP] <----${NC}\n"
}
__python_done_step()
{
    printf "\n${GREEN} ---> [DONE] <----${NC}\n"

}
__create_jar_for_directory()
{
    local DIR=$1
    cd "$DIR"
    mvn compile && mvn package
    cp target/*.jar "$JAR_DIR"
    if [ ! -d "$JAR_DIR"/lib ];
    then 
        cp -r target/lib "$JAR_DIR"
    fi
    cd -
}

__run_map_reduce_stage()
{
    local JAR=$1
    local INPUT=$2
    local OUTPUT=$3
    local SEP=$4

    java -jar "$JAR" "$INPUT" "$OUTPUT" "$SEP"
}

__power_iterations()
{
    local input_file=$1
    local sep=$2

    echo "$input_file" "$sep"
    __run_map_reduce_stage "$JAR_DIR"/MatrixMultiplication*.jar "$input_file" outputs/multiplication "$sep"
    __run_map_reduce_stage "$JAR_DIR"/MatrixAddition*.jar outputs/multiplication/part-r-00000 outputs/addition
    __run_map_reduce_stage "$JAR_DIR"/MatrixNormalizer*.jar outputs/addition/part-r-00000 outputs/normalized

}
__clean_jars()
{
    rm -rf jars/*
}
__clean_outputs()
{
    rm -rf outputs && mkdir outputs/ 
}

__create_initial_eigen_vector()
{
    local INPUT_FILE=$1

    BASE_NAME=$(basename "$INPUT_FILE")
    BASE_NAME="$(echo ${BASE_NAME%%".txt"})"

    COND_FILE="input/$BASE_NAME-cond.txt"
    OUTPUT_FILE="input/$BASE_NAME-modified.txt"


    printf "${BLUE}Conditioning input data... ${NC}\n"

    __python_start_step
    python preprocessor.py "--input-file" "$INPUT_FILE" "--output-file" "$OUTPUT_FILE" "--is-adjacency" "--is-initial"
    __python_done_step

    __power_iterations "$OUTPUT_FILE" "\t" > /dev/null
}

__multiply_vector_with_norm()
{
    NORMALIZED_VALUE=$(head -n 1 outputs/normalized/part-r-00000 | awk -F"\t" '{print $2}')
    INPUT_FILE="input/resulting-vector.txt"
    OUTPUT_FILE="input/vector-norm.txt"
    cp outputs/addition/part-r-* $INPUT_FILE 

    __python_start_step
    python preprocessor.py "--vector-with-norm" "--input-file" "$INPUT_FILE" "--output-file" "input/norm-vector-modified.txt" "--separator" "tab" "--normalized-value" "$NORMALIZED_VALUE"
    __python_done_step
    __clean_outputs
    __power_iterations input/norm-vector-modified.txt "\t" > /dev/null
}

__multiply_vector_with_adjacency()
{
    INPUT_FILE=$1
    # Create the eigen vector
    cp outputs/addition/part-r* input/eigen-vector.txt

    __python_start_step    
    python preprocessor.py "--vector-with-adj" "--input-file" "$INPUT_FILE" "--vector-file" "input/eigen-vector.txt" "--output-file" "input/$BASE_NAME-modified.txt"
    __python_done_step

    __clean_outputs
    __power_iterations input/$BASE_NAME-modified.txt "\t" > /dev/null

}

__save_output()
{
    cat outputs/normalized/part-r* >> final.txt
}

if [ ! -d MatrixAddition ] || [ ! -d MatrixMultiplication ] || [ ! -d MatrixNormalizer ];
then
    printf "${RED}Missing either one of the Matrix directories $NC\n"
    exit 1
fi

__clean_outputs 
rm -f final.txt rm input/immediate* input/eigen-* input/*-modified* input/resulting*

printf "${BLUE}Generating JAR files for the project... ${NC}\n"

if [ $(ls "$JAR_DIR" | wc -l) != "4" ];
then
    __create_jar_for_directory MatrixAddition 
    __create_jar_for_directory MatrixMultiplication 
    __create_jar_for_directory MatrixNormalizer
fi


__create_initial_eigen_vector "$1" > /dev/null

OLD_NORMALIZED_VALUE="0.0"



printf "${GREEN}Calculating Eigen Vector Centrality $NC\n"
while :
do
    __multiply_vector_with_norm
    __multiply_vector_with_adjacency "$1"

    FINAL_NORMALIZED_VALUE=$(head -n 1 outputs/normalized/part-r-00000 | awk -F"\t" '{print $2}')

    echo $(printf "%.2f" "$FINAL_NORMALIZED_VALUE")
    if [[ $(printf "%.2f" "$FINAL_NORMALIZED_VALUE") == $(printf "%.2f" "$OLD_NORMALIZED_VALUE") ]];
    then
        break
    fi

    OLD_NORMALIZED_VALUE="$FINAL_NORMALIZED_VALUE"
    __save_output
done

printf "Done -> Final Value ${GREEN}[$FINAL_NORMALIZED_VALUE]$NC\n"
LARGEST=$(awk -F'\t' 'BEGIN { max = -inf } { if ($2 > max) { max = $2; line = $0 } } END { print line }' input/eigen-vector.txt)
printf "Largest Node: -> ${BLUE}${LARGEST}${NC}\n"