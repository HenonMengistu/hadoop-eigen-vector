#!/bin/env

from operator import index
import re
import sys, getopt

to_int = lambda l: list(map(lambda a: int(a), l))
def find_missing_elements(arr):
    return [ele for ele in range(max(arr)+1) if ele not in arr]

def sort_on_index(arr):
    if type(arr) is not list:
        return None
    

    if len(arr) != 0 and type(arr[0]) is not str:
        return None 

    arr.sort(key=lambda a: int(a.split(" ")[0]))
    return arr

def remove_unconnected(input_file, output_file):
    with open(input_file) as f, open(output_file, "w+") as o:
        lines = f.readlines()

        ids=[]
        for line in lines:
            line_elements = line.split(" ")
            ids.append(int(line_elements[0]))

        for missing in find_missing_elements(ids):
            lines.append("%d \n" %(missing))

        lines = sort_on_index(lines)
        for line in lines:
            o.writelines(line)

def generate_pattern(row, index_i, index_j, value):
    return "%s\t%s\t%s\t%s\n" % (row, index_i, index_j, value)

def get_vector_value(vectors, id):
    data = list(filter(lambda a: int(a.split("\t")[0]) == id, vectors))
    if len(data):
        return data[0].split("\t")[1].replace("\n", "")
    else: return 0

def multiply_vector_with_normalized(vector_input_file,output_file, normalized_value, separator):
    with open(vector_input_file) as v, open(output_file, "w+") as o:
        lines = v.readlines()

        normalized_value = 1/float(normalized_value.strip())
        for line in lines:
            indexes = line.split(separator)

            row = indexes[0]

            indexes = indexes[1:]
            output_lines = []
            for index in indexes:
                output_lines.append(generate_pattern(row, row, "0", index.strip()))
                output_lines.append(generate_pattern(row, row, "0", normalized_value))
            
            o.writelines(output_lines)

def multiply_vector_with_adjacency(input_file, vector_file, output_file, separator):
    with open(input_file) as i, open(vector_file) as v, open(output_file, "w+") as o:
        lines = i.readlines()
        vectors = v.readlines()

        for line in lines:
            indexes = line.split(separator)
            row = indexes[0]
            indexes = indexes[1:]
            output_lines = []
            for index in indexes:
                vector_value = get_vector_value(vectors, int(index.strip()))
                output_lines.append(generate_pattern(row, row, index.strip(), "1"))
                output_lines.append(generate_pattern(row, row, index.strip(), vector_value))
            
            o.writelines(output_lines)

def create_pattern(input_file, output_file, is_adjacency, is_initial, separator):
    with open(input_file) as f, open(output_file, "w+") as o:
        lines = f.readlines()

        for line in lines:
            line_elements = line.split(separator)
            
            row_id = line_elements[0]
            index_i = row_id
            value = float(1)/(len(lines)-1) if is_initial else 0
            output_lines = []
            matrix_value = "1" if is_adjacency else 0
            
            for index in line_elements[1:]:
                matrix_value = 0 if int(index) == -1 else matrix_value
                output_lines.append(generate_pattern(row_id, index_i, index.strip(), matrix_value))
                output_lines.append(generate_pattern(row_id, index_i, index.strip(), value))
            
            o.writelines(output_lines)

if __name__ == "__main__":
    input_file=''
    output_file=''
    is_adjacency=False
    is_initial=False
    condition_data=False
    separator=' '
    vec_separator=' '
    with_vector=False
    vector_file=''
    is_normalized=False
    normalized_value=''
    multiply_eigen=False
    vector_with_norm=False
    vector_with_adj=False
    options, args = getopt.getopt(sys.argv[1:], 'i:o' ,[
                                                        'input-file=', 
                                                        'vector-with-norm',
                                                        'vector-with-adj',
                                                        'output-file=',
                                                        'condition-data' ,
                                                        'is-adjacency', 
                                                        "is-initial",
                                                        'separator=',
                                                        'with-vector',
                                                        'vector-file=',
                                                        'is-normalized',
                                                        'multiply-eigen',
                                                        'normalized-value='])

    for opt, arg in options:
        if opt in ('-i', '--input-file'):
            input_file=arg
        if opt in ('-o', '--output-file'):
            output_file=arg
        if opt in ('--is-adjacency'):
            is_adjacency = True
        if opt in ('--is-initial'):
            is_initial = True 
        if opt in ('--condition-data'):
            condition_data = True
        if opt in ('--separator'):
            separator="\t" if arg == "tab" else " "
        if opt in ('--with-vector'):
            with_vector=True
        if opt in ('--vector-file'):
            vector_file = arg
        if opt in ('--is-normalized'):
            is_normalized = True
        if opt in ('--normalized-value'):
            normalized_value = arg
        if opt in ('--multiply-eigen'):
            multiply_eigen = True
        if opt in ('--vector-with-norm'):
            vector_with_norm =True
        if opt in ('--vector-with-adj'):
            vector_with_adj =True

    if vector_with_norm:
        multiply_vector_with_normalized(input_file, output_file, normalized_value, separator)
    elif vector_with_adj:
        multiply_vector_with_adjacency(input_file, vector_file, output_file, separator)
    else:
        create_pattern(input_file, output_file, is_adjacency, is_initial, separator)