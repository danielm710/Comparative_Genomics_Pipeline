#!/bin/bash

#Extracts common set of protein ids between two list of protein ids

prot_id1=$1
prot_id2=$2
common_prot=$3
prot1_only=$4
prot2_only=$5

#sort each id file
sort $prot_id1 > "tmp_sorted1"
sort $prot_id2 > "tmp_sorted2"

#compare sorted protein id files
comm "tmp_sorted1" "tmp_sorted2" > "tmp_gene_comparison"

#extract proteins ids
awk -F"\t" '{print $1}' "tmp_gene_comparison" | grep -v '^$' > $prot1_only
awk -F"\t" '{print $2}' "tmp_gene_comparison" | grep -v '^$' > $prot2_only
awk -F"\t" '{print $3}' "tmp_gene_comparison" | grep -v '^$' > $common_prot

#remove all the temporary files
rm "tmp_"*
