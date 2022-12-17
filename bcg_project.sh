kinit -kt /home/users/dscmbtch/dscmbtch.keytab dscmbtch

#!/bin/bash


export inputData="test/bcg"

export outputData="output/bcg"

/usr/hdp/2.6.4.0-91/spark2/bin/spark-submit --class BcgCaseStudy --master yarn /grid/0/bin_case_study/BcgCaseStudy/bcgcasestudy_2.11-1.0.jar $inputData $outputData

status=$?

echo $status
