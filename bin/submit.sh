#!/bin/bash

cluster_name="amiyaguchi-test"
bucket=${cluster_name}

# create the local package
pipenv run python setup.py bdist_egg
egg_name=`ls dist/ | grep egg | head -n1`
gsutil cp dist/${egg_name} gs://${bucket}/artifact/

# bring down the cluster at the end of the script
function cleanup {
    gcloud dataproc \
        clusters delete ${cluster_name}
}
trap cleanup EXIT

# create the dataproc cluster with the bootstrap script
gsutil cp bin/bootstrap.sh gs://${bucket}/artifact/
gcloud dataproc \
    clusters create ${cluster_name} \
    --bucket ${bucket} \
    --initialization-actions gs://${bucket}/artifact/bootstrap.sh

# submit the job
gcloud dataproc jobs submit pyspark \
    --cluster ${cluster_name} \
    --py-files gs://${bucket}/artifact/${egg_name} \
    bin/run.py