clusterName="recommendation-engine"

gcloud dataproc clusters create $clusterName --zone europe-west1-b --master-machine-type n1-standard-4 --master-boot-disk-size 100 --num-workers 2 --worker-machine-type n1-standard-4 --worker-boot-disk-size 50 --project amedia-analytics-eu

gcloud dataproc jobs submit pyspark --cluster $clusterName testscript.py

gcloud dataproc clusters delete $clusterName --quiet

