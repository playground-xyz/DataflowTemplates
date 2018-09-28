PROJECT_ID=creative-analytics
BUCKET_NAME=adevents.playground.xyz
PIPELINE_FOLDER=gs://${BUCKET_NAME}/dataflow

# Set the runner
RUNNER=DataflowRunner
# RUNNER=DirectRunner

# Build the template
mvn compile exec:java \
-Dexec.mainClass=com.google.cloud.teleport.templates.PubSubToBigQuery \
-Dexec.cleanupDaemonThreads=false \
-Dexec.args=" \
--project=${PROJECT_ID} \
--stagingLocation=${PIPELINE_FOLDER}/staging \
--tempLocation=${PIPELINE_FOLDER}/temp \
--templateLocation=${PIPELINE_FOLDER}/template \
--runner=${RUNNER}"

# Execute the template
JOB_NAME=pubsub-to-bq-subscriber

gcloud dataflow jobs run ${JOB_NAME} \
--gcs-location=${PIPELINE_FOLDER}/template \
--zone=us-east1-d
