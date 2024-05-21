gcloud functions deploy data_acquisition \
    --runtime python312 \
    --trigger-http \
    --allow-unauthenticated \
    --set-env-vars client_id=${client_id},client_secret=${client_secret}