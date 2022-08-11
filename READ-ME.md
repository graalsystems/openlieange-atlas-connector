## Create dbt entity types (dbt_application and dbt_process) :

'''python create_dbt_entity_types.py'''

## Run pulsar consumer with docker to send events to atlas :

'''docker build -t consumer .'''

'''docker run --name lineage-consumer consumer'''