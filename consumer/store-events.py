import pulsar
from pulsar import AuthenticationToken
import os
from dotenv import load_dotenv 
from apache_atlas.client.base_client import AtlasClient
from apache_atlas.model.instance     import AtlasEntity, AtlasEntityWithExtInfo, AtlasEntitiesWithExtInfo, AtlasRelatedObjectId
from apache_atlas.model.enums        import EntityOperation
import json

load_dotenv()

pulsar_url = os.getenv('PULSAR_URL')
pulsar_topic = os.getenv('PULSAR_TOPIC')
pulsar_subscription_name = os.getenv('PULSAR_SUBSCRIPTION_NAME')
auth_token = os.getenv('PULSAR_AUTH_TOKEN')
atlas_url = os.getenv('ATLAS_URL')
atlas_user = os.getenv('ATLAS_USER')
atlas_pwd = os.getenv('ATLAS_PWD')

client = pulsar.Client(pulsar_url,  tls_allow_insecure_connection=True, tls_validate_hostname=False, authentication=AuthenticationToken(auth_token))
consumer = client.subscribe(pulsar_topic, subscription_name=pulsar_subscription_name)
atlas_client = AtlasClient(atlas_url, (atlas_user, atlas_pwd))

while True:

    
    msg = consumer.receive()

    try:
        print("Received message: Data: %s \nProperties : %s \n" % (msg.data(), msg.properties()))
        #properties_list=list(msg.properties().values())
        
        event=json.loads(msg.data().decode('utf-8'))

        if (event['eventType']=='COMPLETE'):

            if ((event['producer']).endswith('spark')):

                # App for Job

                ol_atlas_spark_app= AtlasEntity({ 'typeName': 'spark_application' })

                job_attributes=event["job"]
                qualified_name=job_attributes['name'] + '@prod'
                atlas_attributes={'qualifiedName':qualified_name}
                attributes=job_attributes | atlas_attributes
                ol_atlas_spark_app.attributes=attributes

                #print(ol_atlas_spark_app)
                ol_atlas_info        = AtlasEntityWithExtInfo()
                ol_atlas_info.entity = ol_atlas_spark_app

                print('Creating ol_atlas_spark_app')
                resp = atlas_client.entity.create_entity(ol_atlas_info)

                guid_spark_app = resp.get_assigned_guid(ol_atlas_spark_app.guid)

                print('created ol_atlas_spark_app: guid=' + guid_spark_app)

                # Process for Run

                ol_atlas_run_process= AtlasEntity({ 'typeName': 'spark_process' })

                run_attributes=event["run"]
                process_name='run_'+run_attributes['runId']
                atlas_attributes={'name':process_name, 'qualifiedName':run_attributes['runId'] + '@prod'}
                attributes=run_attributes | atlas_attributes
                ol_atlas_run_process.attributes=attributes
                ol_atlas_run_process.relationshipAttributes = { 'application' : AtlasRelatedObjectId({ 'guid': guid_spark_app})  }

                #print(ol_atlas_run_process)
                ol_atlas_info        = AtlasEntityWithExtInfo()
                ol_atlas_info.entity = ol_atlas_run_process

                print('Creating ol_atlas_run_process')

                resp = atlas_client.entity.create_entity(ol_atlas_info)

                guid_spark_run_process = resp.get_assigned_guid(ol_atlas_run_process.guid)

                print('created ol_atlas_run_process: guid=' + guid_spark_run_process)

                # Processes for inputs

                event_inputs=event["inputs"]

                for i in range(len(event_inputs)):

                    base_name='input_'
                    name=f'{base_name}{i+1}' + '_' + event['run']['runId']
                    qualified_name=name+'@prod'
                    
                    ol_atlas_inputs_process= AtlasEntity({ 'typeName': 'spark_process' })
                    
                    input_attributes=event_inputs[i]
                    atlas_attributes={'name':name, 'qualifiedName':qualified_name}
                    attributes= input_attributes | atlas_attributes
                    ol_atlas_inputs_process.attributes=attributes
                    ol_atlas_inputs_process.relationshipAttributes = { 'application' : AtlasRelatedObjectId({ 'guid': guid_spark_app}), 'run' : AtlasRelatedObjectId({ 'guid': guid_spark_run_process}) }

                    #print(ol_atlas_inputs_process)
                    ol_atlas_info        = AtlasEntityWithExtInfo()
                    ol_atlas_info.entity = ol_atlas_inputs_process

                    print('Creating ' + name)

                    resp = atlas_client.entity.create_entity(ol_atlas_info)
                    guid_spark_inputs_process = resp.get_assigned_guid(ol_atlas_inputs_process.guid)

                    print('created ol_atlas_inputs_process: guid=' + guid_spark_inputs_process)

                # Processes for outputs

                event_outputs=event["outputs"]

                for i in range(len(event_outputs)):

                    base_name='output_'
                    name=f'{base_name}{i+1}' + '_' + event['run']['runId']
                    qualified_name=name+'@prod'
                    
                    ol_atlas_outputs_process= AtlasEntity({ 'typeName': 'spark_process' })
                    
                    output_attributes=event_outputs[i]
                    atlas_attributes={'name':name, 'qualifiedName':qualified_name}
                    attributes= output_attributes | atlas_attributes
                    ol_atlas_outputs_process.attributes=attributes
                    ol_atlas_outputs_process.relationshipAttributes = { 'application' : AtlasRelatedObjectId({ 'guid': guid_spark_app}), 'run' : AtlasRelatedObjectId({ 'guid': guid_spark_run_process}) }

                    #print(ol_atlas_outputs_process)
                    ol_atlas_info        = AtlasEntityWithExtInfo()
                    ol_atlas_info.entity = ol_atlas_outputs_process

                    print('Creating ' + name)

                    resp = atlas_client.entity.create_entity(ol_atlas_info)
                    guid_spark_outputs_process = resp.get_assigned_guid(ol_atlas_outputs_process.guid)

                    print('created ol_atlas_outputs_process: guid=' + guid_spark_outputs_process)

            elif ((event['producer']).endswith('dbt')):

                print("PRODUCER IS DBT")

                
                # App for Job

                ol_atlas_dbt_app= AtlasEntity({ 'typeName': 'dbt_application' })

                job_attributes=event["job"]
                qualified_name=job_attributes['name'] + '@prod'
                atlas_attributes={'qualifiedName':qualified_name}
                attributes=job_attributes | atlas_attributes
                ol_atlas_dbt_app.attributes=attributes

                #print(ol_atlas_dbt_app)
                ol_atlas_info        = AtlasEntityWithExtInfo()
                ol_atlas_info.entity = ol_atlas_dbt_app

                print('Creating ol_atlas_dbt_app')
                resp = atlas_client.entity.create_entity(ol_atlas_info)

                guid_dbt_app = resp.get_assigned_guid(ol_atlas_dbt_app.guid)

                print('created ol_atlas_dbt_app: guid=' + guid_dbt_app)

                # Process for Run

                ol_atlas_run_process= AtlasEntity({ 'typeName': 'dbt_process' })

                run_attributes=event["run"]
                process_name='run_'+run_attributes['runId']
                atlas_attributes={'name':process_name, 'qualifiedName':run_attributes['runId'] + '@prod'}
                attributes=run_attributes | atlas_attributes
                ol_atlas_run_process.attributes=attributes
                ol_atlas_run_process.relationshipAttributes = { 'application' : AtlasRelatedObjectId({ 'guid': guid_dbt_app})  }

                #print(ol_atlas_run_process)
                ol_atlas_info        = AtlasEntityWithExtInfo()
                ol_atlas_info.entity = ol_atlas_run_process

                print('Creating ol_atlas_run_process')

                resp = atlas_client.entity.create_entity(ol_atlas_info)

                guid_dbt_run_process = resp.get_assigned_guid(ol_atlas_run_process.guid)

                print('created ol_atlas_run_process: guid=' + guid_dbt_run_process)

                # Processes for inputs

                event_inputs=event["inputs"]

                for i in range(len(event_inputs)):

                    base_name='input_'
                    name=f'{base_name}{i+1}' + '_' + event['run']['runId']
                    qualified_name=name+'@prod'
                    
                    ol_atlas_inputs_process= AtlasEntity({ 'typeName': 'dbt_process' })
                    
                    input_attributes=event_inputs[i]
                    atlas_attributes={'name':name, 'qualifiedName':qualified_name}
                    attributes= input_attributes | atlas_attributes
                    ol_atlas_inputs_process.attributes=attributes
                    ol_atlas_inputs_process.relationshipAttributes = { 'application' : AtlasRelatedObjectId({ 'guid': guid_dbt_app}), 'run' : AtlasRelatedObjectId({ 'guid': guid_dbt_run_process}) }

                    #print(ol_atlas_inputs_process)
                    ol_atlas_info        = AtlasEntityWithExtInfo()
                    ol_atlas_info.entity = ol_atlas_inputs_process

                    print('Creating ' + name)

                    resp = atlas_client.entity.create_entity(ol_atlas_info)
                    guid_dbt_inputs_process = resp.get_assigned_guid(ol_atlas_inputs_process.guid)

                    print('created ol_atlas_inputs_process: guid=' + guid_dbt_inputs_process)

                # Processes for outputs

                event_outputs=event["outputs"]

                for i in range(len(event_outputs)):

                    base_name='output_'
                    name=f'{base_name}{i+1}' + '_' + event['run']['runId']
                    qualified_name=name+'@prod'
                    
                    ol_atlas_outputs_process= AtlasEntity({ 'typeName': 'dbt_process' })
                    
                    output_attributes=event_outputs[i]
                    atlas_attributes={'name':name, 'qualifiedName':qualified_name}
                    attributes= output_attributes | atlas_attributes
                    ol_atlas_outputs_process.attributes=attributes
                    ol_atlas_outputs_process.relationshipAttributes = { 'application' : AtlasRelatedObjectId({ 'guid': guid_dbt_app}), 'run' : AtlasRelatedObjectId({ 'guid': guid_dbt_run_process}) }

                    #print(ol_atlas_outputs_process)
                    ol_atlas_info        = AtlasEntityWithExtInfo()
                    ol_atlas_info.entity = ol_atlas_outputs_process

                    print('Creating ' + name)

                    resp = atlas_client.entity.create_entity(ol_atlas_info)
                    guid_dbt_outputs_process = resp.get_assigned_guid(ol_atlas_outputs_process.guid)

                    print('created ol_atlas_outputs_process: guid=' + guid_dbt_outputs_process)


            else :
                print("Producer is : ", event['producer'], '\nThis framework is not implemented')

        consumer.acknowledge(msg)

        print("Message successfully processed")



    except Exception as e:
        print(e)
        consumer.negative_acknowledge(msg)
        print("Message failed to be processed")
        


client.close()



