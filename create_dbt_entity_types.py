from apache_atlas.client.base_client import AtlasClient
from apache_atlas.model.instance import AtlasEntity, AtlasEntityWithExtInfo, AtlasEntitiesWithExtInfo, AtlasRelatedObjectId
from apache_atlas.model.enums import EntityOperation
from apache_atlas.model.typedef import AtlasEntityDef
import json

client = AtlasClient('http://localhost:21000/', ('admin', 'admin'))

dbt_application=dict(name="dbt_application", 
                     superTypes=[
                         "Process"
                     ],
                     serviceType="dbt"
                    )

dbt_process=dict(name="dbt_process", 
                 superTypes=[
                     "Process"
                 ], 
                 serviceType="dbt", 
                 attributeDefs=[
                     dict(
                     name= "executionId", 
                     typeName = "long",
                     isOptional = True,
                     cardinality="SINGLE",
                     isUnique= False,
                     isIndexable = False,
                     searchWeight = 10
                     )
                 ]
                )

type_df=dict(entityDefs=[dbt_application, dbt_process])
print("Creating dbt entity types ...")
result=client.typedef.create_atlas_typedefs(type_df)
print("Dbt entity types successfully created")

print(json.dumps(result))
