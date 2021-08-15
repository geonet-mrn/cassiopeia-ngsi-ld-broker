export class PsqlTableConfig {

    //############# BEGIN SQL table and column namen constants ###############
    readonly TBL_ATTR = "attributes"
    readonly TBL_ENT = "entities"
    readonly VIEW_LATEST_ATTR = "view_latest_attributes"
    readonly TBL_LATEST_ATTR2 = "latest_attributes"
    readonly TBL_LATEST_ATTR_MATERIALIZED = "latest_attributes_materialized"

    
    readonly COL_ATTR_EID = "eid"
    readonly COL_ATTR_NAME = "attr_name"
    readonly COL_ATTR_TYPE = "attr_type"
    readonly COL_DATASET_ID = "dataset_id"
    readonly COL_INSTANCE_ID = "instance_id"
    readonly COL_INSTANCE_JSON = "json"


    readonly COL_ATTR_CREATED_AT = "attr_created_at"
    readonly COL_ATTR_MODIFIED_AT = "attr_modified_at"
    readonly COL_ATTR_OBSERVED_AT = "attr_observed_at"

    readonly COL_ENT_INTERNAL_ID = "id"
    readonly COL_ENT_ID = "ent_id"
    readonly COL_ENT_TYPE = "ent_type"
    readonly COL_ENT_CREATED_AT = "ent_created_at"
    readonly COL_ENT_MODIFIED_AT = "ent_modified_at"    
    //############# END SQL table and column namen constants ###############
}