// TODO: 3 Remove entity table row if no attribute table entries are left

import * as pg from 'pg'
import { errorTypes } from '../errorTypes'
import { NgsiLdQueryParser } from './NgsiLdQueryParser'
import { EntityTypeList } from '../dataTypes/EntityTypeList'
import { EntityType } from '../dataTypes/EntityType'
import { EntityTypeInfo } from '../dataTypes/EntityTypeInfo'
import { Attribute } from '../dataTypes/Attribute'
import { AttributeList } from '../dataTypes/AttributeList'
import { Query } from '../dataTypes/Query'
import { checkReifiedAttribute, isDateTimeUtcString, checkGeoQuery, checkQuery, isReifiedAttribute } from '../validate'
import { InsertQueryBuilder } from './InsertQueryBuilder'
import { PsqlTableConfig } from './PsqlTableConfig'
import { makeGeoQueryCondition } from './makeGeoQueryCondition'
import { makeTemporalQueryCondition } from './makeTemporalQueryCondition'
import { compactObject, expandObject, getNormalizedContext } from '../jsonld'
import { JsonLdContextNormalized } from 'jsonld-context-parser'
import { UpdateResult } from '../dataTypes/UpdateResult'
import { NotUpdatedDetails } from '../dataTypes/NotUpdatedDetails'


const ignoreAttributes = ["@id", "@type", "@context"]

const uri_modifiedAt = "https://uri.etsi.org/ngsi-ld/modifiedAt"
const uri_datasetId = "https://uri.etsi.org/ngsi-ld/datasetId"
const uri_createdAt = "https://uri.etsi.org/ngsi-ld/createdAt"
const uri_instanceId = "https://uri.etsi.org/ngsi-ld/instanceId"

export class PsqlBackend {

    readonly tableCfg = new PsqlTableConfig()

    // ATTENTION: Changing the order of items in attributeTypes corrupts the database!
    private readonly attributeTypes = ["https://uri.etsi.org/ngsi-ld/Property", "https://uri.etsi.org/ngsi-ld/GeoProperty", "https://uri.etsi.org/ngsi-ld/Relationship"]


    temporalAppend = false



    // The PostgreSQL connection object:
    private readonly pool!: pg.Pool

    private readonly ngsiQueryParser = new NgsiLdQueryParser(this.tableCfg)


    constructor(config: any, private ngsiLdCoreContext: JsonLdContextNormalized) {

        this.pool = new pg.Pool(config.psql)
    }


    async appendEntityAttributes(entityInternalId: any, fragment_expanded: any, overwrite: boolean, updateOnly: boolean, temporal: boolean) {

        const result = new UpdateResult()

        // NOTE:
        // There is a theoretically important difference between creating one single transaction query for all
        // instance updates and running this one query after the for loop over the attributes is completed,
        // versus creating a separate transaction query for each attribute and running each query at the end
        // of each loop iteration: 

        // The difference is that if we run the update query for each attribute separately,
        // the fetching of existing attribute instances in each loop iteration might return different results,
        // because matching attribute instances might have been created in previous loop iterations.
        // If we run only one big update query after the loops is complete, nothing is writting to the database
        // during the loop, and attributes which are created by the update are not known to the system while
        // the loop is still running.

        // So, in theory, doing smaller sequential updates can help to detect inconsistencies like multiple identical
        // datasetIds. In practice, this should not be necessary since input data should be validated before
        // it is written to the database. However, for now, we stick with the sequential step-by-step update
        // since it adds an additional layer of consistency checking.

        //####################### BEGIN Iterate over attributes #############################
        for (const attributeId_expanded in fragment_expanded) {

            let sql_transaction = "BEGIN;"


            // Do not process @id, @type and @context:
            if (ignoreAttributes.includes(attributeId_expanded)) {
                continue
            }


            let attribute_expanded = (fragment_expanded as any)[attributeId_expanded]

            if (!(attribute_expanded instanceof Array)) {
                attribute_expanded = [attribute_expanded]
            }

            //#################### BEGIN Validate attribute ####################
            const reifiedAttributeCheck = checkReifiedAttribute(attribute_expanded, attributeId_expanded, undefined, false)

            if (reifiedAttributeCheck.length > 0) {

                let errorMsg = ""

                for (const msg of reifiedAttributeCheck) {
                    errorMsg += msg + "\n"
                }

                result.notUpdated.push(new NotUpdatedDetails(attributeId_expanded, "Not a valid reified attribute: \n" + errorMsg))

                continue
            }
            //#################### END Validate attribute ####################

            let updated = false

            //#################### BEGIN Iterate over attribute instances #####################
            for (const instance_expanded of attribute_expanded) {

                const datasetId = instance_expanded['https://uri.etsi.org/ngsi-ld/datasetId']
                const existingInstances = await this.getAttributeInstances(entityInternalId, attributeId_expanded, datasetId)


                if (updateOnly) {
                  
                    let lastCreatedInstance: any = null

                    for (const exInst of existingInstances) {
                        if (lastCreatedInstance == null || exInst.instance_id > lastCreatedInstance.instance_id) {
                            lastCreatedInstance = exInst
                        }
                    }

                    if (lastCreatedInstance != null) {
                        sql_transaction += this.makeUpdateAttributeInstanceQuery(lastCreatedInstance.instance_id, instance_expanded, true)
                        updated = true
                    }
                }

                else {
                    // In temporal mode, attribute instances are always appended, regardless of datasetId:                            
                    if (existingInstances.length == 0 || temporal) {

                        sql_transaction += this.makeCreateAttributeInstanceQuery(entityInternalId, attributeId_expanded, instance_expanded)

                        updated = true
                    }
                    else {

                        if (overwrite) {
                            let lastCreatedInstance: any = null


                            for (const exInst of existingInstances) {
                                if (lastCreatedInstance == null || exInst.instance_id > lastCreatedInstance.instance_id) {
                                    lastCreatedInstance = exInst
                                }
                            }

                            if (lastCreatedInstance != null) {
                                sql_transaction += this.makeUpdateAttributeInstanceQuery(lastCreatedInstance.instance_id, instance_expanded, true)
                                updated = true
                            }
                        }
                    }
                }
            }
            //################## END Iterate over attribute instances #######################

            if (updated) {
                result.updated.push(attributeId_expanded)

                sql_transaction += this.makeUpdateEntityModifiedAtQuery(entityInternalId)
                sql_transaction += "COMMIT;"
                await this.runSqlQuery(sql_transaction)
            }
            else {
                result.notUpdated.push(new NotUpdatedDetails(attributeId_expanded, "Attribute instance(s) already exist and no overwrite was ordered."))
            }
        }
        //####################### END Iterate over attributes #############################



        return new Promise<UpdateResult>((resolve, reject) => {
            resolve(result)
        })
    }


    private async getAttributeInstances(entityInternalId: number, attributeName: string, datasetId: string | null | undefined): Promise<any> {

        // TODO 3: Order and limit to improve performance?

        if (datasetId === undefined) {
            datasetId = null
        }

        let sql = `SELECT * FROM ${this.tableCfg.TBL_ATTR} WHERE eid = ${entityInternalId} `

        sql += ` AND ${this.tableCfg.COL_ATTR_NAME} = '${attributeName}'`
        sql += this.makeSqlCondition_datasetId(datasetId)

        const sqlResult = await this.runSqlQuery(sql)

        return new Promise((resolve, reject) => {
            resolve(sqlResult.rows)
        })
    }



    async countEntitiesByType(type: string): Promise<number> {

        let sql = `SELECT COUNT(*) FROM ${this.tableCfg.TBL_ENT} WHERE ${this.tableCfg.COL_ENT_TYPE} = '${type}'`


        let sqlResult = await this.runSqlQuery(sql)

        return new Promise((resolve, reject) => {
            resolve(sqlResult.rows[0].count)
        })
    }


    async createEntity(entity_expanded: any, temporal: boolean): Promise<number> {

        //############## BEGIN Build INSERT query for entities table ###########
        const now = new Date()

        const queryBuilder = new InsertQueryBuilder()

        queryBuilder.add(this.tableCfg.COL_ENT_ID, entity_expanded['@id'])
        queryBuilder.add(this.tableCfg.COL_ENT_TYPE, entity_expanded['@type'])
        queryBuilder.add(this.tableCfg.COL_ENT_CREATED_AT, now.toISOString())
        queryBuilder.add(this.tableCfg.COL_ENT_MODIFIED_AT, now.toISOString())
        queryBuilder.add(this.tableCfg.COL_ENT_TEMPORAL, temporal.toString())
        //############## END Build INSERT query for entities table ###########


        //################# BEGIN Create entities table entry #################
        const queryResult = await this.runSqlQuery(queryBuilder.getStringForTable(this.tableCfg.TBL_ENT, "id")).catch((error: any) => { })

        if (queryResult == undefined) {

            return new Promise<number>((resolve, reject) => {
                resolve(-1)
            })
        }

        const insertId = queryResult.rows[0].id
        //################# END Create entities table entry #################

        await this.appendEntityAttributes(insertId, entity_expanded, false, false, false)

        return new Promise<number>((resolve, reject) => {
            resolve(1)
        })
    }


    async createOrUpdateTemporalEntity(entity_expanded: any) {

        // NOTE: We should probably not merge this with createEntity because the behaviours of both methods are different:
        // The temporal version supports updates of an existing entity with the same request while the non-temporal version
        // doesn't.

        const entityMetadata = await this.getEntityMetadata(entity_expanded['@id'])

        // If the entity doesn't exist yet, create it:
        if (entityMetadata == undefined) {

            await this.createEntity(entity_expanded, true)

            return new Promise<number>((resolve, reject) => { resolve(201) })
        }
        else {

            // If the entity already exists:

            // 5.6.4.11: 

            // "If the NGSI-LD endpoint already knows about this Temporal Representation of an Entity, 
            // because there is an existing Temporal Representation of an Entity whose id (URI) is the same, 
            // then all the Attribute instances included by the Temporal Representation shall be added to 
            // the existing Entity as mandated by clause 5.6.12.":

            // NOTE: If "temporal" (last parameter) is true, then "overwrite" (second-last parameter)
            // has no effect. We set it to false, but setting it to true wouldn't change the result.
            // In temporal mode, attribute instances are always appended and never overwritten.
            await this.appendEntityAttributes(entityMetadata.id, entity_expanded, false, false, true)

            return new Promise<number>((resolve, reject) => {
                resolve(204)
            })
        }
    }


    async deleteAllEntities() {
        await this.runSqlQuery(`DELETE FROM ${this.tableCfg.TBL_ATTR}`)
        await this.runSqlQuery(`DELETE FROM ${this.tableCfg.TBL_ENT}`)
    }


    // Spec 5.6.5
    async deleteAttribute(
        entityInternalId: number,
        attributeId: string,
        instanceId: string | undefined,
        datasetId: string | null | undefined): Promise<number> {

        // NOTE: This method returns a Promise with the number of deleted rows

        let sql = `DELETE FROM ${this.tableCfg.TBL_ATTR} WHERE eid = ${entityInternalId} `

        // Match attribute ID:
        sql += ` AND ${this.tableCfg.COL_ATTR_NAME} = '${attributeId}' `


        // Match instance ID if provided:
        if (instanceId != undefined) {
            // NOTE: We assume that the attribute instances is passed in the form "urn:ngsi-ld:InstanceId:instance_<number>"
            const instanceId_number = parseFloat(instanceId.split("_")[1])

            // TODO: 1 Make function to get instance number from instance ID string
            sql += ` AND ${this.tableCfg.COL_INSTANCE_ID} = '${instanceId_number}'`
        }

        // Possible cases:
        // datasetId_expanded == null -> delete default instance(s) (i.e. instances without datasetId)
        // datasetId_expanded == undefined -> delete all instances
        // datasetId_expanded == something else -> delete instance(s) with the specified dataset id


        // Match dataset ID if provided:
        // ATTENTION: It is REQUIRED to compare with a "!==" here! We must NOT use a "!="!
        if (datasetId !== undefined) {
            sql += this.makeSqlCondition_datasetId(datasetId)
        }

        const queryResult = await this.runSqlQuery(sql)

        // Return number of deleted rows as promise:
        return new Promise((resolve, reject) => {
            resolve(queryResult.rowCount)
        })
    }


    async deleteEntity(entityId: string): Promise<boolean> {

        // TODO: 2 Catch SQL exceptions here instead of returning them

        // SQL query to delete the entity's row from the entities table:
        // Note that this delete query returns the internal ID of the deleted entity.
        // The internal ID is then used to find and delete the entity's rows in the attributes table.
        const sql_delete_entity_metadata = `DELETE FROM ${this.tableCfg.TBL_ENT} WHERE ${this.tableCfg.COL_ENT_ID} = '${entityId}' RETURNING id`

        const queryResult1 = await this.runSqlQuery(sql_delete_entity_metadata)

        if (queryResult1.rows.length == 0) {
            // Return number of deleted rows as promise:
            return new Promise((resolve, reject) => {
                reject(false)
            })
        }

        // NOTE: If everything is as expected, there should always be at most 1 row returned. 
        // Nevertheless, we use a for loop here, just to make sure.


        //############ BEGIN Build and run transaction query to delete all attribute rows ###########
        let sql_delete_attributes = "BEGIN;"

        // Add queries to delete all of the entity's attributes to the transaction:
        for (const row of queryResult1.rows) {
            sql_delete_attributes += `DELETE FROM ${this.tableCfg.TBL_ATTR} WHERE eid = ${row[this.tableCfg.COL_ENT_INTERNAL_ID]};`
        }

        sql_delete_attributes += "COMMIT;"

        // Run transaction query:        
        const queryResult2 = await this.runSqlQuery(sql_delete_attributes)
        //############ END Build and run transaction query to delete all attribute rows ###########


        // Return number of deleted rows as promise:
        return new Promise((resolve, reject) => {
            resolve(true)
        })
    }


    async getAvailableAttributes(): Promise<AttributeList> {

        // TODO: 3 Ask: Should default attributes like "createdAt" be included here?

        const result = new AttributeList()

        const sql = `SELECT DISTINCT ${this.tableCfg.COL_ATTR_NAME} FROM ${this.tableCfg.TBL_ATTR}`

        const sqlResult = await this.runSqlQuery(sql)

        for (const row of sqlResult.rows) {
            result.attributeList.push(row[this.tableCfg.COL_ATTR_NAME])
        }

        return new Promise<AttributeList>((resolve, reject) => {
            resolve(result)
        })
    }


    async getDetailsOfAvailableAttributes(): Promise<Array<Attribute>> {

        const attributes = await this.getAvailableAttributes()

        let result = Array<Attribute>()

        for (const attrName of attributes.attributeList) {

            let attribute = await this.getAttributeInfo(attrName)

            result.push(attribute)
        }

        return new Promise<Array<Attribute>>((resolve, reject) => {
            resolve(result)
        })
    }


    async getDetailsOfEntityTypes(): Promise<Array<EntityType>> {

        const sql = `SELECT DISTINCT ${this.tableCfg.COL_ENT_TYPE}, ${this.tableCfg.COL_ATTR_NAME} FROM ${this.tableCfg.TBL_ENT} AS t1, ${this.tableCfg.TBL_ATTR} AS t2 WHERE t1.${this.tableCfg.COL_ENT_INTERNAL_ID} = t2.eid`

        const queryResult = await this.runSqlQuery(sql)

        const types = new Map<String, EntityType>()


        for (const row of queryResult.rows) {

            const typeName = row[this.tableCfg.COL_ENT_TYPE]
            const attrName = row[this.tableCfg.COL_ATTR_NAME]

            if (types.get(typeName) == undefined) {
                types.set(typeName, new EntityType(typeName))
            }

            const type = types.get(typeName)!

            if (!type.attributeNames.includes(attrName)) {
                type.attributeNames.push(attrName)
            }
        }


        let result = new Array<EntityType>()

        for (let type of types.values()) {
            result.push(type)
        }

        return new Promise((resolve, reject) => {
            resolve(result)
        })
    }


    async getAttributeInfo(attributeId_expanded: string): Promise<Attribute> {


        const sql = `SELECT ${this.tableCfg.COL_ENT_TYPE}, ${this.tableCfg.COL_ATTR_TYPE} FROM ${this.tableCfg.TBL_ENT} as t1, ${this.tableCfg.TBL_ATTR} as t2 WHERE t1.${this.tableCfg.COL_ENT_INTERNAL_ID} = t2.eid AND ${this.tableCfg.COL_ATTR_NAME} = '${attributeId_expanded}'`

        let sqlResult = await this.runSqlQuery(sql)

        let result = new Attribute(attributeId_expanded, attributeId_expanded, sqlResult.rows.length)


        for (const row of sqlResult.rows) {

            const attrInstanceType = row[this.tableCfg.COL_ATTR_TYPE]
            const entityType = row[this.tableCfg.COL_ENT_TYPE]

            if (!result.attributeTypes.includes(this.attributeTypes[attrInstanceType])) {
                result.attributeTypes.push(this.attributeTypes[attrInstanceType])
            }

            if (!result.typeNames.includes(entityType)) {
                result.typeNames.push(entityType)
            }
        }


        return new Promise((resolve, reject) => {
            resolve(result)
        })
    }


    async getEntityTypeInformation(type: string): Promise<EntityTypeInfo> {

        const entityCount = await this.countEntitiesByType(type)

        const sql = `SELECT DISTINCT ${this.tableCfg.COL_ATTR_NAME} FROM ${this.tableCfg.TBL_ENT} as t1, ${this.tableCfg.TBL_ATTR} as t2 WHERE t1.${this.tableCfg.COL_ENT_INTERNAL_ID} = t2.eid AND ${this.tableCfg.COL_ENT_TYPE} = '${type}'`

        const sqlResult = await this.runSqlQuery(sql)

        const result = new EntityTypeInfo(type, entityCount)

        for (const row of sqlResult.rows) {

            const attribute = await this.getAttributeInfo(row[this.tableCfg.COL_ATTR_NAME])

            result.attributeDetails.push(attribute)
        }

        return new Promise((resolve, reject) => {
            resolve(result)
        })
    }


    async getEntityTypes(): Promise<EntityTypeList> {

        const queryResult = await this.runSqlQuery(`SELECT DISTINCT ${this.tableCfg.COL_ENT_TYPE} FROM ${this.tableCfg.TBL_ENT}`)

        let result = new EntityTypeList()

        for (let row of queryResult.rows) {
            result.typeList.push(row[this.tableCfg.COL_ENT_TYPE])
        }

        return new Promise((resolve, reject) => {
            resolve(result)
        })
    }


    async getEntitiesBySqlWhere(sql_where: string, includeSysAttrs: boolean, orderBySql: string | undefined,
        lastN: number | undefined, attrNames_expanded: Array<string> | undefined, temporal: boolean): Promise<Array<any>> {


        //############# BEGIN Build "ORDER BY" query part ############
        let orderBy = ""

        if (orderBySql != undefined) {
            orderBy = " ORDER BY " + orderBySql
        }
        //############# END Build "ORDER BY" query part ############


        // ATTENTION: The 'sql_where' string must begin with and "AND"!

        const fields = Array<string>()

        fields.push(this.tableCfg.COL_ENT_TYPE)
        fields.push(this.tableCfg.COL_ENT_ID)
        fields.push(this.tableCfg.COL_ATTR_NAME)
        fields.push(this.tableCfg.COL_INSTANCE_ID)
        fields.push(this.tableCfg.COL_INSTANCE_JSON)
        fields.push(`${this.tableCfg.COL_ENT_CREATED_AT} at time zone 'utc' as ent_created_at`)
        fields.push(`${this.tableCfg.COL_ENT_MODIFIED_AT} at time zone 'utc' as ent_modified_at`)
        fields.push(`${this.tableCfg.COL_ATTR_CREATED_AT} at time zone 'utc' as attr_created_at`)
        fields.push(`${this.tableCfg.COL_ATTR_MODIFIED_AT} at time zone 'utc' as attr_modified_at`)
        fields.push(`${this.tableCfg.COL_ATTR_OBSERVED_AT} at time zone 'utc' as attr_observed_at`)

        let sql = `SELECT ${fields.join(',')} FROM ${this.tableCfg.TBL_ENT} AS t1, ${this.tableCfg.TBL_ATTR} AS t2 WHERE t1.${this.tableCfg.COL_ENT_INTERNAL_ID} = t2.eid ${sql_where} ${orderBy}`

        // If lastN is defined, wrap limiting query around the original query:
        // See https://stackoverflow.com/questions/1124603/grouped-limit-in-postgresql-show-the-first-n-rows-for-each-group

        if (typeof (lastN) == "number" && lastN > 0) {
            sql = `SELECT * FROM (SELECT ROW_NUMBER() OVER (PARTITION BY ent_id, attr_name ${orderBy}) AS r, t.* FROM (${sql}) t) x WHERE x.r <= ${lastN};`
        }

        const queryResult = await this.runSqlQuery(sql)

        const entitiesByNgsiId: any = {}

        //#################### BEGIN Iterate over returned attribute instance rows ####################
        for (const row of queryResult.rows) {

            const ent_id = row[this.tableCfg.COL_ENT_ID]
            const attr_name = row[this.tableCfg.COL_ATTR_NAME]

            //############## BEGIN Get or create Entity in memory #############
            let entity = entitiesByNgsiId[ent_id]

            if (!entity) {

                entity = {
                    "@id": ent_id,
                    "@type": row[this.tableCfg.COL_ENT_TYPE]
                }

                if (includeSysAttrs) {
                    entity[uri_createdAt] = row[this.tableCfg.COL_ENT_CREATED_AT]
                    entity[uri_modifiedAt] = row[this.tableCfg.COL_ENT_MODIFIED_AT]
                }

                entitiesByNgsiId[ent_id] = entity
            }
            //############## END Get or create Entity in memory #############


            //############## BEGIN Get or create Attribute instance in memory #############
            let attribute = entity[attr_name]

            if (!attribute) {
                attribute = []
                entity[attr_name] = attribute
            }
            //############## END Get or create Attribute instance in memory #############

            // ATTENTION: We must write the entire dataset object to the JSON field. 
            // Only putting the value field there is not sufficient, since there might 
            // be other fields as siblings of the value field which must be stored too (e.g. "source", see spec ?)

            const instance = row[this.tableCfg.COL_INSTANCE_JSON]

            // TODO: 1 Add method to create instance ID string from number

            // ATTENTION: The returned instance ID value string MUST contain an "_" (underscore) because we
            // use it in PsqlBackend::deleteAttribute() as a string separator character to extract the
            // actual instance id number from a passed instance id string.

            instance[uri_instanceId] = "urn:ngsi-ld:InstanceId:instance_" + row[this.tableCfg.COL_INSTANCE_ID]

            // ATTENTION: We always add the modified timestamp first, regardless of whether includeSysAttrs is true,
            // because we need it to find the most recently modified attribute instance if this is not a
            // temporal API query:


            //####### BEGIN Restore JSON fields that have their own database column ##########
            instance[uri_createdAt] = row["attr_created_at"]
            instance[uri_modifiedAt] = row["attr_modified_at"]

            if (row["attr_observed_at"] != null) {
                instance["https://uri.etsi.org/ngsi-ld/observedAt"] = row["attr_observed_at"]
            }
            //####### END Restore JSON fields that have their own database column ##########



            // If the temporal representation of an entity is requested, all attribute instances are included:
            if (temporal) {
                attribute.push(instance)
            }

            // If the "normal" representation of an entity is requested and there are multiple attribute 
            // instances with the same datasetId, only the most recently created attribute instance of each 
            // particular datasetId (identified by highest instanceId) is returned:
            else {
                let replaceIndex = null

                for (let ii = 0; ii < attribute.length; ii++) {
                    let existingInstance = attribute[ii]

                    if (existingInstance[uri_datasetId] == instance[uri_datasetId] && existingInstance[uri_instanceId] <= instance[uri_instanceId]) {
                        replaceIndex = ii
                        //console.log("replacing " + existingInstance[uri_instanceId] + " " + instance[uri_instanceId])
                    }
                }

                if (replaceIndex != null) {

                    attribute[replaceIndex] = instance
                }
                else {
                    attribute.push(instance)
                }

            }
        }
        //#################### END Iterate over returned attribute instance rows ####################


        //################ BEGIN Create result array of entities ###################
        let result = Array<any>()

        for (const entityId in entitiesByNgsiId) {
            result.push(entitiesByNgsiId[entityId])
        }
        //################ END Create result array of entities ###################


        //############# BEGIN Add empty arrays for requested attributes with no matching instances #############

        // "For the avoidance of doubt, if for a requested Attribute no instance fulfils the temporal query, 
        // then an empty Array of instances shall be provided as the representation for such Attribute.":

        if (attrNames_expanded instanceof Array && attrNames_expanded.length > 0) {
            for (const entity of result) {

                for (const attributeName of attrNames_expanded) {
                    if (entity[attributeName] == undefined) {
                        entity[attributeName] = []
                    }
                }
            }
        }
        //############# END Add empty arrays for requested attributes with no matching instances #############

        return new Promise((resolve, reject) => {
            resolve(result)
        })
    }


    async getEntityMetadata(entityId: string): Promise<any> {

        //const sql = `SELECT * FROM ${this.tableCfg.TBL_ENT} WHERE ${this.tableCfg.COL_ENT_ID} = '${entityId}' AND ${this.tableCfg.COL_ENT_TEMPORAL} = ${temporal.toString()}`

        const sql = `SELECT * FROM ${this.tableCfg.TBL_ENT} WHERE ${this.tableCfg.COL_ENT_ID} = '${entityId}'`

        const sqlResult = await this.runSqlQuery(sql)

        // No entitiy with passed ID was found:
        if (sqlResult.rows.length == 0) {
            return new Promise((resolve, reject) => {
                resolve(undefined)
            })
        }

        // 1 Entity with passed ID was found:
        else if (sqlResult.rows.length == 1) {

            const row = sqlResult.rows[0]
            const metadata = { id: row[this.tableCfg.COL_ENT_INTERNAL_ID], type: row[this.tableCfg.COL_ENT_TYPE] }

            return new Promise((resolve, reject) => {
                resolve(metadata)
            })
        }

        // More than 1 Entity with passed ID was found. This should never happen:
        else if (sqlResult.rows.length > 1) {
            throw errorTypes.InternalError.withDetail(`getEntityMetadata(): More than one Entity with ID '${entityId}' found. This is an invalid database state and should never happen.`)
        }
    }


    getTemporalTableColumn(timeproperty: string): string | undefined {

        //################## BEGIN Figure out temporal table column to query ####################
        const temporalFields: any = {
            'observedAt': this.tableCfg.COL_ATTR_OBSERVED_AT,
            'modifiedAt': this.tableCfg.COL_ATTR_MODIFIED_AT,
            'createdAt': this.tableCfg.COL_ATTR_CREATED_AT
        }

        if (!(timeproperty in temporalFields)) {
            return undefined
        }

        return temporalFields[timeproperty]
    }



    makeCreateAttributeInstanceQuery(entityInternalId: number, attributeId: string, instance_expanded: any): string {

        // NOTE: This is implemented as a method that returns an SQL string instead of
        // a method which directly creates an attribute, because in some places, we want
        // to combine multiple attribute creation queries in one transaction.

        const queryBuilder = new InsertQueryBuilder()

        //#################### BEGIN Add entity id to insert query #################### 

        // NOTE: By passing -1 as the value for entityInternalId, this method will use the id that was
        // last used in an insert query on the 'entities' table. We use this when we create new entities
        // and add their attributes in one transactional query 
        // (i.e. INSERT on 'entities' table + INSERT(s) on 'attributes' table in one transaction).

        if (entityInternalId == -1) {
            queryBuilder.add("eid", "currval('entities_id_seq')", true)
        }
        else {
            queryBuilder.add("eid", entityInternalId)
        }
        //###################### END Add entity id to insert query ################## 

        const attributeTypeIndex = this.attributeTypes.indexOf(instance_expanded['@type'])

        if (attributeTypeIndex < 0) {
            throw errorTypes.InternalError.withDetail("Invalid attribute type: " + instance_expanded['@type'])
        }

        queryBuilder.add(this.tableCfg.COL_ATTR_NAME, attributeId)
        queryBuilder.add(this.tableCfg.COL_ATTR_TYPE, attributeTypeIndex)
        queryBuilder.add(this.tableCfg.COL_DATASET_ID, instance_expanded['https://uri.etsi.org/ngsi-ld/datasetId'])
        queryBuilder.add(this.tableCfg.COL_INSTANCE_JSON, JSON.stringify(instance_expanded))

        // Write 'geom' column:
        if (instance_expanded['@type'] == "https://uri.etsi.org/ngsi-ld/GeoProperty") {

            const geojson_expanded = instance_expanded['https://uri.etsi.org/ngsi-ld/hasValue']

            // TODO: 1 Is it correct to simply use the NGSI-LD core context here?
            const geojson_compacted = compactObject(geojson_expanded, this.ngsiLdCoreContext)

            const geojson_string = JSON.stringify(geojson_compacted)

            queryBuilder.add("geom", `ST_SetSRID(ST_GeomFromGeoJSON('${geojson_string}'), 4326)`, true)
        }

        // Write 'observed_at' column:
        if (isDateTimeUtcString(instance_expanded["https://uri.etsi.org/ngsi-ld/observedAt"])) {
            queryBuilder.add(this.tableCfg.COL_ATTR_OBSERVED_AT, instance_expanded["https://uri.etsi.org/ngsi-ld/observedAt"])
        }

        // Write "created at" and "modified at" columns:
        const now = new Date()
        queryBuilder.add(this.tableCfg.COL_ATTR_CREATED_AT, now.toISOString())
        queryBuilder.add(this.tableCfg.COL_ATTR_MODIFIED_AT, now.toISOString())


        let sql = queryBuilder.getStringForTable(this.tableCfg.TBL_ATTR)

        // Add SQL query to update entity:

        // NOTE: If multiple attribute create queries are performed in a request, the update of the
        // entity's modified_at field will be performed as many times redundantly. 
        // This is probably not a problem, but it should be mentioned.

        sql += `UPDATE ${this.tableCfg.TBL_ENT} SET ${this.tableCfg.COL_ENT_MODIFIED_AT} = '${now.toISOString()}' WHERE ${this.tableCfg.COL_ENT_INTERNAL_ID} = ${entityInternalId};`

        return sql
    }


    makeSqlCondition_datasetId(datasetId: string | null | undefined): string {

        if (datasetId === null) {
            return ` AND ${this.tableCfg.COL_DATASET_ID} is null`
        }
        else if (datasetId === undefined) {
            return ""
        }
        else {
            return ` AND ${this.tableCfg.COL_DATASET_ID} = '${datasetId}'`
        }
    }


    makeUpdateAttributeInstanceQuery(
        instanceId: number,
        instance: any,
        allowAttributeTypeChange: boolean): string {

        // NOTE: This method returns a Promise that contains the number of updated attribute instances.




        //################# BEGIN Build SQL query to update attribute instance #####################
        let sql = `UPDATE ${this.tableCfg.TBL_ATTR} SET ${this.tableCfg.COL_INSTANCE_JSON} = '${JSON.stringify(instance)}'`

        // Write 'geom' column:
        if (instance['@type'] == "https://uri.etsi.org/ngsi-ld/GeoProperty") {

            const geojson_expanded = instance['https://uri.etsi.org/ngsi-ld/hasValue']

            const geojson_compacted = compactObject(geojson_expanded, this.ngsiLdCoreContext)

            const geojson_string = JSON.stringify(geojson_compacted)

            sql += `, geom = ST_SetSRID(ST_GeomFromGeoJSON('${geojson_string}'), 4326)`
        }

        // Write 'observed_at' column:
        if (isDateTimeUtcString(instance["https://uri.etsi.org/ngsi-ld/observedAt"])) {
            sql += `, ${this.tableCfg.COL_ATTR_OBSERVED_AT} = '${instance["https://uri.etsi.org/ngsi-ld/observedAt"]}'`
        }


        // Write 'modified_at' column:    
        const now = new Date()
        sql += `, ${this.tableCfg.COL_ATTR_MODIFIED_AT} = '${now.toISOString()}'`



        // Add WHERE conditions:        
        sql += ` WHERE ${this.tableCfg.COL_INSTANCE_ID} = ${instanceId}`


        if (!allowAttributeTypeChange) {
            // ATTENTION: COL_ATTR_TYPE is of type smallint, so no quotes around the value here!
            sql += ` AND ${this.tableCfg.COL_ATTR_TYPE} = ${this.attributeTypes.indexOf(instance['@type'])}`
        }

        //################# END Build SQL query to update attribute instance #####################

        sql += ';'

        return sql
    }


    makeUpdateEntityModifiedAtQuery(entityInternalId: number): string {
        const now = new Date()

        return `UPDATE ${this.tableCfg.TBL_ENT} SET ${this.tableCfg.COL_ENT_MODIFIED_AT} = '${now.toISOString()}' WHERE ${this.tableCfg.COL_ENT_INTERNAL_ID} = ${entityInternalId};`
    }


    // Spec 5.7.2
    async queryEntities(query: Query, temporal: boolean, includeSysAttrs: boolean, context: JsonLdContextNormalized): Promise<Array<any>> {

        //########################### BEGIN Validation ###########################      

        const queryCheckResult = checkQuery(query)

        if (queryCheckResult.length > 0) {
            throw errorTypes.BadRequestData.withDetail("Invalid query: " + queryCheckResult.join(". "))
        }

        if (query.geoQ != undefined) {

            const geoQueryCheckResult = checkGeoQuery(query.geoQ)

            if (geoQueryCheckResult.length > 0) {
                throw errorTypes.BadRequestData.withDetail(geoQueryCheckResult.join(". "))
            }
        }

        if (query.temporalQ != undefined) {
            // TODO: Validate temporal query
        }

        // TODO: 4: "If the list of Entity identifiers includes a URI which it is not valid, 
        // or the query, geo-query or context source filter are not syntactically valid 
        // (as per the referred clauses 4.9 and 4.10) an error of type BadRequestData
        // shall be raised.

        //############################# END Validation #########################

        let orderBySql = undefined
        let lastN = undefined


        let sql_where = ""

        //########## BEGIN Build entity IDs and types filter expression from EntityInfo array #############
        const entityTypes_expanded: Array<string> = []
        const entityIds: Array<string> = []
        const idPatterns: Array<string> = []

        if (query.entities instanceof Array) {

            for (const ei of query.entities) {

                if (typeof (ei.type) == "string") {

                    entityTypes_expanded.push(expandObject(ei.type, context))
                }

                if (typeof (ei.id) == "string") {
                    entityIds.push(ei.id)
                }

                if (typeof (ei.idPattern) == "string") {
                    idPatterns.push(ei.idPattern)
                }
            }
        }


        if (entityTypes_expanded.length > 0) {
            sql_where += ` AND t1.${this.tableCfg.COL_ENT_TYPE} IN ('${entityTypes_expanded.join("','")}')`
        }

        if (entityIds.length > 0) {
            sql_where += ` AND t1.${this.tableCfg.COL_ENT_ID} IN ('${entityIds.join("','")}')`
        }

        // TODO: 1 ADD FEATURE - "id matches the id patterns passed as parameter"


        //########## END Build entity IDs and types filter expression from EntityInfo array #############



        //####################### BEGIN Match specified Attributes #######################
        // - "attribute matches any of the expanded attribute(s) in the list that is passed as parameter":

        // NOTE: The addition of this condition also automatically covers spec 5.7.2.6: 
        // "For each matching Entity only the Attributes specified by the Attribute list 
        // parameter shall be included."


        if (query.attrs instanceof Array && query.attrs.length > 0) {

            const attrs_expanded = expandObject(query.attrs, context)

            sql_where += ` AND t2.${this.tableCfg.COL_ATTR_NAME} IN ('${attrs_expanded.join("','")}')`
        }
        //####################### END Match specified Attributes #######################



        //#################### BEGIN Match NGSI-LD query #################

        // - "the filter conditions specified by the query are met (as mandated by clause 4.9)":
        if (query.q != undefined) {

            const ngsi_query_sql = await this.ngsiQueryParser.makeQuerySql(query, context)

            sql_where += ` AND t1.${this.tableCfg.COL_ENT_INTERNAL_ID} IN ${ngsi_query_sql}`
        }
        //#################### END Match NGSI-LD query #################


        //####################### BEGIN Match GeoQuery #######################

        // - "the geospatial restrictions imposed by the geoquery are met (as mandated by clause 4.10).
        // if there are multiple instances of the GeoProperty on which the geoquery is based, 
        // it is sufficient if any of these instances meets the geospatial restrictions":

        if (query.geoQ != undefined) {
            sql_where += ` AND t1.${this.tableCfg.COL_ENT_INTERNAL_ID} IN ${makeGeoQueryCondition(query.geoQ, context, this.tableCfg)}`
        }
        //####################### END Match GeoQuery #######################




        // TODO: 2 - "the entity is available at the Context Source(s) that match the context source filter conditions."


        // TODO: 2 - "if the Attribute list is present, in order for an Entity to match, 
        //            it shall contain at least one of the Attributes in the Attribute list."

        // NOTE that this is related to:

        // TODO: 2 Ask/understand what is the difference between

        // "- attribute matches any of the expanded attribute(s) in the list that is passed as parameter;"

        // and

        // "if the Attribute list is present, in order for an Entity to match, 
        // it shall contain at least one of the Attributes in the Attribute list."


        // TODO: 4 "Pagination logic shall be in place as mandated by clause 5.5.9."

        // TODO: 4 All other things in 5.7.2.4 that are still missing


        //################### BEGIN Match temporal query ######################
        if (query.temporalQ != undefined) {

            sql_where += makeTemporalQueryCondition(query.temporalQ, this.tableCfg)

            orderBySql = this.getTemporalTableColumn(query.temporalQ.timeproperty) + " DESC"
            lastN = query.temporalQ.lastN
        }
        //################### END Match temporal query ######################



        const attrNames_expanded = expandObject(query.attrs, context) as Array<string>

        // Run query and return result:
        return await this.getEntitiesBySqlWhere(sql_where, includeSysAttrs, orderBySql, lastN, attrNames_expanded, temporal)
    }


    private async runSqlQuery(sql: string): Promise<pg.QueryResult> {

        const resultPromise = this.pool.query(sql)

        //console.log(sql)
        // Print error, but still continue with the normal promise chain:

        resultPromise.then(null, (e) => {
            console.log()
            console.log("Something went wrong:")
            console.log("------------------------------")
            console.log(e)
            console.log("------------------------------")
        })

        return resultPromise
    }


    async updateAttributeInstance(entityId: string, instanceId_expanded: string, instance: any) {

        //####################### BEGIN Try to fetch existing entity ###########################
        const entityMetadata = await this.getEntityMetadata(entityId)

        if (entityMetadata == undefined) {
            throw errorTypes.ResourceNotFound.withDetail(`No entity with ID '${entityId}' exists.`)
        }
        //####################### END Try to fetch existing entity ###########################

        const instanceId_number = parseInt(instanceId_expanded.split("_")[1])

        const query = this.makeUpdateAttributeInstanceQuery(instanceId_number, instance, false)

        const queryResult = await this.runSqlQuery(query)
        const queryResult2 = await this.runSqlQuery(this.makeUpdateEntityModifiedAtQuery(entityMetadata.id))
    }


    /*
    async updateEntityAttributes(entityId: string, fragment_expanded: any, attributeIdToUpdate: string | undefined) {

        // TODO: Compare this with appendEntityAttributes and see if we can merge them

        //############# BEGIN Get internal ID of entity #############
        const entityMetadata = await this.getEntityMetadata(entityId)

        if (!entityMetadata) {
            throw errorTypes.ResourceNotFound.withDetail("No entity with the passed ID exists: " + entityId)
        }

        const entityInternalId = entityMetadata.id
        //############# END Get internal ID of entity #############


        const result = new UpdateResult()



        //####################### BEGIN Iterate over attributes #############################
        for (const attributeId_expanded in fragment_expanded) {

            let sql_transaction = "BEGIN;"

            if (attributeIdToUpdate != undefined && attributeId_expanded != attributeIdToUpdate) {
                continue
            }

            // Do not process @id, @type and @context:
            if (ignoreAttributes.includes(attributeId_expanded)) {
                continue
            }

            let attribute_expanded = fragment_expanded[attributeId_expanded]

            if (!(attribute_expanded instanceof Array)) {
                attribute_expanded = [attribute_expanded]
            }


            //#################### BEGIN Validate attribute ####################
            const reifiedAttributeCheck = checkReifiedAttribute(attribute_expanded, attributeId_expanded, undefined, false)

            if (reifiedAttributeCheck.length > 0) {

                let errorMsg = ""

                for (const msg of reifiedAttributeCheck) {
                    errorMsg += msg + "\n"
                }

                result.notUpdated.push(new NotUpdatedDetails(attributeId_expanded, "Not a valid reified attribute: \n" + errorMsg))

                continue
            }
            //#################### END Validate attribute ####################

            let updated = false

            //############ BEGIN Iterate over attribute instances ###############
            for (const instance_expanded of attribute_expanded) {

                const datasetId = instance_expanded['https://uri.etsi.org/ngsi-ld/datasetId']
                const existingInstances = await this.getAttributeInstances(entityInternalId, attributeId_expanded, datasetId)


                let lastCreatedInstance: any = null

                for (const exInst of existingInstances) {
                    if (lastCreatedInstance == null || exInst.instance_id > lastCreatedInstance.instance_id) {
                        lastCreatedInstance = exInst
                    }
                }

                if (lastCreatedInstance != null) {
                    sql_transaction += this.makeUpdateAttributeInstanceQuery(lastCreatedInstance.instance_id, instance_expanded, true)
                    updated = true
                }

            }
            //############## END Iterate over attribute instances ##################

            if (updated) {
                result.updated.push(attributeId_expanded)

                sql_transaction += this.makeUpdateEntityModifiedAtQuery(entityInternalId)
                sql_transaction += "COMMIT;"


                await this.runSqlQuery(sql_transaction)
            }
            else {

                result.notUpdated.push(new NotUpdatedDetails(attributeId_expanded, "No attribute instance(s) with the specified attribute ID and instance ID(s) exists."))
            }


        }
        //####################### END Iterate over attributes #############################




        return new Promise<UpdateResult>((resolve, reject) => {
            resolve(result)
        })
    }
        */

}