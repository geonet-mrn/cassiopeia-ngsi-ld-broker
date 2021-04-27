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
import { TemporalQuery } from '../dataTypes/TemporalQuery'
import { InsertQueryBuilder } from './InsertQueryBuilder'
import { PsqlTableConfig } from './PsqlTableConfig'
import { makeGeoQueryCondition } from './makeGeoQueryCondition'
import { makeTemporalQueryCondition } from './makeTemporalQueryCondition'
import { compactObject, expandObject } from '../jsonld'
import { JsonLdContextNormalized } from 'jsonld-context-parser'
import { UpdateResult } from '../dataTypes/UpdateResult'
import { NotUpdatedDetails } from '../dataTypes/NotUpdatedDetails'


export class PsqlBackend {

    readonly tableCfg = new PsqlTableConfig()

    // ATTENTION: Changing the order of items in attributeTypes corrupts the database!
    private readonly attributeTypes = ["https://uri.etsi.org/ngsi-ld/Property", "https://uri.etsi.org/ngsi-ld/GeoProperty", "https://uri.etsi.org/ngsi-ld/Relationship"]



    // The PostgreSQL connection object:
    private readonly pool!: pg.Pool

    private readonly ngsiQueryParser = new NgsiLdQueryParser(this.tableCfg)


    constructor(config: any, private ngsiLdCoreContext: JsonLdContextNormalized) {

        this.pool = new pg.Pool(config.psql)
    }



    async addAttributesToTemporalEntity(entityId: string, fragment_expanded: any) {

        //###################### BEGIN Try to fetch existing entity ########################
        const entityMetadata = await this.getEntityMetadata(entityId, true)

        if (!entityMetadata) {
            throw errorTypes.ResourceNotFound.withDetail("No entity with the passed ID exists: " + entityId)
        }
        //###################### END Try to fetch existing entity ########################


        // "For each Attribute included by the Entity Fragment at root level":

        //####################### BEGIN Iterate over attributes #############################
        for (const attributeId in fragment_expanded) {

            let attribute = (fragment_expanded as any)[attributeId]

            if (!isReifiedAttribute(attribute)) {
                continue
            }

            if (!(attribute instanceof Array)) {
                attribute = [attribute]
            }

            //#################### BEGIN Iterate over attribute instances #####################

            let sql_transaction = "BEGIN;"

            for (const instance of attribute) {
                //await this.psql.runSqlQuery(this.psql.makeCreateAttributeQuery(entityMetadata.id, attributeId, instance))
                sql_transaction += this.makeCreateAttributeQuery(entityMetadata.id, attributeId, instance)
            }

            sql_transaction += "COMMIT;"

            await this.runSqlQuery(sql_transaction)
        }
        //################## END Iterate over attributes #######################
    }



    async countAttributeInstances(entityInternalId: number, attributeName: string, datasetId: string | null | undefined): Promise<number> {

        let sql = `SELECT COUNT(*) FROM ${this.tableCfg.TBL_ATTR} WHERE eid = ${entityInternalId} `

        sql += ` AND ${this.tableCfg.COL_ATTR_NAME} = '${attributeName}'`


        if (datasetId != undefined) {
            sql += ` AND ${this.makeSqlCondition_datasetId(datasetId)}`
        }



        const sqlResult = await this.runSqlQuery(sql)

        return new Promise((resolve, reject) => {
            resolve(sqlResult.rows[0].count)
        })
    }


    async countEntitiesByType(type: string): Promise<number> {

        let sql = `SELECT COUNT(*) FROM ${this.tableCfg.TBL_ENT} WHERE ${this.tableCfg.COL_ENT_TYPE} = '${type}'`


        let sqlResult = await this.runSqlQuery(sql)

        return new Promise((resolve, reject) => {
            resolve(sqlResult.rows[0].count)
        })
    }


    async createEntity(entity: any, temporal: boolean): Promise<number> {


        const now = new Date()

        // Begin construction of transaction query:
        let query_transaction = "BEGIN;"


        //############## BEGIN Build INSERT query for entities table ###########
        const queryBuilder = new InsertQueryBuilder()

        queryBuilder.add(this.tableCfg.COL_ENT_ID, entity['@id'])
        queryBuilder.add(this.tableCfg.COL_ENT_TYPE, entity['@type'])
        queryBuilder.add(this.tableCfg.COL_ENT_CREATED_AT, now.toISOString())
        queryBuilder.add(this.tableCfg.COL_ENT_MODIFIED_AT, now.toISOString())
        queryBuilder.add(this.tableCfg.COL_ENT_TEMPORAL, temporal.toString())
        //############## END Build INSERT query for entities table ###########

        // Create row in entity metadata table:     
        query_transaction += queryBuilder.getStringForTable(this.tableCfg.TBL_ENT)

        //################## BEGIN Create rows in attributes table ##################
        for (const attributeId in entity) {

            let attribute = (entity as any)[attributeId]

            let checkResult = checkReifiedAttribute(attribute, attributeId, undefined, false)


            if (checkResult.length > 0) {
                continue
            }


            if (!(attribute instanceof Array)) {
                attribute = [attribute]
            }

            for (const instance of attribute) {
                query_transaction += this.makeCreateAttributeQuery(-1, attributeId, instance)
            }
        }
        //################## END Create rows in attributes table ##################


        // Finish construction of transaction query:
        query_transaction += "COMMIT;"

        // Run transaction query:

        // TODO: 1 Don't catch SQL exception here
        let transactionResult: any = await this.runSqlQuery(query_transaction).catch((error: any) => {

            if (error.code == "23505") {
                return new Promise<number>((resolve, reject) => {
                    reject(error.code)
                })
            }
        })


        return new Promise<number>((resolve, reject) => {
            resolve(1)
        })
    }


    async createOrUpdateTemporalEntity(entity_expanded: any) {

        const entityMetadata = await this.getEntityMetadata(entity_expanded['@id'], true)


        // If the entity doesn't exist yet, create it:

        if (entityMetadata == undefined) {

            const createResult = await this.createEntity(entity_expanded, true)

            return new Promise<number>((resolve, reject) => {
                resolve(201)
            })
        }


        // Otherwise, update it (append attributes):

        // Begin construction of transaction query:
        let query_transaction = "BEGIN;"

        //####################### BEGIN Iterate over attributes #############################
        for (const attributeId in entity_expanded) {

            let attribute = (entity_expanded as any)[attributeId]

            if (!isReifiedAttribute(attribute)) {
                continue
            }

            if (!(attribute instanceof Array)) {
                attribute = [attribute]
            }

            //#################### BEGIN Iterate over attribute instances #####################
            for (const instance of attribute) {
                query_transaction += this.makeCreateAttributeQuery(entityMetadata.id, attributeId, instance)
            }
            //################## END Iterate over attribute instances #######################
        }
        //####################### END Iterate over attributes #############################

        // Finish construction of transaction query:
        query_transaction += "COMMIT;"


        // Run transaction query:
        const transactionResult: any = await this.runSqlQuery(query_transaction).catch((error: any) => {

            // TODO: What errors can happen here?
            /*
            if (error.code == "23505") {
                return new Promise<number>((resolve, reject) => {
                    reject(error.code)
                })
            }
            */
        })

        return new Promise<number>((resolve, reject) => {
            resolve(204)
        })
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
            sql += ` AND ${this.tableCfg.COL_INSTANCE_ID} = ${instanceId_number}`
        }


        // Match dataset ID if provided:
        if (datasetId != undefined) {
            sql += ` AND ${this.makeSqlCondition_datasetId(datasetId)}`
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

        let queryResult1 = await this.runSqlQuery(sql_delete_entity_metadata)

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
        let queryResult2 = await this.runSqlQuery(sql_delete_attributes)
        //############ END Build and run transaction query to delete all attribute rows ###########


        // Return number of deleted rows as promise:
        return new Promise((resolve, reject) => {
            resolve(true)
        })
    }


    async getAttribute(entityId: string, attributeId: string, datasetId: string | null | undefined, includeSysAttrs: boolean) {

        let sql_where = ` AND ${this.tableCfg.COL_ENT_ID} = '${entityId}' AND ${this.tableCfg.COL_ATTR_NAME} = '${attributeId} `

        if (datasetId != undefined) {
            sql_where += ` AND ${this.makeSqlCondition_datasetId(datasetId)}`
        }

        return this.getEntitiesBySqlWhere(sql_where, includeSysAttrs, undefined, undefined)
    }


    async getAvailableAttributes(): Promise<AttributeList> {

        // TODO: 2 Should default attributes like "createdAt" be included here?

        let result = new AttributeList()

        let sql = `SELECT DISTINCT ${this.tableCfg.COL_ATTR_NAME} FROM ${this.tableCfg.TBL_ATTR}`

        let sqlResult = await this.runSqlQuery(sql)

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

        // TODO: Differentiate between temporal and non-temporal entities?
        const entityCount = await this.countEntitiesByType(type)

        const sql = `SELECT DISTINCT ${this.tableCfg.COL_ATTR_NAME} FROM ${this.tableCfg.TBL_ENT} as t1, ${this.tableCfg.TBL_ATTR} as t2 WHERE t1.${this.tableCfg.COL_ENT_INTERNAL_ID} = t2.eid AND ${this.tableCfg.COL_ENT_TYPE} = '${type}'`

        const sqlResult = await this.runSqlQuery(sql)



        let result = new EntityTypeInfo(type, entityCount)

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


    async getEntitiesBySqlWhere(sql_where: string, includeSysAttrs: boolean, orderBySql: string | undefined, lastN: number | undefined): Promise<Array<any>> {


        // TODO: 2 Hard-coded only for testing
        //includeSysAttrs = true


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

        //console.log(queryResult)

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
                    entity["https://uri.etsi.org/ngsi-ld/createdAt"] = row[this.tableCfg.COL_ENT_CREATED_AT]
                    entity["https://uri.etsi.org/ngsi-ld/modifiedAt"] = row[this.tableCfg.COL_ENT_MODIFIED_AT]
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


            //####### BEGIN Restore JSON fields that have their own database column ##########
            if (includeSysAttrs) {
                instance["https://uri.etsi.org/ngsi-ld/createdAt"] = row["attr_created_at"]
                instance["https://uri.etsi.org/ngsi-ld/modifiedAt"] = row["attr_modified_at"]

                if (row["attr_observed_at"] != null) {
                    instance["https://uri.etsi.org/ngsi-ld/observedAt"] = row["attr_observed_at"]
                }

                // TODO: 1 Add method to create instance ID string from number
                instance["https://uri.etsi.org/ngsi-ld/instanceId"] = "urn:ngsi-ld:InstanceId:instance_" + row[this.tableCfg.COL_INSTANCE_ID]
            }
            //####### END Restore JSON fields that have their own database column ##########

            attribute.push(instance)
        }
        //#################### END Iterate over returned attribute instance rows ####################


        let result = Array<any>()

        for (let key in entitiesByNgsiId) {
            result.push(entitiesByNgsiId[key])
        }

        console.log("# returned entities: " + result.length)

        return new Promise((resolve, reject) => {
            resolve(result)
        })
    }


    async getEntity(entityId: string,
        temporal: boolean,
        attrNames: Array<string> | undefined,
        temporalQ: TemporalQuery | undefined,
        includeSysAttrs: boolean): Promise<any> {


        // ATTENTION:

        // There is Spec 4.5.5:
        // "In case of conflicting information for an Attribute, where a datasetId is duplicated, 
        // but there are differences in the other attribute data, the one with the most recent 
        // observedAt DateTime, if present, and otherwise the one with the most recent
        //  modifiedAt DateTime shall be provided.".

        // HOWEVER, in order to implement this, we CAN NOT simply set lastN = 1 and order by observedAt
        // and modifiedAt here, since we still want to retrieve all attribute instances with different
        // datasetIds!

        let orderBySql: string | undefined = undefined
        let lastN: number | undefined = undefined


        let sql_where = ` AND t1.${this.tableCfg.COL_ENT_ID} = '${entityId}' AND t1.${this.tableCfg.COL_ENT_TEMPORAL} = ${temporal.toString()}`

        //############### BEGIN Only return selected attributes #################
        if (attrNames instanceof Array) {
            const wherePieces = []

            for (const attr of attrNames) {
                wherePieces.push(`t2.${this.tableCfg.COL_ATTR_NAME} = '${attr}'`)
            }

            sql_where += " AND (" + wherePieces.join(" OR ") + ") "
        }
        //############### END Only return selected attributes #################


        // ############# BEGIN Only return attribute instances within temporal query interval #############
        if (temporalQ != undefined) {
            sql_where += makeTemporalQueryCondition(temporalQ, this.tableCfg)

            orderBySql = this.getTemporalTableColumn(temporalQ.timeproperty) + " DESC"

            lastN = temporalQ.lastN
        }

        // ############# END Only return attribute instances within temporal query interval #############


        // Fetch matching entities by SQL. If everything is correct, no more than one should be returned:
        const entities = await this.getEntitiesBySqlWhere(sql_where, includeSysAttrs, orderBySql, lastN)


        if (entities.length == 0) {
            throw errorTypes.ResourceNotFound.withDetail("No entity found.")
        }
        else if (entities.length > 1) {
            throw errorTypes.InternalError.withDetail("More than one entity with the same ID was found. This is a database corruption and should never happen.")
        }

        const entity: any = entities[0]


        //############# BEGIN Add empty arrays for requested attributes with no matching instances #############
        // "For the avoidance of doubt, if for a requested Attribute no instance fulfils the temporal query, 
        // then an empty Array of instances shall be provided as the representation for such Attribute.":

        if (attrNames instanceof Array) {

            for (const attributeName of attrNames) {
                for (const e of entities) {

                    const entity = e as any

                    if (entity[attributeName] == undefined) {
                        entity[attributeName] = []
                    }
                }
            }
        }
        //############# END Add empty arrays for requested attributes with no matching instances #############


        return new Promise((resolve, reject) => {
            resolve(entity)
        })
    }


    async getEntityMetadata(entityId: string, temporal: boolean): Promise<any> {

        const sql = `SELECT * FROM ${this.tableCfg.TBL_ENT} WHERE ${this.tableCfg.COL_ENT_ID} = '${entityId}' AND ${this.tableCfg.COL_ENT_TEMPORAL} = ${temporal.toString()}`

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


    makeCreateAttributeQuery(entityInternalId: number, attributeId: string, instance: any): string {

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

        queryBuilder.add(this.tableCfg.COL_ATTR_NAME, attributeId)
        queryBuilder.add(this.tableCfg.COL_ATTR_TYPE, this.attributeTypes.indexOf(instance.type))
        queryBuilder.add(this.tableCfg.COL_DATASET_ID, instance['https://uri.etsi.org/ngsi-ld/datasetId'])
        queryBuilder.add(this.tableCfg.COL_INSTANCE_JSON, JSON.stringify(instance))

        // Write 'geom' column:
        if (instance['@type'] == "https://uri.etsi.org/ngsi-ld/GeoProperty") {

            const geojson_expanded = instance['https://uri.etsi.org/ngsi-ld/hasValue']

            // TODO: 1 Is it correct to simply use the NGSI-LD core context here?
            const geojson_compacted = compactObject(geojson_expanded, this.ngsiLdCoreContext)

            const geojson_string = JSON.stringify(geojson_compacted)

            queryBuilder.add("geom", `ST_SetSRID(ST_GeomFromGeoJSON('${geojson_string}'), 4326)`, true)
        }

        // Write 'observed_at' column:
        if (isDateTimeUtcString(instance["https://uri.etsi.org/ngsi-ld/observedAt"])) {
            queryBuilder.add(this.tableCfg.COL_ATTR_OBSERVED_AT, instance["https://uri.etsi.org/ngsi-ld/observedAt"])
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


    makeSqlCondition_datasetId(datasetId: string | null): string {

        if (datasetId == null) {
            return `${this.tableCfg.COL_DATASET_ID} is null`
        }
        else {
            return `${this.tableCfg.COL_DATASET_ID} = '${datasetId}'`
        }
    }


    makeUpdateAttributeInstanceQuery(entityInternalId: number,
        attributeId: string,
        instanceId: string | undefined,
        instance: any,
        allowAttributeTypeChange: boolean): string {


        // ATTENTION: 
        // This method will replace ALL attribute instances that 
        // have the same datasetId as the passed instance object!!

        // If this method is used on a "temporal" Entity, all instances of the attribute, regardless of
        // their time stamp, are updated/replaced!

        // Means: You *can* use this method on a "temporal" Entity, but YOU PROBABLY DON'T WANT TO!

        // Generally, the API endpoints for "normal" Entities should not be used to modify "temporal" entities.


        // NOTE: This method returns a Promise that contains the number of updated attribute instances.


        const now = new Date()


        //################# BEGIN Build SQL query to update attribute instance #####################
        let sql = `UPDATE ${this.tableCfg.TBL_ATTR} SET ${this.tableCfg.COL_INSTANCE_JSON} = '${JSON.stringify(instance)}'`

        // Write 'geom' column:
        if (instance['@type'] == "https://uri.etsi.org/ngsi-ld/GeoProperty") {

            const geojson_expanded = instance['https://uri.etsi.org/ngsi-ld/hasValue']

            // TODO: 1 Is it okay to simply use the NGSI-LD core context here?
            const geojson_compacted = this.ngsiLdCoreContext.compactIri(geojson_expanded, true)

            const geojson_string = JSON.stringify(geojson_compacted)

            sql += `, geom = ST_SetSRID(ST_GeomFromGeoJSON('${geojson_string}'), 4326)`
        }

        // Write 'modified_at' column:        
        sql += `, ${this.tableCfg.COL_ATTR_MODIFIED_AT} = '${now.toISOString()}'`


        // Write 'observed_at' column:
        if (isDateTimeUtcString(instance["https://uri.etsi.org/ngsi-ld/observedAt"])) {
            sql += `, ${this.tableCfg.COL_ATTR_OBSERVED_AT} = '${instance["https://uri.etsi.org/ngsi-ld/observedAt"]}'`
        }

        // Add WHERE conditions:

        sql += ` WHERE eid = ${entityInternalId} AND ${this.tableCfg.COL_ATTR_NAME} = '${attributeId}'`
        sql += " AND " + this.makeSqlCondition_datasetId(instance['https://uri.etsi.org/ngsi-ld/datasetId'])

        if (instanceId != undefined) {
            // TODO: 1 Make function to get instance number from instance ID string
            const instanceId_number = parseFloat(instanceId.split("_")[1])

            sql += ` AND ${this.tableCfg.COL_INSTANCE_ID} = ${instanceId_number}`
        }

        if (!allowAttributeTypeChange) {
            // ATTENTION: COL_ATTR_TYPE is of type smallint, so no quotes around the value here!
            sql += ` AND ${this.tableCfg.COL_ATTR_TYPE} = ${this.attributeTypes.indexOf(instance['@type'])}`
        }

        //################# END Build SQL query to update attribute instance #####################


        // Add SQL query to update entity:
        //sql_transaction += `; UPDATE ${this.tableCfg.TBL_ENT} SET ${this.tableCfg.COL_ENT_MODIFIED_AT} = '${now.toISOString()}' WHERE ${this.tableCfg.COL_ENT_INTERNAL_ID} = ${entityInternalId};`
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

        sql_where += ` AND t1.${this.tableCfg.COL_ENT_TEMPORAL} = ${temporal.toString()} `

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


        if (query.attrs != undefined && query.attrs.length > 0) {
            
            console.log(JSON.stringify(context))
            
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


        // Run query and return result:
        const entities_expanded = await this.getEntitiesBySqlWhere(sql_where, includeSysAttrs, orderBySql, lastN)


        //########################## BEGIN Post-process returned entities #########################
        for (const ex of entities_expanded) {

            const entity_expanded = ex as any

            //############# BEGIN Add empty arrays for requested attributes with no matching instances #############

            // "For the avoidance of doubt, if for a requested Attribute no instance fulfils the temporal query, 
            // then an empty Array of instances shall be provided as the representation for such Attribute.":

            if (query.attrs instanceof Array) {

                const attrs_expanded = expandObject(query.attrs, context)

                for (const attrName_expanded of attrs_expanded) {
                    if (entity_expanded[attrName_expanded] == undefined) {
                        entity_expanded[attrName_expanded] = []
                    }
                }
            }
            //############# END Add empty arrays for requested attributes with no matching instances #############
        }
        //########################## END Post-process returned entities #########################

        return entities_expanded
    }


    private async runSqlQuery(sql: string): Promise<pg.QueryResult> {

        //console.log(sql)
        console.log("-------------------------")

        const result = this.pool.query(sql)

        // Print error, but still continue with the normal promise chain:
        result.then(null, (e) => {
            console.log()
            console.log("Something went wrong:")
            console.log("------------------------------")
            console.log(e)
            console.log("------------------------------")
        })

        return result
    }


    async updateAttributeInstanceOfTemporalEntity(entityId: string, attributeId_expanded : string, instanceId_expanded : string, instance: any) {

        //####################### BEGIN Try to fetch existing entity ###########################
        const entityMetadata = await this.getEntityMetadata(entityId, true)

        if (entityMetadata == undefined) {
            throw errorTypes.ResourceNotFound.withDetail(`No entity with ID '${entityId}' exists.`)
        }
        //####################### END Try to fetch existing entity ###########################


        let sql_transaction = "BEGIN;"
        sql_transaction += this.makeUpdateAttributeInstanceQuery(entityMetadata.id, attributeId_expanded, instanceId_expanded, instance, false)
        sql_transaction += this.makeUpdateEntityModifiedAtQuery(entityMetadata.id)

        sql_transaction += "COMMIT;"

        await this.runSqlQuery(sql_transaction)
    }


    async appendEntityAttributes(entityId: any, fragment_expanded: any, overwrite: boolean) {

        let result = new UpdateResult()

        //########### BEGIN Try to fetch existing entity with same ID from the database #############
        const entityMetadata = await this.getEntityMetadata(entityId, false)

        if (!entityMetadata) {
            throw errorTypes.ResourceNotFound.withDetail("No entity with the passed ID exists: " + entityId)
        }

        const entityInternalId = entityMetadata.id
        //########### END Try to fetch existing entity with same ID from the database #############


        // "For each Attribute included by the Entity Fragment at root level":

        //####################### BEGIN Iterate over attributes #############################
        for (const attributeId in fragment_expanded) {

            let attribute = (fragment_expanded as any)[attributeId]

            if (!isReifiedAttribute(attribute)) {
                continue
            }


            if (!(attribute instanceof Array)) {
                attribute = [attribute]
            }


            let updated = false


            // NOTE: The code here looks quite different from the algorithm described in the 
            // specification. However, (I think) it really does the same.

            //#################### BEGIN Iterate over attribute instances #####################

            let sql_transaction = "BEGIN;"

            for (const instance of attribute) {

                let numExistingInstancesWithSameDatasetId = await this.countAttributeInstances(entityInternalId, attributeId, instance['https://uri.etsi.org/ngsi-ld/datasetId'])

                if (numExistingInstancesWithSameDatasetId == 0) {
                    //await this.psql.runSqlQuery(this.psql.makeCreateAttributeQuery(entityInternalId, attributeId, instance))
                    sql_transaction += this.makeCreateAttributeQuery(entityInternalId, attributeId, instance)

                    updated = true
                }
                else {
                    if (overwrite) {
                        //await this.psql.updateAttributeInstance(entityInternalId, attributeId, undefined, instance, true)
                        sql_transaction += this.makeUpdateAttributeInstanceQuery(entityInternalId, attributeId, undefined, instance, true)

                        updated = true
                    }
                }
            }
            //################## END Iterate over attribute instances #######################

            sql_transaction += this.makeUpdateEntityModifiedAtQuery(entityInternalId)

            sql_transaction += "COMMIT;"

            if (updated) {

                await this.runSqlQuery(sql_transaction)

                result.updated.push(attributeId)
            }
            else {
                result.notUpdated.push(new NotUpdatedDetails(attributeId, "Attribute instance(s) already exist and no overwrite was ordered."))
            }
        }
        //####################### END Iterate over attributes #############################


        return new Promise<UpdateResult>((resolve, reject) => {
            resolve(result)
        })
    }



    async partialAttributeUpdate(entityId: any, attributeId_expanded: string, attribute_expanded: any) {

        const existingEntityMetadata = await this.getEntityMetadata(entityId, false)

        if (existingEntityMetadata == undefined) {
            throw errorTypes.ResourceNotFound.withDetail(`No entity with ID '${entityId}' exists.`)
        }


        let sql_transaction = "BEGIN;"

        for (const instance of attribute_expanded) {

            // ATTENTION: Since we use a SQL transaction for this, it is not (easily) possible to determine the
            // number of affected rows. This means that we can't tell whether the target attribute exists in
            // the database.

            // The specification demands that a ResourceNotFound error is thrown if the attribute does not
            // exist. In order to implement this, we perform an SQL query to count the number of existing
            // attribute instances with the specified datasetId:

            const numRowsAffected = await this.countAttributeInstances(existingEntityMetadata.id, attributeId_expanded, instance['https://uri.etsi.org/ngsi-ld/datasetId'])

            // Throw error if no attribute instances with same datasetId are found:
            if (numRowsAffected == 0) {
                throw errorTypes.ResourceNotFound.withDetail(`No instance of attribute '${attributeId_expanded}' with dataset ID '${instance['https://uri.etsi.org/ngsi-ld/datasetId']}' exists on the target entity '${entityId}'.`)
            }

            // Throw error if more than one attribute instance with same datasetId is found (should never happen!):
            if (numRowsAffected > 1) {
                throw errorTypes.InternalError.withDetail(`Multiple attribute instances were updated in an operation that should affect one attribute instance at most. This is sign of invalid database content and should never happen. Entity ID: '${entityId}', Attribute ID: '${attributeId_expanded}', Dataset ID: '${instance['https://uri.etsi.org/ngsi-ld/datasetId']}'.`)
            }

            // If *exactly one* attribute instance with same datasetId is found, update it:
            else {
                sql_transaction += this.makeUpdateAttributeInstanceQuery(existingEntityMetadata.id, attributeId_expanded, undefined, instance, true)
            }
        }

        sql_transaction += this.makeUpdateEntityModifiedAtQuery(existingEntityMetadata.id)

        sql_transaction += "COMMIT;"

        this.runSqlQuery(sql_transaction)
    }


    async updateEntityAttributes(entityId: string, fragment_expanded: any) {

        const entityMetadata = await this.getEntityMetadata(entityId, false)

        if (!entityMetadata) {
            throw errorTypes.ResourceNotFound.withDetail("No entity with the passed ID exists: " + entityId)
        }

        const entityInternalId = entityMetadata.id

        let result = new UpdateResult()


        //####################### BEGIN Build transaction query #############################
        let sql_transaction = "BEGIN;"

        //####################### BEGIN Iterate over attributes #############################
        for (const attributeId_expanded in fragment_expanded) {

            let attribute_expanded = fragment_expanded[attributeId_expanded]

            if (!(attribute_expanded instanceof Array)) {
                attribute_expanded = [attribute_expanded]
            }

            if (!isReifiedAttribute(attribute_expanded)) {
                // NOTE: This check is primarily meant to filter out attributes like "id" and "type",
                // and not for actual validation. This happens earlier.
                continue
            }

            const numInstances = await this.countAttributeInstances(entityInternalId, attributeId_expanded, undefined)

            if (numInstances > 0) {

                //############ BEGIN Iterate over attribute instances ###############
                for (const instance of attribute_expanded) {

                    // NOTE: The following function call automatically incorporeates spec 5.6.2.4:
                    // The type of an Attribute in the Entity Fragment has to be the same as the type of the 
                    // targeted Attribute fragment, i.e. it is not allowed to change the type of an Attribute.
                    sql_transaction += this.makeUpdateAttributeInstanceQuery(entityInternalId, attributeId_expanded, undefined, instance, false)
                }
                //############## END Iterate over attribute instances ##################

                result.updated.push(attributeId_expanded)
            }
            else {
                result.notUpdated.push(new NotUpdatedDetails(attributeId_expanded, "No attribute with the specified ID exists."))
            }
        }
        //####################### END Build transaction query #############################

        //####################### END Iterate over attributes #############################

        sql_transaction += this.makeUpdateEntityModifiedAtQuery(entityInternalId)

        sql_transaction += "COMMIT;"

        this.runSqlQuery(sql_transaction)


        return new Promise<UpdateResult>((resolve, reject) => {
            resolve(result)
        })
    }

}