import * as pg from 'pg'


import { BatchEntityError } from "./dataTypes/BatchEntityError"
import { BatchOperationResult } from "./dataTypes/BatchOperationResult"
import { Feature } from "./dataTypes/Feature"
import { ProblemDetails } from "./dataTypes/ProblemDetails"
import { Query } from "./dataTypes/Query"
import { TemporalQuery } from "./dataTypes/TemporalQuery"
import { UpdateResult } from "./dataTypes/UpdateResult"
import { errorTypes } from "./errorTypes"
import { checkArrayOfEntities, checkArrayOfUris, checkReifiedAttribute, checkEntity, isUri, isDateTimeUtcString, checkGeoQuery, checkQuery, isReifiedAttribute } from "./validate"
import { appendCoreContext, compactObject, expandObject, getNormalizedContext } from "./jsonld"
import { parseJson, compactedEntityToGeoJsonFeature as compactedEntityToGeoJsonFeature } from "./util"
import * as util from './util'
import { EntityInfo } from "./dataTypes/EntityInfo"

import { PsqlTableConfig } from "./PsqlTableConfig"
import { NgsiLdQueryParser } from "./NgsiLdQueryParser"
import { NotUpdatedDetails } from "./dataTypes/NotUpdatedDetails"
import { SqlQueryBuilder } from "./InsertQueryBuilder"
import { Attribute } from "./dataTypes/Attribute"
import { AttributeList } from "./dataTypes/AttributeList"
import { EntityType } from "./dataTypes/EntityType"
import { EntityTypeInfo } from "./dataTypes/EntityTypeInfo"
import { EntityTypeList } from "./dataTypes/EntityTypeList"
import { JsonLdContextNormalized } from "jsonld-context-parser/lib/JsonLdContextNormalized"
import { makeGeoQueryCondition } from "./makeGeoQueryCondition"
import { makeTemporalQueryCondition } from "./makeTemporalQueryCondition"



const ignoreAttributes = ["@id", "@type", "@context"]

const uri_modifiedAt = "https://uri.etsi.org/ngsi-ld/modifiedAt"
const uri_datasetId = "https://uri.etsi.org/ngsi-ld/datasetId"
const uri_createdAt = "https://uri.etsi.org/ngsi-ld/createdAt"
const uri_instanceId = "https://uri.etsi.org/ngsi-ld/instanceId"
const uri_value = "https://uri.etsi.org/ngsi-ld/hasValue"


const tableCfg = new PsqlTableConfig()

const temporalFields: any = {
    'observedAt': tableCfg.COL_ATTR_OBSERVED_AT,
    'modifiedAt': tableCfg.COL_ATTR_MODIFIED_AT,
    'createdAt': tableCfg.COL_ATTR_CREATED_AT
}

export class ContextBroker {

    // ATTENTION: Changing the order of items in attributeTypes corrupts the database!
    private readonly attributeTypes = ["https://uri.etsi.org/ngsi-ld/Property", "https://uri.etsi.org/ngsi-ld/GeoProperty", "https://uri.etsi.org/ngsi-ld/Relationship"]

    private cfg_autoHistory = true

    // The PostgreSQL connection object:
    private readonly pool!: pg.Pool

    private readonly ngsiQueryParser = new NgsiLdQueryParser(tableCfg)

    constructor(private readonly config: any, private ngsiLdCoreContext: JsonLdContextNormalized) {

        this.pool = new pg.Pool(config.psql)

        if (config.autoHistory != undefined) {
            this.cfg_autoHistory = config.autoHistory
        }
    }

    //############################### BEGIN Official API methods ##################################

    // Spec 5.6.1
    async api_5_6_1_createEntity(entityJson_compacted: string, contextUrl: string | undefined) {

        const entity_from_payload = parseJson(entityJson_compacted)

        if (entity_from_payload == undefined) {
            throw errorTypes.BadRequestData.withDetail("The request payload is not valid JSON")
        }


        const nonNormalizedContext = (contextUrl != undefined) ? contextUrl : entity_from_payload['@context']
        const actualContext = appendCoreContext(nonNormalizedContext)
        const context = await getNormalizedContext(actualContext)

        let entity_expanded = await expandObject(entity_from_payload, context)

        entity_expanded = util.unpackGeoPropertyStringValues(entity_expanded)


        const entityCheckResults = checkEntity(entity_expanded, true)

        if (entityCheckResults.length > 0) {
            throw errorTypes.InvalidRequest.withDetail("The submitted data is not a valid NGSI-LD entity: " + entityCheckResults.join(" "))
        }


        const resultCode = await this.createEntity(entity_expanded, false)

        if (resultCode == -1) {
            throw errorTypes.AlreadyExists.withDetail(`An Entity with the ID '${entity_expanded['@id']}' already exists.`)
        }
    }


    // Spec 5.6.2
    async api_5_6_2_updateEntityAttributes(entityId: string, fragmentString: string, contextUrl: string | undefined): Promise<UpdateResult> {

        //################### BEGIN Validation & Preparation ################

        // TODO: 3 "If the Entity Id is not present or it is not a valid URI then an error of type BadRequestData shall be raised."

        const fragment_compacted = parseJson(fragmentString)

        const nonNormalizedContext = (contextUrl != undefined) ? contextUrl : fragment_compacted['@context']
        const actualContext = appendCoreContext(nonNormalizedContext)
        const context = await getNormalizedContext(actualContext)


        let fragment_expanded = expandObject(fragment_compacted, context)

        fragment_expanded = util.unpackGeoPropertyStringValues(fragment_expanded)

        const entityCheckResults = checkEntity(fragment_expanded, true)

        if (entityCheckResults.length > 0) {
            throw errorTypes.InvalidRequest.withDetail("The submitted data is not a valid NGSI-LD entity: " + entityCheckResults.join(" "))
        }

        //############# BEGIN Get internal ID of entity #############
        const entityMetadata = await this.getEntityMetadata(entityId)

        if (!entityMetadata) {
            throw errorTypes.ResourceNotFound.withDetail("No entity with the passed ID exists: " + entityId)
        }

        const entityInternalId = entityMetadata.id
        //############# END Get internal ID of entity #############

        return await this.writeAttributes(entityInternalId, fragment_expanded, true, false)
    }


    // Spec 5.6.3
    async api_5_6_3_appendEntityAttributes(entityId: string, fragmentJsonString: string, contextUrl: string | undefined, overwrite: boolean): Promise<UpdateResult> {

        const fragment_compacted = parseJson(fragmentJsonString)

        const nonNormalizedContext = (contextUrl != undefined) ? contextUrl : fragment_compacted['@context']
        const actualContext = appendCoreContext(nonNormalizedContext)
        const context = await getNormalizedContext(actualContext)

        let fragment_expanded = expandObject(fragment_compacted, context)

        fragment_expanded = util.unpackGeoPropertyStringValues(fragment_expanded)

        //################### BEGIN Validation ################
        if (!isUri(entityId)) {
            throw errorTypes.BadRequestData.withDetail("No valid NGSI-LD entity ID passed.")
        }

        // NOTE: Here, we change the order of checks compared to the specification, 
        // because it is more performant to first validate the uploaded entity fragment 
        // and only *then* make the database query to check whether an entity with the same ID exists:

        // Validate uploaded entity fragment:
        const entityCheckResults = checkEntity(fragment_expanded, true)

        if (entityCheckResults.length > 0) {
            throw errorTypes.InvalidRequest.withDetail("The submitted data is not a valid NGSI-LD entity fragment: " + entityCheckResults.join(" "))
        }
        //################### END Validation ################


        //########### BEGIN Try to fetch existing entity with same ID from the database #############
        const entityMetadata = await this.getEntityMetadata(entityId)

        if (!entityMetadata) {
            throw errorTypes.ResourceNotFound.withDetail("No entity with the passed ID exists: " + entityId)
        }

        const entityInternalId = entityMetadata.id
        //########### END Try to fetch existing entity with same ID from the database #############

        return await this.writeAttributes(entityInternalId, fragment_expanded, overwrite, true)

    }


    // Spec 5.6.4
    async api_5_6_4_partialAttributeUpdate(entityId: string, attributeId_compacted: string, fragmentJsonString: string, contextUrl: string | undefined) {

        const fragment_compacted = parseJson(fragmentJsonString)

        const nonNormalizedContext = (contextUrl != undefined) ? contextUrl : fragment_compacted['@context']

        const actualContext = appendCoreContext(nonNormalizedContext)
        const context = await getNormalizedContext(actualContext)

        let fragment_expanded = expandObject(fragment_compacted, context)

        fragment_expanded = util.unpackGeoPropertyStringValues(fragment_expanded)

        const attributeId_expanded = expandObject(attributeId_compacted, context)

        //################### BEGIN Input validation ##################
        if (!isUri(entityId)) {
            throw errorTypes.BadRequestData.withDetail("Passend Entity ID is not a valid URI: " + entityId)
        }

        if (!isUri(attributeId_expanded)) {
            throw errorTypes.BadRequestData.withDetail("Passed attribute ID is not a valid URI: " + attributeId_expanded)
        }

        const entityCheckResults = checkEntity(fragment_expanded, true)

        if (entityCheckResults.length > 0) {
            throw errorTypes.InvalidRequest.withDetail("The submitted data is not a valid NGSI-LD entity: " + entityCheckResults.join(" "))
        }



        let fragment_attribute_expanded = fragment_expanded[attributeId_expanded]

        if (fragment_attribute_expanded == undefined) {
            throw errorTypes.BadRequestData.withDetail("The passed fragment does not contain an attribute with the ID specified in the request URL: " + attributeId_expanded)
        }

        // Convert attribute to array representation if it isn't yet:
        if (!(fragment_attribute_expanded instanceof Array)) {
            fragment_attribute_expanded = [fragment_attribute_expanded]
        }

        const attributeCheckResults = checkReifiedAttribute(fragment_attribute_expanded, attributeId_expanded, undefined, true)

        if (attributeCheckResults.length != 0) {
            throw errorTypes.BadRequestData.withDetail(`The field '${attributeId_expanded}' in the uploaded entity fragment is not a valid NGSI-LD attribute: ${attributeCheckResults.join("\n")}`)
        }
        //################### END Input validation ##################




        //############# BEGIN Get internal ID of entity #############
        const entityMetadata = await this.getEntityMetadata(entityId)

        if (!entityMetadata) {
            throw errorTypes.ResourceNotFound.withDetail("No entity with the passed ID exists: " + entityId)
        }

        const entityInternalId = entityMetadata.id
        //############# END Get internal ID of entity #############


        const fragment_reduced: any = {}

        for (const key in fragment_expanded) {
            if (key == attributeId_expanded) {
                fragment_reduced[key] = fragment_expanded[key]
            }
        }

        const updateResult = await this.writeAttributes(entityInternalId, fragment_reduced, true, false)

        // 5.6.4.4: "If the target Entity does not contain the target Attribute ... then an error of type ResourceNotFound shall be raised":
        if (updateResult.updated.length == 0) {
            throw errorTypes.ResourceNotFound.withDetail("The specified entity does not have an attribute with the specified ID: " + attributeId_expanded)
        }
    }


    // Spec 5.6.5
    async api_5_6_5_deleteEntityAttribute(entityId: string, attributeId_compacted: string, datasetId_compacted: string | undefined, contextUrl: string | undefined, deleteAll: boolean) {

        //################## BEGIN Determine actual datasetId to use ################
        let useDatasetId_compacted: string | null | undefined = datasetId_compacted

        // If datasetId is undefined, but 'deleteAll' is not set, this means that the default instance
        // should be deleted, which is characterized by having datasetId = null:

        if (datasetId_compacted == undefined) {
            useDatasetId_compacted = null
        }

        if (deleteAll) {
            useDatasetId_compacted = undefined
        }
        //################## END Determine actual datasetId to use ################

        await this.deleteAttribute(entityId, attributeId_compacted, useDatasetId_compacted, undefined, false, contextUrl)
    }


    // Spec 5.6.6
    async api_5_6_6_deleteEntity(entityId: string) {

        if (!isUri(entityId)) {
            throw errorTypes.BadRequestData.withDetail(`'${entityId}' is not a valid NGSI-LD entity ID.`)
        }

        let result = await this.deleteEntity(entityId).catch((e) => {
            throw errorTypes.ResourceNotFound.withDetail("No entity with the passed ID exists: " + entityId)
        })
    }


    // Spec 5.6.7
    async api_5_6_7_batchEntityCreation(jsonString: string, contextUrl: string | undefined): Promise<BatchOperationResult> {

        const entities_compacted = parseJson(jsonString)

        //############### BEGIN Validate input ###############

        if (!(entities_compacted instanceof Array)) {
            throw errorTypes.BadRequestData.withDetail("The provided payload is not a JSON array")
        }

        const entities_expanded = []

        for (const entity_compacted of entities_compacted) {
            const nonNormalizedContext = (contextUrl != undefined) ? contextUrl : entity_compacted['@context']
            const actualContext = appendCoreContext(nonNormalizedContext)
            const context = await getNormalizedContext(actualContext)

            let entity_expanded = expandObject(entity_compacted, context)

            entity_expanded = util.unpackGeoPropertyStringValues(entity_expanded)

            entities_expanded.push(entity_expanded)
        }

        let checkResult = checkArrayOfEntities(entities_expanded, true, true)

        if (checkResult.length > 0) {
            throw errorTypes.BadRequestData.withDetail(checkResult.join("\n"))
        }
        //############### END Validate input ###############


        //######## BEGIN Iterate over list of uploaded entities and try to write them to the database ########
        const result = new BatchOperationResult()

        for (const entity_compacted of entities_compacted) {

            const nonNormalizedContext = (contextUrl != undefined) ? contextUrl : entity_compacted['@context']
            const actualContext = appendCoreContext(nonNormalizedContext)
            const context = await getNormalizedContext(actualContext)

            const entity_expanded = expandObject(entity_compacted, context)

            const resultCode = await this.createEntity(entity_expanded, false)

            if (resultCode == 1) {
                result.success.push(entity_expanded['@id'])
            }
            else {
                result.errors.push(new BatchEntityError(entity_expanded['@id'], new ProblemDetails("", "Entity creation failed.", "An entity with the same ID already exists.", 409)))
            }
        }
        //######## END Iterate over list of uploaded entities and try to write them to the database ########

        return result
    }


    // Spec 5.6.8
    async api_5_6_8_batchEntityUpsert(jsonString: string, options: string, contextUrl: string | undefined): Promise<BatchOperationResult> {

        // ATTENTION: This method is already implemented as specified in NGSI-LD 1.4.1, as opposed to 1.3.1 like most
        // other parts of Cassipeia. The reason for this is that the NGSI-LD 1.3.1 specification is not clear / contains
        // inconsistencies in the description of what information is returned by the protocol-independent API method
        // (spec 5.6.8) and what information is returned by the HTTP API endpoint as response.

        const entities_compacted = parseJson(jsonString)

        //############### BEGIN Validate input ###############

        if (!(entities_compacted instanceof Array)) {
            throw errorTypes.BadRequestData.withDetail("The provided payload is not a JSON array")
        }

        const entities_expanded = []

        for (const entity_compacted of entities_compacted) {
            const nonNormalizedContext = (contextUrl != undefined) ? contextUrl : entity_compacted['@context']
            const actualContext = appendCoreContext(nonNormalizedContext)
            const context = await getNormalizedContext(actualContext)


            let entity_expanded = expandObject(entity_compacted, context)

            entity_expanded = util.unpackGeoPropertyStringValues(entity_expanded)

            entities_expanded.push(entity_expanded)
        }

        const checkResult = checkArrayOfEntities(entities_expanded, true, true)

        if (checkResult.length > 0) {
            throw errorTypes.BadRequestData.withDetail(checkResult.join("\n"))
        }
        //############### END Validate input ###############


        const entity_ids_created = Array<string>()
        const entity_ids_updated = Array<string>()

        const result = new BatchOperationResult()


        //######## BEGIN Iterate over list of uploaded entities and try to upsert them ########
        for (const entity_expanded of entities_expanded) {

            // Try to fetch entity metadata to check whether or not the entity already exists:
            const existingEntityMetadata = await this.getEntityMetadata(entity_expanded['@id'])


            // ############## BEGIN CREATE the Entity if it does not exist ###############            
            if (!existingEntityMetadata) {

                const resultCode = await this.createEntity(entity_expanded, false)

                if (resultCode == 1) {
                    entity_ids_created.push(entity_expanded['@id'])
                }
                else if (resultCode != 1) {
                    result.errors.push(new BatchEntityError(entity_expanded['@id'], new ProblemDetails("", "Entity creation failed.", "An entity with the same ID already exists.", 409)))
                }
            }
            // ############## END CREATE the Entity if it does not exist ###############


            // ############## BEGIN Otherwise, UPDATE existing entity ###############
            else {

                // "If there were an existing Entity with the same Entity Id, 
                // it shall be completely replaced by the new Entity content provided, 
                // if the requested update mode is 'replace'.":

                if (options == "replace") {

                    // First delete the existing entity:
                    const deleteResult = await this.deleteEntity(entity_expanded['@id']).catch((e) => {
                        // NOTE: If the entity does not exist, deleteEntity() throws an exception.
                        // We can and must ignore this exception. Non-existence of an entity
                        // with the same ID is not a problem here, since this is an UPSERT.

                        // If the delete failed due to another error that is not handled here,
                        // the creation of the replacement entity (next step) will fail, and the
                        // error is handled then.
                    })

                    // NOTE: No need to process return value

                    const resultCode = await this.createEntity(entity_expanded, false)

                    if (resultCode == 1) {
                        entity_ids_updated.push(entity_expanded['@id'])
                    }
                    else {
                        result.errors.push(new BatchEntityError(entity_expanded['@id'], new ProblemDetails("", "Entity replace failed.", "An entity with the same ID already exists.", 409)))
                    }
                }

                // "If there were an existing Entity with the same Entity Id, it shall be executed the 
                // behaviour defined by clause 5.6.3, if the requested update mode is 'update'.":

                else if (options == "update") {

                    const updateResult = await this.writeAttributes(existingEntityMetadata.id, entity_expanded, true, true)

                    // TODO: 3 Add information about failed updates to result?
                    if (updateResult.notUpdated.length == 0) {
                        entity_ids_updated.push(entity_expanded['@id'])
                    }
                    else {
                        // TODO: Add to error message the list of attributes that could not be appended
                        result.errors.push(new BatchEntityError(entity_expanded['@id'], new ProblemDetails("", "Entity update failed.", "Some attributes could not be appended", 409)))
                    }
                }
                else {
                    // Invalid mode. Must be either "replace" or "update"
                }
            }
            // ############## END Otherwise, UPDATE existing entity ###############
        }
        //######## END Iterate over list of uploaded entities and try to upsert them ########


        if (result.errors.length == 0) {
            result.success = entity_ids_created
        }
        else {
            result.success == entity_ids_created.concat(entity_ids_updated)
        }

        return result
    }


    // Spec 5.6.9
    async api_5_6_9_batchEntityUpdate(jsonString: string, contextUrl: string | undefined, overwrite: boolean): Promise<BatchOperationResult> {

        // TODO: 1 What to do with the overwrite parameter?

        const entities_compacted = parseJson(jsonString)

        if (!(entities_compacted instanceof Array)) {
            throw errorTypes.BadRequestData.withDetail("The provided payload is not a JSON array")
        }

        const entities_expanded = []

        for (const entity_compacted of entities_compacted) {
            const nonNormalizedContext = (contextUrl != undefined) ? contextUrl : entity_compacted['@context']
            const actualContext = appendCoreContext(nonNormalizedContext)
            const context = await getNormalizedContext(actualContext)


            let entity_expanded = expandObject(entity_compacted, context)

            entity_expanded = util.unpackGeoPropertyStringValues(entity_expanded)

            entities_expanded.push(entity_expanded)
        }

        //############### BEGIN Validate input ###############
        const checkResult = checkArrayOfEntities(entities_expanded, true, true)

        if (checkResult.length > 0) {
            throw errorTypes.BadRequestData.withDetail(checkResult.join("\n"))
        }
        //############### END Validate input ###############


        const result = new BatchOperationResult()

        //######## BEGIN Iterate over list of uploaded entities and try to update them ########
        for (const entity_expanded of entities_expanded) {

            // NOTE: We need to stringify the entity here to pass it to appendEntityAttributes():
            const entityJsonString = JSON.stringify(entity_expanded)

            // TODO: 2 Really call another top-level API method here?
            const appendResult = await this.api_5_6_3_appendEntityAttributes(entity_expanded['@id'], entityJsonString, contextUrl, true).catch((problem) => {
                result.errors.push(new BatchEntityError(entity_expanded['@id'], problem))
            })

            if (appendResult) {
                result.success.push(entity_expanded['@id'])
            }

        }
        //######## END Iterate over list of uploaded entities and try to update them ########

        return result
    }


    // Spec 5.6.10
    async api_5_6_10_batchEntityDelete(jsonString: string): Promise<BatchOperationResult> {

        const entityIds = parseJson(jsonString)

        //############### BEGIN Validate input ###############

        if (!(entityIds instanceof Array)) {
            throw errorTypes.BadRequestData.withDetail("The provided payload is not a JSON array")
        }

        const checkResult = checkArrayOfUris(entityIds, true)

        if (checkResult.length > 0) {
            throw errorTypes.BadRequestData.withDetail(checkResult.join("\n"))
        }
        //############### END Validate input ###############


        const result = new BatchOperationResult()


        //################ BEGIN Iterate over entity IDs and delete the respective entities ###############
        for (const id of entityIds) {
            const deleteResult = await this.deleteEntity(id).catch((e) => {
                result.errors.push(new BatchEntityError(id, new ProblemDetails("", "Failed to delete entity.", "No entity with the provided ID exists.", 404)))
            })

            if (deleteResult == true) {
                result.success.push(id)
            }
        }
        //################ END Iterate over entity IDs and delete the respective entities ###############

        return result
    }


    // Spec 5.6.11
    async api_5_6_11_createOrUpdateTemporalEntity(temporalEntityString: string, contextUrl: string | undefined): Promise<number> {

        const entity_compacted = parseJson(temporalEntityString)

        const nonNormalizedContext = (contextUrl != undefined) ? contextUrl : entity_compacted['@context']
        const actualContext = appendCoreContext(nonNormalizedContext)
        const context = await getNormalizedContext(actualContext)

        let entity_expanded = expandObject(entity_compacted, context)


        entity_expanded = util.unpackGeoPropertyStringValues(entity_expanded)

        //################# BEGIN Validate input ##################
        const entityCheckResults = checkEntity(entity_expanded, false)

        if (entityCheckResults.length > 0) {
            throw errorTypes.InvalidRequest.withDetail("The submitted data is not a valid NGSI-LD temporal entity: " + entityCheckResults.join(" "))
        }
        //################# END Validate input ##################

        // NOTE: This currently returns a HTTP status code. This is perhaps not ideal.


        // NOTE: We should probably not merge this with createEntity because the behaviours of both methods are different:
        // The temporal version supports updates of an existing entity with the same request while the non-temporal version
        // doesn't.

        const entityMetadata = await this.getEntityMetadata(entity_expanded['@id'])

        // If the entity doesn't exist yet, create it:
        if (entityMetadata == undefined) {

            await this.createEntity(entity_expanded, true)

            return 201
        }
        else {

            // If the entity already exists:

            // 5.6.4.11: 

            // "If the NGSI-LD endpoint already knows about this Temporal Representation of an Entity, 
            // because there is an existing Temporal Representation of an Entity whose id (URI) is the same, 
            // then all the Attribute instances included by the Temporal Representation shall be added to 
            // the existing Entity as mandated by clause 5.6.12.":

            // NOTE: If "temporal" (last parameter) is true, then "overwrite" (second-last parameter)
            // has no effect. We set it to true, but setting it to false wouldn't change the result.
            // In temporal mode, attribute instances are always appended and never overwritten.
            await this.writeTemporalAttributes(entityMetadata.id, entity_expanded)

            return 204
        }
    }


    // Spec 5.6.12
    async api_5_6_12_addAttributesToTemporalEntity(entityId: string, fragmentString: string, contextUrl: string | undefined) {

        const fragment_compacted = parseJson(fragmentString)

        const nonNormalizedContext = (contextUrl != undefined) ? contextUrl : fragment_compacted['@context']
        const actualContext = appendCoreContext(nonNormalizedContext)
        const context = await getNormalizedContext(actualContext)

        let fragment_expanded = expandObject(fragment_compacted, context)


        fragment_expanded = util.unpackGeoPropertyStringValues(fragment_expanded)


        const entityCheckResults = checkEntity(fragment_expanded, false)

        if (entityCheckResults.length > 0) {
            throw errorTypes.InvalidRequest.withDetail("The submitted data is not a valid NGSI-LD entity: " + entityCheckResults.join(" "))
        }


        //###################### BEGIN Try to fetch existing entity ########################
        const entityMetadata = await this.getEntityMetadata(entityId)

        if (!entityMetadata) {
            throw errorTypes.ResourceNotFound.withDetail("No entity with the passed ID exists: " + entityId)
        }
        //###################### END Try to fetch existing entity ########################


        await this.writeTemporalAttributes(entityMetadata.id, fragment_expanded)
    }


    // Spec 5.6.13
    async api_5_6_13_deleteAttributeFromTemporalEntity(entityId: string, attributeId_compacted: string, datasetId_compacted: string | undefined, contextUrl: string | undefined, deleteAll: boolean) {

        //################## BEGIN Determine actual datasetId to use ################
        let useDatasetId_compacted: string | null | undefined = datasetId_compacted

        // If datasetId is undefined, but 'deleteAll' is not set, this means that the default instance
        // should be deleted, which is characterized by having datasetId = null:

        if (datasetId_compacted == undefined) {
            useDatasetId_compacted = null
        }

        if (deleteAll) {
            useDatasetId_compacted = undefined
        }
        //################## END Determine actual datasetId to use ################


        await this.deleteAttribute(entityId, attributeId_compacted, useDatasetId_compacted, undefined, true, contextUrl)
    }


    // Spec 5.6.14
    async api_5_6_14_updateAttributeInstanceOfTemporalEntity(entityId: string, attributeId_compacted: string,
        instanceId_compacted: string, fragmentString_compacted: string, contextUrl: string | undefined) {


        const fragment_compacted = parseJson(fragmentString_compacted)

        const nonNormalizedContext = (contextUrl != undefined) ? contextUrl : fragment_compacted['@context']
        const actualContext = appendCoreContext(nonNormalizedContext)
        const context = await getNormalizedContext(actualContext)

        const fragment_expanded = expandObject(fragment_compacted, context)

        const attributeId_expanded = expandObject(attributeId_compacted, context)
        const instanceId_expanded = expandObject(instanceId_compacted, context)

        //########################### BEGIN Generic NGSI-LD Input validation #########################
        if (!isUri(entityId)) {
            throw errorTypes.BadRequestData.withDetail(`'${entityId}' is not a valid URI.`)
        }

        if (!isUri(attributeId_expanded)) {
            throw errorTypes.BadRequestData.withDetail(`'${attributeId_expanded}' is not a valid URI.`)
        }

        if (!isUri(instanceId_compacted)) {
            throw errorTypes.BadRequestData.withDetail(`'${instanceId_compacted}' is not a valid URI.`)
        }

        const entityCheckResults = checkEntity(fragment_expanded, false)

        if (entityCheckResults.length > 0) {
            throw errorTypes.InvalidRequest.withDetail("The submitted data is not a valid NGSI-LD entity fragment: " + entityCheckResults.join(" "))
        }
        //########################### END Generic NGSI-LD Input validation #########################


        //########################### BEGIN Use-Case-specific NGSI-LD Input validation #########################

        let attribute_expanded = fragment_expanded[attributeId_expanded]

        if (attribute_expanded == undefined) {
            throw errorTypes.BadRequestData.withDetail("Provided entity fragment does not contain an attribute with the id " + attributeId_expanded)
        }

        if (!(attribute_expanded instanceof Array)) {
            throw errorTypes.ResourceNotFound.withDetail(`The attribute to patch ("${attributeId_expanded}") is not an array in the provided entity fragment`)
        }

        if (attribute_expanded.length != 1) {
            throw errorTypes.BadRequestData.withDetail(`The attribute to patch ("${attributeId_expanded}") does not have the expected amount of exactly 1 attribute instances in the submitted NGSI-LD fragment`)
        }
        //########################### END Use-Case-specific NGSI-LD Input validation #########################

        const instance_expanded = attribute_expanded[0]


        //####################### BEGIN Try to fetch existing entity ###########################
        const entityMetadata = await this.getEntityMetadata(entityId)

        if (entityMetadata == undefined) {
            throw errorTypes.ResourceNotFound.withDetail(`No entity with ID '${entityId}' exists.`)
        }
        //####################### END Try to fetch existing entity ###########################

        const instanceId_number = parseInt(instanceId_expanded.split("_")[1])



        const queryBuilder = this.makeQueryBuilder(instance_expanded)

        let sql_update = "BEGIN;"


        //################ BEGIN Update temporal table ################
        sql_update += queryBuilder.getUpdateQueryForTable(tableCfg.TBL_ATTR_TEMPORAL)

        // Add WHERE conditions:    

        sql_update += ` WHERE ${tableCfg.COL_INSTANCE_ID} = ${instanceId_number}`

        // Don't allow attribute type change:
        // ATTENTION: COL_ATTR_TYPE is of type smallint, so no quotes around the value here!
        sql_update += ` AND ${tableCfg.COL_ATTR_TYPE} = ${this.attributeTypes.indexOf(instance_expanded['@type'])}`
        sql_update += ';'
        //################ END Update temporal table ################


        /*
        //################ BEGIN Update last created table ################
        sql_update += queryBuilder.getUpdateQueryForTable(tableCfg.TBL_ATTR_LATEST)

        // Add WHERE conditions:    

        sql_update += ` WHERE ${tableCfg.COL_INSTANCE_ID} = ${instanceId_number}`

        // Don't allow attribute type change:
        // ATTENTION: COL_ATTR_TYPE is of type smallint, so no quotes around the value here!
        sql_update += ` AND ${tableCfg.COL_ATTR_TYPE} = ${this.attributeTypes.indexOf(instance_expanded['@type'])}`
        sql_update += ';'
        //################ END Update last created table ################

        // TODO: Error message if attribute type change was attempted?
        */


        sql_update += this.makeUpdateEntityModifiedAtQuery(entityMetadata.id)
        sql_update += "COMMIT;"

        await this.runSqlQuery(sql_update)
    }


    // Spec 5.6.15
    async api_5_6_15_deleteAttributeInstanceOfTemporalEntity(entityId: string, attributeId_compacted: string, instanceId_compacted: string, contextUrl: string | undefined) {

        await this.deleteAttribute(entityId, attributeId_compacted, undefined, instanceId_compacted, true, contextUrl)
    }


    // Spec 5.6.16
    async api_5_6_16_deleteTemporalEntity(entityId: string) {

        if (!isUri(entityId)) {
            throw errorTypes.BadRequestData.withDetail("Passed Entity ID is not a valid URI: " + entityId)
        }

        // TODO: 2 Catch SQL exceptions here instead of returning them
        const result = await this.deleteEntity(entityId).catch((e) => {
            throw errorTypes.ResourceNotFound.withDetail("No entity with the passed ID exists: " + entityId)
        })
    }



    // Spec 5.7.1
    async api_5_7_1_retrieveEntity(entityId: string,
        attrs_compacted: Array<string> | undefined,
        geometryProperty_compacted: string | undefined,
        datasetId: string | undefined,
        options: Array<string>,
        contextUrl: string | undefined): Promise<any | Feature> {

        const actualContext = appendCoreContext(contextUrl)
        const context = await getNormalizedContext(actualContext)

        const keyValues = options.includes("keyValues")
        const includeSysAttrs = options.includes("sysAttrs")


        const query = new Query([new EntityInfo(entityId, undefined, undefined)], attrs_compacted, undefined, undefined, undefined, undefined)
        const entities = await this.queryEntities(query, false, includeSysAttrs, context)

        if (entities.length == 0) {
            throw errorTypes.ResourceNotFound.withDetail("No entity found.")
        }
        else if (entities.length > 1) {
            throw errorTypes.InternalError.withDetail("More than one entity with the same ID was found. This is a database corruption and should never happen.")
        }


        let result_expanded = entities[0]

        if (keyValues) {
            result_expanded = util.simplifyEntity(result_expanded)
        }

        // Return GeoJSON representation if requested:
        if (geometryProperty_compacted != undefined) {

            const geometryProperty_expanded = expandObject(geometryProperty_compacted, context)

            result_expanded = compactedEntityToGeoJsonFeature(result_expanded, geometryProperty_expanded, datasetId)
        }


        const result_compacted = compactObject(result_expanded, context)

        result_compacted['@context'] = actualContext

        return result_compacted
    }


    // Spec 5.7.2
    async api_5_7_2_queryEntities(query: Query, contextUrl: string | undefined, options: Array<string>): Promise<Array<any>> {

        let includeSysAttrs = options.includes("sysAttrs")
        let keyValues = options.includes("keyValues")


        const actualContext = appendCoreContext(contextUrl)

        const context = await getNormalizedContext(actualContext)


        // Fetch entities
        let entities_expanded = await this.queryEntities(query, false, includeSysAttrs, context)

        //#################### BEGIN Create simplified representation if requested ##################
        if (keyValues) {

            const entities_simplified = []

            for (const entity of entities_expanded) {
                entities_simplified.push(util.simplifyEntity(entity))
            }

            entities_expanded = entities_simplified
        }
        //#################### END Create simplified representation if requested ##################


        //############### BEGIN Compact the result #################
        const result = Array<any>()

        for (const entity_expanded of entities_expanded) {
            let entity_compacted = compactObject(entity_expanded, context)
            entity_compacted['@context'] = actualContext
            result.push(entity_compacted)
        }
        //############### END Compact the result #################

        // NOTE: The conversion to GeoJSON representation is implemented in the HTTPBinding class.

        return result
    }


    // Spec 5.7.3
    async api_5_7_3_retrieveTemporalEntity(
        entityId: string,
        attrs_compacted: Array<string> | undefined,
        // NOTE: The parameter "lastN" is part of TemporalQuery
        temporalQ: TemporalQuery | undefined,
        contextUrl: string | undefined,
        options: Array<string>) {

        const includeSysAttrs = options.includes("sysAttrs")

        // TODO: 3 Implement simplified representation (6.3.7, 6.3.12)

        //#################### BEGIN Validation ###################
        if (!isUri(entityId)) {
            throw errorTypes.BadRequestData.withDetail(`'${entityId}' is not a valid NGSI-LD entity ID.`)
        }
        //#################### END Validation ###################


        const actualContext = appendCoreContext(contextUrl)
        const context = await getNormalizedContext(actualContext)

        const query = new Query([new EntityInfo(entityId, undefined, undefined)], attrs_compacted, undefined, undefined, undefined, temporalQ)
        const entities = await this.queryEntities(query, true, includeSysAttrs, context)


        if (entities.length == 0) {
            throw errorTypes.ResourceNotFound.withDetail("No entity found.")
        }
        else if (entities.length > 1) {
            throw errorTypes.InternalError.withDetail("More than one entity with the same ID was found. This is a database corruption and should never happen.")
        }

        return compactObject(entities[0], context)
    }


    // Spec 5.7.4
    async api_5_7_4_queryTemporalEntities(query: Query, contextUrl: string | undefined, options: Array<string>) {

        const actualContext = appendCoreContext(contextUrl)
        const context = await getNormalizedContext(actualContext)

        //################ BEGIN Validation #################

        // TODO: Only for testing. A temporal query is required according to the spec.
        /*
        if (query.temporalQ == undefined) {
            throw errorTypes.BadRequestData.withDetail("No temporal query provided in request.")
        }

        if (query.temporalQ.timerel == undefined) {
            throw errorTypes.BadRequestData.withDetail("'timerel' is undefined")
        }

        if (query.temporalQ.timeAt == undefined) {
            throw errorTypes.BadRequestData.withDetail("'timeAt' is undefined")
        }
        */
        //################ END Validation ################# 



        let includeSysAttrs = options.includes("sysAttrs")



        // Fetch entities
        const entities_expanded = await this.queryEntities(query, true, includeSysAttrs, context)

        //############### BEGIN Compact the result #################
        const result = Array<any>()

        for (const entity_expanded of entities_expanded) {
            let entity_compacted = compactObject(entity_expanded, context)
            entity_compacted['@context'] = actualContext
            result.push(entity_compacted)
        }
        //############### END Compact the result #################

        return result
    }


    // Spec 5.7.5
    async api_5_7_5_retrieveAvailableEntityTypes() {

        const sql_select = `SELECT DISTINCT ${tableCfg.COL_ENT_TYPE} FROM ${tableCfg.TBL_ENT}`
        const queryResult = await this.runSqlQuery(sql_select)

        let result = new EntityTypeList()

        for (let row of queryResult.rows) {
            result.typeList.push(row[tableCfg.COL_ENT_TYPE])
        }

        return result
    }


    // Spec 5.7.6
    async api_5_7_6_retrieveAvailableEntityTypeDetails() {

        const sql_select = `SELECT DISTINCT ${tableCfg.COL_ENT_TYPE}, ${tableCfg.COL_ATTR_NAME} FROM ${tableCfg.TBL_ENT} AS t1, ${tableCfg.TBL_ATTR_LATEST} AS t2 WHERE t1.${tableCfg.COL_ENT_INTERNAL_ID} = t2.eid`

        const queryResult = await this.runSqlQuery(sql_select)

        const types = new Map<String, EntityType>()


        for (const row of queryResult.rows) {

            const typeName = row[tableCfg.COL_ENT_TYPE]
            const attrName = row[tableCfg.COL_ATTR_NAME]

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

        return result
    }


    // Spec 5.7.7
    async api_5_7_7_retrieveAvailableEntityTypeInformation(type: string) {


        // TODO: Read from temporal table or only from most recent attributes table?
        let sql_count = `SELECT COUNT(*) FROM ${tableCfg.TBL_ENT} WHERE ${tableCfg.COL_ENT_TYPE} = '${type}'`


        let sqlResult_count = await this.runSqlQuery(sql_count)


        const entityCount = sqlResult_count.rows[0].count

        const sql_select = `SELECT DISTINCT ${tableCfg.COL_ATTR_NAME} FROM ${tableCfg.TBL_ENT} as t1, ${tableCfg.TBL_ATTR_LATEST} as t2 WHERE t1.${tableCfg.COL_ENT_INTERNAL_ID} = t2.eid AND ${tableCfg.COL_ENT_TYPE} = '${type}'`

        const sqlResult = await this.runSqlQuery(sql_select)

        const result = new EntityTypeInfo(type, entityCount)

        for (const row of sqlResult.rows) {

            const attribute = await this.getAttributeInfo(row[tableCfg.COL_ATTR_NAME])

            result.attributeDetails.push(attribute)
        }

        return result
    }


    // Spec 5.7.8 
    async api_5_7_8_retrieveAvailableAttributes() {


        // TODO: 3 Ask: Should default attributes like "createdAt" be included here?

        const attrNames_expanded = new AttributeList()

        const sql_select = `SELECT DISTINCT ${tableCfg.COL_ATTR_NAME} FROM ${tableCfg.TBL_ATTR_LATEST}`

        const sqlResult = await this.runSqlQuery(sql_select)

        for (const row of sqlResult.rows) {
            attrNames_expanded.attributeList.push(row[tableCfg.COL_ATTR_NAME])
        }

        let result = Array<Attribute>()

        for (const attrName_expanded of attrNames_expanded.attributeList) {

            let attribute = await this.getAttributeInfo(attrName_expanded)

            result.push(attribute)
        }


        return result
    }


    // Spec 5.7.9
    async api_5_7_9_retrieveAvailableAttributeDetails() {

        // TODO: 3 Ask: Should default attributes like "createdAt" be included here?

        const result = new AttributeList()

        const sql_select = `SELECT DISTINCT ${tableCfg.COL_ATTR_NAME} FROM ${tableCfg.TBL_ATTR_LATEST}`

        const sqlResult = await this.runSqlQuery(sql_select)

        for (const row of sqlResult.rows) {
            result.attributeList.push(row[tableCfg.COL_ATTR_NAME])
        }


        return result
    }


    // Spec 5.7.10
    async api_5_7_10_retrieveAvailableAttributeInformation(attrType_compacted: string, contextUrl: string | undefined) {

        const actualContext = appendCoreContext(contextUrl)
        const context = await getNormalizedContext(actualContext)

        const attrType_expanded = expandObject(attrType_compacted, context)

        return await this.getAttributeInfo(attrType_expanded)
    }


    // TODO: 4 Implement 5.8 - 5.11

    //################################# END Official API methods ######################################



    //############################ BEGIN Inofficial API methods ############################
    async api_inofficial_deleteAllEntities() {
        let sql_t_delete_all = "BEGIN;"
        sql_t_delete_all += `DELETE FROM ${tableCfg.TBL_ENT};`
        sql_t_delete_all += `DELETE FROM ${tableCfg.TBL_ATTR_TEMPORAL};`
        sql_t_delete_all += `DELETE FROM ${tableCfg.TBL_ATTR_LATEST};`
        sql_t_delete_all += "COMMIT;"

        await this.runSqlQuery(sql_t_delete_all)
    }


    async api_inofficial_temporalEntityOperationsUpsert(jsonString: any, contextUrl: string | undefined) {

        // TODO: 3 "createOrUpdate" isn't really an upsert! 
        // It only *adds* attributes, it doesn't replace the entire entity!

        const entities_compacted = parseJson(jsonString)

        for (const ec of entities_compacted) {
            await this.api_5_6_11_createOrUpdateTemporalEntity(JSON.stringify(ec), contextUrl)
        }
    }
    //############################ END Inofficial API methods ############################



    private async deleteAttribute(entityId: string, attributeId_compacted: string, datasetId_compacted: string | null | undefined, instanceId: string | undefined, temporal: boolean, contextUrl: any) {

        const actualContext = appendCoreContext(contextUrl)
        const context = await getNormalizedContext(actualContext)

        const attributeId_expanded = expandObject(attributeId_compacted, context)
        const datasetId_expanded = expandObject(datasetId_compacted, context)


        //######################## BEGIN Input validation ##############################
        if (!isUri(entityId)) {
            throw errorTypes.BadRequestData.withDetail("Passed entity ID is not a valid URI.")
        }

        if (!isUri(attributeId_expanded)) {
            throw errorTypes.BadRequestData.withDetail("Passed attribute ID is not a valid URI.")
        }

        if (datasetId_expanded != undefined && datasetId_expanded != null && !isUri(datasetId_expanded)) {
            throw errorTypes.BadRequestData.withDetail("Passed dataset ID is not a valid URI.")
        }

        if (instanceId != undefined && !isUri(instanceId)) {
            throw errorTypes.BadRequestData.withDetail("Passed instance ID is not a valid URI.")
        }
        //######################## END Input validation ##############################


        //######## BEGIN Read target entity from database to get its internal ID, which is required for the delete call ##########
        const entityMetadata = await this.getEntityMetadata(entityId)

        if (!entityMetadata) {
            throw errorTypes.ResourceNotFound.withDetail("No entity with the passed ID exists: " + entityId)
        }

        const entityInternalId = entityMetadata.id
        //######## END Read target entity from database to get its internal ID, which is required for the delete call ##########


        let sql_t_delete = "BEGIN;"


        if (temporal) {

            //################# BEGIN Delete from temporal attributes table ##################
            sql_t_delete += `DELETE FROM ${tableCfg.TBL_ATTR_TEMPORAL} WHERE eid = ${entityInternalId} `

            // Match attribute ID:
            sql_t_delete += ` AND ${tableCfg.COL_ATTR_NAME} = '${attributeId_expanded}' `

            // Match instance ID:

            if (instanceId != undefined) {
                // NOTE: We assume that the attribute instances is passed in the form "urn:ngsi-ld:InstanceId:instance_<number>"
                const instanceId_number = parseInt(instanceId.split("_")[1])

                sql_t_delete += ` AND ${tableCfg.COL_INSTANCE_ID} = '${instanceId_number}'`
            }

            // Match dataset ID if provided:        
            sql_t_delete += this.makeSqlCondition_datasetId(datasetId_expanded)
            sql_t_delete += ";"
            //################# END Delete from temporal attributes table ##################

            console.log("DELETE TEMPORAL")
        }
        else {

            //################# BEGIN Delete from latest attributes table ##################
            sql_t_delete += `DELETE FROM ${tableCfg.TBL_ATTR_LATEST} WHERE eid = ${entityInternalId} `

            // Match attribute ID:
            sql_t_delete += ` AND ${tableCfg.COL_ATTR_NAME} = '${attributeId_expanded}' `

            // NOTE: We don't specify the instance ID here!

            // Match dataset ID if provided:        
            sql_t_delete += this.makeSqlCondition_datasetId(datasetId_expanded)
            sql_t_delete += ";"
            //################# END Delete from latest attributes table ##################
        }



        sql_t_delete += ";COMMIT;"

        const queryResult = await this.runSqlQuery(sql_t_delete)

        // TODO: 2 Row count does probably not work as expected here
        if (queryResult.rowCount == 0) {
            throw errorTypes.ResourceNotFound.withDetail(`Failed to delete attribute instance. No attribute instance with the following properties exists: Entity ID = '${entityId}', Attribute ID ='${attributeId_expanded}', Instance ID = '${instanceId}'.`)
        }
    }



    private async createEntity(entity_expanded: any, temporal: boolean): Promise<number> {

        //############## BEGIN Build INSERT query for entities table ###########
        const now = new Date()

        const queryBuilder = new SqlQueryBuilder()

        queryBuilder.add(tableCfg.COL_ENT_ID, entity_expanded['@id'])
        queryBuilder.add(tableCfg.COL_ENT_TYPE, entity_expanded['@type'])
        queryBuilder.add(tableCfg.COL_ENT_CREATED_AT, now.toISOString())
        queryBuilder.add(tableCfg.COL_ENT_MODIFIED_AT, now.toISOString())
        //############## END Build INSERT query for entities table ###########

        const sql_insert_ent = queryBuilder.getInsertQueryForTable(tableCfg.TBL_ENT, "id") + ";"

        const queryResult = await this.runSqlQuery(sql_insert_ent).catch((error: any) => { })

        if (queryResult == undefined) {
            return -1
        }

        const insertId = queryResult.rows[0].id

        await this.writeAttributes(insertId, entity_expanded, true, true)

        return 1
    }



    async deleteEntity(entityId: string): Promise<boolean> {

        // TODO: 2 Catch SQL exceptions here instead of returning them

        // SQL query to delete the entity's row from the entities table:
        // Note that this delete query returns the internal ID of the deleted entity.
        // The internal ID is then used to find and delete the entity's rows in the attributes table.
        const sql_delete_entity_metadata = `DELETE FROM ${tableCfg.TBL_ENT} WHERE ${tableCfg.COL_ENT_ID} = '${entityId}' RETURNING id`

        const queryResult1 = await this.runSqlQuery(sql_delete_entity_metadata)

        if (queryResult1.rows.length == 0) {
            return false
        }

        // NOTE: If everything is as expected, there should always be at most 1 row returned. 
        // Nevertheless, we use a for loop here, just to make sure.


        //############ BEGIN Build and run transaction query to delete all attribute rows ###########
        let sql_delete_attributes = "BEGIN;"

        // Add queries to delete all of the entity's attributes to the transaction:
        for (const row of queryResult1.rows) {
            sql_delete_attributes += `DELETE FROM ${tableCfg.TBL_ATTR_TEMPORAL} WHERE eid = ${row[tableCfg.COL_ENT_INTERNAL_ID]};`
            sql_delete_attributes += `DELETE FROM ${tableCfg.TBL_ATTR_LATEST} WHERE eid = ${row[tableCfg.COL_ENT_INTERNAL_ID]};`
        }

        sql_delete_attributes += "COMMIT;"

        // Run transaction query:        
        await this.runSqlQuery(sql_delete_attributes)
        //############ END Build and run transaction query to delete all attribute rows ###########

        return true
    }


    async getAttributeInfo(attributeId_expanded: string): Promise<Attribute> {


        const sql_select = `SELECT ${tableCfg.COL_ENT_TYPE}, ${tableCfg.COL_ATTR_TYPE} FROM ${tableCfg.TBL_ENT} as t1, ${tableCfg.TBL_ATTR_LATEST} as t2 WHERE t1.${tableCfg.COL_ENT_INTERNAL_ID} = t2.eid AND ${tableCfg.COL_ATTR_NAME} = '${attributeId_expanded}'`

        let sqlResult = await this.runSqlQuery(sql_select)

        let result = new Attribute(attributeId_expanded, attributeId_expanded, sqlResult.rows.length)


        for (const row of sqlResult.rows) {

            const attrInstanceType = row[tableCfg.COL_ATTR_TYPE]
            const entityType = row[tableCfg.COL_ENT_TYPE]

            if (!result.attributeTypes.includes(this.attributeTypes[attrInstanceType])) {
                result.attributeTypes.push(this.attributeTypes[attrInstanceType])
            }

            if (!result.typeNames.includes(entityType)) {
                result.typeNames.push(entityType)
            }
        }

        return result
    }


    async getEntityMetadata(entityId: string): Promise<any> {

        const sql_select = `SELECT * FROM ${tableCfg.TBL_ENT} WHERE ${tableCfg.COL_ENT_ID} = '${entityId}'`

        const sqlResult = await this.runSqlQuery(sql_select)

        // No entitiy with passed ID was found:
        if (sqlResult.rows.length == 0) {
            return undefined
        }

        // 1 Entity with passed ID was found:
        else if (sqlResult.rows.length == 1) {

            const row = sqlResult.rows[0]
            const metadata = { id: row[tableCfg.COL_ENT_INTERNAL_ID], type: row[tableCfg.COL_ENT_TYPE] }

            return metadata
        }

        // More than 1 Entity with passed ID was found. This should never happen:
        else if (sqlResult.rows.length > 1) {
            throw errorTypes.InternalError.withDetail(`getEntityMetadata(): More than one Entity with ID '${entityId}' found. This is an invalid database state and should never happen.`)
        }
    }


    makeSqlCondition_datasetId(datasetId: string | null | undefined): string {

        if (datasetId === null) {
            return ` AND ${tableCfg.COL_DATASET_ID} is null`
        }
        else if (datasetId === undefined) {
            // ATTENTION: Returning nothing here is correct
            return ""
        }
        else {
            return ` AND ${tableCfg.COL_DATASET_ID} = '${datasetId}'`
        }
    }


    cleanUpAttributeInstanceForWrite(instance_expanded: any): any {
        let cleanedInstance: any = {}

        if (instance_expanded["@type"] == "https://uri.etsi.org/ngsi-ld/Property" || instance_expanded["@type"] == "https://uri.etsi.org/ngsi-ld/GeoProperty") {
            cleanedInstance[uri_value] = instance_expanded[uri_value]
        }
        else if (instance_expanded["@type"] == "https://uri.etsi.org/ngsi-ld/Relationship") {
            cleanedInstance["https://uri.etsi.org/ngsi-ld/hasObject"] = instance_expanded["https://uri.etsi.org/ngsi-ld/hasObject"]
        }

        if (typeof instance_expanded["https://uri.etsi.org/ngsi-ld/unitCode"] == "string") {
            cleanedInstance["https://uri.etsi.org/ngsi-ld/unitCode"] = instance_expanded["https://uri.etsi.org/ngsi-ld/unitCode"]
        }

        for (const key in instance_expanded) {
            if (isReifiedAttribute(instance_expanded[key], key)) {
                cleanedInstance[key] = instance_expanded[key]
            }
        }

        return cleanedInstance
    }


    makeQueryBuilder(instance_expanded: any): SqlQueryBuilder {

        const queryBuilder = new SqlQueryBuilder()

        // Write 'dataset_id' column:    
        let datasetId_sql = instance_expanded[uri_datasetId] === undefined ? null : instance_expanded[uri_datasetId]
        queryBuilder.add(tableCfg.COL_DATASET_ID, datasetId_sql)

        // Write JSON:
        const cleanedInstance = this.cleanUpAttributeInstanceForWrite(instance_expanded)

        let cleanedInstanceString = JSON.stringify(cleanedInstance)

        // Escape single quotes:
        cleanedInstanceString = cleanedInstanceString.replace(/'/g, "''")

        
        queryBuilder.add(tableCfg.COL_INSTANCE_JSON, cleanedInstanceString)

        // ############### BEGIN Write 'geom' column ################
        if (instance_expanded['@type'] == "https://uri.etsi.org/ngsi-ld/GeoProperty") {

            // ATTENTION: 
            // Since property values are not expanded, we don't need to re-compact the GeoJSON object here:

            const geojson_string = JSON.stringify(instance_expanded['https://uri.etsi.org/ngsi-ld/hasValue'])
            queryBuilder.add("geom", `ST_SetSRID(ST_GeomFromGeoJSON('${geojson_string}'), 4326)`, true)
        }
        // ############### END Write 'geom' column ################


        //################# BEGIN Write attribute type ################
        const attributeTypeIndex = this.attributeTypes.indexOf(instance_expanded['@type'])

        if (attributeTypeIndex < 0) {
            throw errorTypes.InternalError.withDetail("Invalid attribute type: " + instance_expanded['@type'])
        }

        queryBuilder.add(tableCfg.COL_ATTR_TYPE, attributeTypeIndex)
        //################# END Write attribute type ################

        // Write 'observed_at' column:
        if (isDateTimeUtcString(instance_expanded["https://uri.etsi.org/ngsi-ld/observedAt"])) {
            queryBuilder.add(tableCfg.COL_ATTR_OBSERVED_AT, instance_expanded["https://uri.etsi.org/ngsi-ld/observedAt"])
        }

        return queryBuilder
    }


    makeUpdateEntityModifiedAtQuery(entityInternalId: number): string {
        const now = new Date()

        return `UPDATE ${tableCfg.TBL_ENT} SET ${tableCfg.COL_ENT_MODIFIED_AT} = '${now.toISOString()}' WHERE ${tableCfg.COL_ENT_INTERNAL_ID} = ${entityInternalId};`
    }


    // Spec 5.7.2
    async queryEntities(query: Query, temporal: boolean, includeSysAttrs: boolean, context: JsonLdContextNormalized): Promise<Array<any>> {

        const attr_table = temporal ? tableCfg.TBL_ATTR_TEMPORAL : tableCfg.TBL_ATTR_LATEST


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

        // TODO 4: "If the list of Entity identifiers includes a URI which it is not valid, 
        // or the query, geo-query or context source filter are not syntactically valid 
        // (as per the referred clauses 4.9 and 4.10) an error of type BadRequestData
        // shall be raised.

        //############################# END Validation #########################


        let sql_where = ""

        //################ BEGIN Build entity IDs and types filter expression from EntityInfo array ##################
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
            sql_where += ` AND t1.${tableCfg.COL_ENT_TYPE} IN ('${entityTypes_expanded.join("','")}')`
        }

        if (entityIds.length > 0) {
            sql_where += ` AND t1.${tableCfg.COL_ENT_ID} IN ('${entityIds.join("','")}')`
        }

        // TODO: 3 ADD FEATURE - "id matches the id patterns passed as parameter"

        //############### END Build entity IDs and types filter expression from EntityInfo array ##################



        //####################### BEGIN Match specified Attributes #######################
        // - "attribute matches any of the expanded attribute(s) in the list that is passed as parameter":

        // NOTE: The addition of this condition also automatically covers spec 5.7.2.6: 
        // "For each matching Entity only the Attributes specified by the Attribute list 
        // parameter shall be included."


        if (query.attrs instanceof Array && query.attrs.length > 0) {

            const attrs_expanded = expandObject(query.attrs, context)

            sql_where += ` AND t2.${tableCfg.COL_ATTR_NAME} IN ('${attrs_expanded.join("','")}')`
        }
        //####################### END Match specified Attributes #######################



        //#################### BEGIN Match NGSI-LD query #################

        // - "the filter conditions specified by the query are met (as mandated by clause 4.9)":
        if (query.q != undefined) {

            const ngsi_query_sql = this.ngsiQueryParser.makeQuerySql(query, context, attr_table)

            sql_where += ` AND t1.${tableCfg.COL_ENT_INTERNAL_ID} IN ${ngsi_query_sql}`
        }
        //#################### END Match NGSI-LD query #################


        //####################### BEGIN Match GeoQuery #######################

        // - "the geospatial restrictions imposed by the geoquery are met (as mandated by clause 4.10).
        // if there are multiple instances of the GeoProperty on which the geoquery is based, 
        // it is sufficient if any of these instances meets the geospatial restrictions":

        if (query.geoQ != undefined) {
            sql_where += ` AND t1.${tableCfg.COL_ENT_INTERNAL_ID} IN ${makeGeoQueryCondition(query.geoQ, context, tableCfg, attr_table)}`
        }
        //####################### END Match GeoQuery #######################

        // TODO: 2 - "the entity is available at the Context Source(s) that match the context source filter conditions."




        // TODO: 4 "Pagination logic shall be in place as mandated by clause 5.5.9."

        // TODO: 4 All other things in 5.7.2.4 that are still missing


        //################### BEGIN Match temporal query ######################
        let orderBySql = undefined
        let lastN = undefined

        if (query.temporalQ != undefined) {

            sql_where += makeTemporalQueryCondition(query.temporalQ, tableCfg)

            let ttc = undefined

            if ((query.temporalQ.timeproperty in temporalFields)) {
                ttc = temporalFields[query.temporalQ.timeproperty]
            }

            orderBySql = " ORDER BY " + ttc + " DESC"
            lastN = query.temporalQ.lastN
        }
        //################### END Match temporal query ######################

        const fields = Array<string>()

        fields.push(tableCfg.COL_ENT_INTERNAL_ID)
        fields.push(tableCfg.COL_ENT_TYPE)
        fields.push(tableCfg.COL_ENT_ID)
        fields.push(tableCfg.COL_ATTR_NAME)
        fields.push(tableCfg.COL_ATTR_EID)
        fields.push(tableCfg.COL_INSTANCE_ID)
        fields.push(tableCfg.COL_DATASET_ID)
        fields.push(tableCfg.COL_INSTANCE_JSON)
        fields.push(`${tableCfg.COL_ENT_CREATED_AT} at time zone 'utc' as ent_created_at`)
        fields.push(`${tableCfg.COL_ENT_MODIFIED_AT} at time zone 'utc' as ent_modified_at`)
        fields.push(`${tableCfg.COL_ATTR_CREATED_AT} at time zone 'utc' as attr_created_at`)
        fields.push(`${tableCfg.COL_ATTR_MODIFIED_AT} at time zone 'utc' as attr_modified_at`)
        fields.push(`${tableCfg.COL_ATTR_OBSERVED_AT} at time zone 'utc' as attr_observed_at`)

        let sql_select = `SELECT ${fields.join(',')} FROM ${tableCfg.TBL_ENT} AS t1, ${attr_table} AS t2 WHERE t1.${tableCfg.COL_ENT_INTERNAL_ID} = t2.eid ${sql_where}`

        // If lastN is defined, wrap limiting query around the original query:
        // See https://stackoverflow.com/questions/1124603/grouped-limit-in-postgresql-show-the-first-n-rows-for-each-group

        if (temporal && typeof (lastN) == "number" && lastN > 0) {
            sql_select = `SELECT * FROM (SELECT ROW_NUMBER() OVER (PARTITION BY ent_id, attr_name ${orderBySql}) AS r, t.* FROM (${sql_select}) t) x WHERE x.r <= ${lastN};`
        }




        const queryResult = await this.runSqlQuery(sql_select)


        const entitiesByNgsiId: any = {}

        //#################### BEGIN Iterate over returned attribute instance rows ####################
        for (const row of queryResult.rows) {

            const ent_id = row[tableCfg.COL_ENT_ID]
            const attr_name = row[tableCfg.COL_ATTR_NAME]

            //############## BEGIN Get or create Entity in memory #############
            let entity = entitiesByNgsiId[ent_id]

            if (!entity) {

                entity = {
                    "@id": ent_id,
                    "@type": row[tableCfg.COL_ENT_TYPE]
                }

                if (includeSysAttrs) {
                    entity[uri_createdAt] = row[tableCfg.COL_ENT_CREATED_AT]
                    entity[uri_modifiedAt] = row[tableCfg.COL_ENT_MODIFIED_AT]
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

            const instance = row[tableCfg.COL_INSTANCE_JSON]


            //############### BEGIN Restore JSON fields that have their own database column ##############

            // TODO: 4 Add method to create instance ID string from number

            // ATTENTION: The returned instance ID value string MUST contain an "_" (underscore) because we
            // use it in PsqlBackend::deleteAttribute() as a string separator character to extract the
            // actual instance id number from a passed instance id string.

            if (temporal) {
                instance[uri_instanceId] = "urn:ngsi-ld:InstanceId:instance_" + row[tableCfg.COL_INSTANCE_ID]
            }

            // ATTENTION: We always add the modified timestamp first, regardless of whether includeSysAttrs is true,
            // because we need it to find the most recently modified attribute instance if this is not a
            // temporal API query:

            instance["@type"] = this.attributeTypes[row["attr_type"]]

            if (row["dataset_id"] != null) {
                instance[uri_datasetId] = row["dataset_id"]
            }


            if (row["attr_observed_at"] != null) {
                instance["https://uri.etsi.org/ngsi-ld/observedAt"] = row["attr_observed_at"]
            }

            if (includeSysAttrs) {
                instance[uri_createdAt] = row["attr_created_at"]
                instance[uri_modifiedAt] = row["attr_modified_at"]
            }
            //############ END Restore JSON fields that have their own database column ##############

            attribute.push(instance)
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

        const attrNames_expanded = expandObject(query.attrs, context) as Array<string>

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

        //#################### BEGIN Order attributes alphabetically ##############
        let result_ordered = []

        for (const entity of result) {
            const ordered = Object.keys(entity).sort().reduce(
                (obj: any, key: string) => {
                    obj[key] = entity[key];
                    return obj;
                },
                {}
            );

            result_ordered.push(ordered)
        }
        //#################### END Order attributes alphabetically ##############

        return result_ordered
    }



    private async runSqlQuery(sql: string): Promise<pg.QueryResult> {

        const resultPromise = this.pool.query(sql)

        /*
        console.log("---")
        console.log(sql)
        console.log("---")
        */
        // Print error, but still continue with the normal promise chain:

        resultPromise.then(null, (e) => {

            console.log("######################## SOMETHING WENT WRONG ########################")
            console.log()
            console.log("SQL:")
            console.log()
            console.log(sql)
            console.log()
            console.log("ERROR:")
            console.log()
            console.log(e)
            console.log()
            console.log("#######################################################################")
        })

        return resultPromise
    }



    private async writeAttributes(entityInternalId: number, fragment_expanded: any, overwrite: boolean, append: boolean) {

        const now = new Date()

        const result = new UpdateResult()

        //###################### BEGIN Get existing attribute instances #####################
        let sql_getExistingInstances = `SELECT * FROM ${tableCfg.TBL_ATTR_LATEST} WHERE eid = ${entityInternalId} `

        const sqlResultTemporal = await this.runSqlQuery(sql_getExistingInstances)

        const existingInstancesOfAllAttributes = sqlResultTemporal.rows
        //###################### END Get existing attribute instances #####################


        let sql_t_append_or_update = "BEGIN;"

        //####################### BEGIN Iterate over attributes #############################
        for (const attributeId_expanded in fragment_expanded) {

            const existingAttributeInstances = []

            for (const inst of existingInstancesOfAllAttributes) {
                if (inst["attr_name"] == attributeId_expanded) {
                    existingAttributeInstances.push(inst)
                }
            }

            // Do not process @id, @type and @context:
            if (ignoreAttributes.includes(attributeId_expanded)) {
                continue
            }

            let attribute_expanded = (fragment_expanded as any)[attributeId_expanded]

            if (!(attribute_expanded instanceof Array)) {
                attribute_expanded = [attribute_expanded]
            }

            let attributeUpdated = false

            //#################### BEGIN Iterate over attribute instances #####################
            for (const instance_expanded of attribute_expanded) {

                const datasetId_expanded = instance_expanded[uri_datasetId]
                const datasetId_sql = (datasetId_expanded === undefined) ? null : datasetId_expanded

                const existingInstancesWithSameDatasetId = []

                for (const inst of existingAttributeInstances) {
                    if (inst["dataset_id"] == datasetId_sql) {
                        existingInstancesWithSameDatasetId.push(inst)
                    }
                }


                let instanceUpdated = false

                //################## BEGIN Build upsert query for latest attributes table #####################
                const queryBuilder = this.makeQueryBuilder(instance_expanded)

                queryBuilder.add(tableCfg.COL_ATTR_MODIFIED_AT, now.toISOString())

                if (existingInstancesWithSameDatasetId.length == 0 && append) {

                    queryBuilder.add(tableCfg.COL_ATTR_EID, entityInternalId, true)
                    queryBuilder.add(tableCfg.COL_ATTR_NAME, attributeId_expanded)
                    queryBuilder.add(tableCfg.COL_ATTR_CREATED_AT, now.toISOString())

                    sql_t_append_or_update += queryBuilder.getInsertQueryForTable(tableCfg.TBL_ATTR_LATEST) + ";"

                    instanceUpdated = true

                }
                else if (existingInstancesWithSameDatasetId.length == 1 && overwrite) {

                    //if (overwrite && JSON.stringify(existingInstancesWithSameDatasetId[0]["json"]) != JSON.stringify(this.cleanUpAttributeInstanceForWrite(instance_expanded))) {                        

                    sql_t_append_or_update += queryBuilder.getUpdateQueryForTable(tableCfg.TBL_ATTR_LATEST)

                    sql_t_append_or_update += ` WHERE ${tableCfg.COL_ATTR_EID} = ${entityInternalId} AND ${tableCfg.COL_ATTR_NAME} = '${attributeId_expanded}' `
                    sql_t_append_or_update += this.makeSqlCondition_datasetId(existingInstancesWithSameDatasetId[0]["dataset_id"])
                    sql_t_append_or_update += ";"

                    instanceUpdated = true
                }
                else if (existingInstancesWithSameDatasetId.length > 1) {
                    throw errorTypes.InternalError.withDetail("Multiple instances with same datasetId")
                }
                //################## END Build upsert query for latest attributes table #####################


                if (instanceUpdated) {
                    attributeUpdated = true

                    //################ BEGIN Update temporal table ##############
                    const queryBuilder2 = this.makeQueryBuilder(instance_expanded)

                    queryBuilder2.add(tableCfg.COL_ATTR_EID, entityInternalId, true)
                    queryBuilder2.add(tableCfg.COL_ATTR_NAME, attributeId_expanded)
                    queryBuilder2.add(tableCfg.COL_ATTR_CREATED_AT, now.toISOString())
                    queryBuilder2.add(tableCfg.COL_ATTR_MODIFIED_AT, now.toISOString())

                    sql_t_append_or_update += queryBuilder2.getInsertQueryForTable(tableCfg.TBL_ATTR_TEMPORAL) + ";"
                    //################ END Update temporal table ##############
                }
            }
            //################## END Iterate over attribute instances #######################

            if (attributeUpdated) {
                result.updated.push(attributeId_expanded)
            }
            else {
                result.notUpdated.push(new NotUpdatedDetails(attributeId_expanded, "Attribute was not updated."))
            }
        }
        //####################### END Iterate over attributes #############################

        if (result.updated.length > 0) {
            sql_t_append_or_update += this.makeUpdateEntityModifiedAtQuery(entityInternalId)
            sql_t_append_or_update += "COMMIT;"

            await this.runSqlQuery(sql_t_append_or_update)
        }

        return result
    }



    private async writeTemporalAttributes(entityInternalId: number, fragment_expanded: any) {


        const now = new Date()

        const result = new UpdateResult()


        let sql_t_append_or_update = "BEGIN;"

        //####################### BEGIN Iterate over attributes #############################
        for (const attributeId_expanded in fragment_expanded) {

            // Do not process @id, @type and @context:
            if (ignoreAttributes.includes(attributeId_expanded)) {
                continue
            }



            let attribute_expanded = (fragment_expanded as any)[attributeId_expanded]

            if (!(attribute_expanded instanceof Array)) {
                attribute_expanded = [attribute_expanded]
            }



            //#################### BEGIN Iterate over attribute instances #####################
            for (const instance_expanded of attribute_expanded) {



                //################ BEGIN Update temporal table ##############
                const queryBuilder2 = this.makeQueryBuilder(instance_expanded)

                queryBuilder2.add(tableCfg.COL_ATTR_EID, entityInternalId, true)
                queryBuilder2.add(tableCfg.COL_ATTR_NAME, attributeId_expanded)
                queryBuilder2.add(tableCfg.COL_ATTR_CREATED_AT, now.toISOString())
                queryBuilder2.add(tableCfg.COL_ATTR_MODIFIED_AT, now.toISOString())

                sql_t_append_or_update += queryBuilder2.getInsertQueryForTable(tableCfg.TBL_ATTR_TEMPORAL) + ";"
                //################ END Update temporal table ##############

            }
            //################## END Iterate over attribute instances #######################


        }
        //####################### END Iterate over attributes #############################


        sql_t_append_or_update += this.makeUpdateEntityModifiedAtQuery(entityInternalId)
        sql_t_append_or_update += "COMMIT;"

        await this.runSqlQuery(sql_t_append_or_update)


        return result
    }
}