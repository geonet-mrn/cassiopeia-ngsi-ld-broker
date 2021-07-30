import { BatchEntityError } from "./dataTypes/BatchEntityError"
import { BatchOperationResult } from "./dataTypes/BatchOperationResult"
import { Feature } from "./dataTypes/Feature"
import { FeatureCollection } from "./dataTypes/FeatureCollection"
import { ProblemDetails } from "./dataTypes/ProblemDetails"
import { Query } from "./dataTypes/Query"
import { TemporalQuery } from "./dataTypes/TemporalQuery"
import { UpdateResult } from "./dataTypes/UpdateResult"
import { errorTypes } from "./errorTypes"
import { PsqlBackend } from "./psqlBackend/PsqlBackend"
import { checkArrayOfEntities, checkArrayOfUris, checkReifiedAttribute, checkEntity, isUri } from "./validate"
import { appendCoreContext, compactObject, expandObject, getNormalizedContext } from "./jsonld"
import { parseJson, compactedEntityToGeoJsonFeature as compactedEntityToGeoJsonFeature } from "./util"
import * as util from './util'
import { POINT_CONVERSION_COMPRESSED } from "constants"


export class ContextBroker {

    constructor(private readonly psql: PsqlBackend) { }


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

        const entity_expanded = await expandObject(entity_from_payload, context)

        const entityCheckResults = checkEntity(entity_expanded, true)

        if (entityCheckResults.length > 0) {
            throw errorTypes.InvalidRequest.withDetail("The submitted data is not a valid NGSI-LD entity: " + entityCheckResults.join(" "))
        }


        const resultCode = await this.psql.createEntity(entity_expanded, false)

        if (resultCode == -1) {             
            throw errorTypes.AlreadyExists.withDetail(`An Entity with the ID '${entity_expanded['@id']}' already exists.`)
        }

    }


    // Spec 5.6.2
    async api_5_6_2_updateEntityAttributes(entityId: string, fragmentString: string, contextUrl: string | undefined): Promise<UpdateResult> {

        //################### BEGIN Validation & Preparation ################

        const fragment_compacted = parseJson(fragmentString)

        const nonNormalizedContext = (contextUrl != undefined) ? contextUrl : fragment_compacted['@context']
        const actualContext = appendCoreContext(nonNormalizedContext)
        const context = await getNormalizedContext(actualContext)


        const fragment_expanded = expandObject(fragment_compacted, context)

        const entityCheckResults = checkEntity(fragment_expanded, true)

        if (entityCheckResults.length > 0) {
            throw errorTypes.InvalidRequest.withDetail("The submitted data is not a valid NGSI-LD entity: " + entityCheckResults.join(" "))
        }

        return await this.psql.updateEntityAttributes(entityId, fragment_expanded)
    }


    // Spec 5.6.3
    async api_5_6_3_appendEntityAttributes(entityId: string, fragmentJsonString: string, contextUrl: string | undefined, overwrite: boolean): Promise<UpdateResult> {

        const fragment_compacted = parseJson(fragmentJsonString)

        const nonNormalizedContext = (contextUrl != undefined) ? contextUrl : fragment_compacted['@context']
        const actualContext = appendCoreContext(nonNormalizedContext)
        const context = await getNormalizedContext(actualContext)

        const fragment_expanded = expandObject(fragment_compacted, context)



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


        return await this.psql.appendEntityAttributes(entityId, fragment_expanded, overwrite)

    }


    // Spec 5.6.4
    async api_5_6_4_partialAttributeUpdate(entityId: string, attributeId_compacted: string, fragmentJsonString: string, contextUrl: string | undefined) {

        const fragment_compacted = parseJson(fragmentJsonString)

        const nonNormalizedContext = (contextUrl != undefined) ? contextUrl : fragment_compacted['@context']

        const actualContext = appendCoreContext(nonNormalizedContext)
        const context = await getNormalizedContext(actualContext)

        const fragment_expanded = expandObject(fragment_compacted, context)
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

        // Convert attribute to array representation if it isn't yet:
        if (!(fragment_attribute_expanded instanceof Array)) {
            fragment_attribute_expanded = [fragment_attribute_expanded]
        }

        const attributeCheckResults = checkReifiedAttribute(fragment_attribute_expanded, attributeId_expanded, undefined, true)

        if (attributeCheckResults.length != 0) {
            throw errorTypes.BadRequestData.withDetail(`The field '${attributeId_expanded}' in the uploaded entity fragment is not a valid NGSI-LD attribute: ${attributeCheckResults.join("\n")}`)
        }
        //################### END Input validation ##################

        await this.psql.partialAttributeUpdate(entityId, attributeId_expanded, fragment_attribute_expanded)

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


        await this.deleteAttribute(entityId, false, attributeId_compacted, useDatasetId_compacted, undefined, contextUrl)
    }


    // Spec 5.6.6
    async api_5_6_6_deleteEntity(entityId: string) {

        if (!isUri(entityId)) {
            throw errorTypes.BadRequestData.withDetail(`'${entityId}' is not a valid NGSI-LD entity ID.`)
        }

        let result = await this.psql.deleteEntity(entityId).catch((e) => {
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

        for (const ec of entities_compacted) {
            const nonNormalizedContext = (contextUrl != undefined) ? contextUrl : ec['@context']
            const actualContext = appendCoreContext(nonNormalizedContext)
            const context = await getNormalizedContext(actualContext)

            entities_expanded.push(expandObject(ec, context))
        }

        let checkResult = checkArrayOfEntities(entities_expanded, true, true)

        if (checkResult.length > 0) {
            throw errorTypes.BadRequestData.withDetail(checkResult.join("\n"))
        }
        //############### END Validate input ###############


        //######## BEGIN Iterate over list of uploaded entities and try to write them to the database ########
        const result = new BatchOperationResult()

        for (const ec of entities_compacted) {

            const nonNormalizedContext = (contextUrl != undefined) ? contextUrl : ec['@context']
            const actualContext = appendCoreContext(nonNormalizedContext)
            const context = await getNormalizedContext(actualContext)

            const entity_expanded = expandObject(ec, context)

           

            const resultCode = await this.psql.createEntity(entity_expanded, false)

            if (resultCode == 1) {                
                result.success.push(entity_expanded['@id'])
            }
            else {
                result.errors.push(new BatchEntityError(entity_expanded['@id'], new ProblemDetails("", "Entity creation failed.", "An entity with the same ID already exists.", 409)))
            }
        }
        //######## END Iterate over list of uploaded entities and try to write them to the database ########

        return new Promise<BatchOperationResult>((resolve, reject) => {
            resolve(result)
        })
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

        for (const ec of entities_compacted) {
            const nonNormalizedContext = (contextUrl != undefined) ? contextUrl : ec['@context']
            const actualContext = appendCoreContext(nonNormalizedContext)
            const context = await getNormalizedContext(actualContext)

            entities_expanded.push(expandObject(ec, context))
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
            const existingEntityMetadata = await this.psql.getEntityMetadata(entity_expanded['@id'], false)


            // ############## BEGIN CREATE the Entity if it does not exist ###############            
            if (!existingEntityMetadata) {

                const resultCode = await this.psql.createEntity(entity_expanded, false)

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
                    const deleteResult = await this.psql.deleteEntity(entity_expanded['@id']).catch((e) => {
                        // NOTE: If the entity does not exist, deleteEntity() throws an exception.
                        // We can and must ignore this exception. Non-existence of an entity
                        // with the same ID is not a problem here, since this is an UPSERT.

                        // If the delete failed due to another error that is not handled here,
                        // the creation of the replacement entity (next step) will fail, and the
                        // error is handled then.
                    })

                    // NOTE: No need to process return value

                    const resultCode = await this.psql.createEntity(entity_expanded, false)

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

                    const updateResult = await this.psql.appendEntityAttributes(entity_expanded['@id'], entity_expanded, true)

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


        return new Promise<BatchOperationResult>((resolve, reject) => {
            resolve(result)
        })
    }


    // Spec 5.6.9
    async api_5_6_9_batchEntityUpdate(jsonString: string, contextUrl: string | undefined, overwrite: boolean): Promise<BatchOperationResult> {

        // TODO: 1 What to do with the overwrite parameter?

        const entities_compacted = parseJson(jsonString)

        if (!(entities_compacted instanceof Array)) {
            throw errorTypes.BadRequestData.withDetail("The provided payload is not a JSON array")
        }

        const entities_expanded = []

        for (const ec of entities_compacted) {
            const nonNormalizedContext = (contextUrl != undefined) ? contextUrl : ec['@context']
            const actualContext = appendCoreContext(nonNormalizedContext)
            const context = await getNormalizedContext(actualContext)

            entities_expanded.push(expandObject(ec, context))
        }

        //############### BEGIN Validate input ###############


        const checkResult = checkArrayOfEntities(entities_expanded, true, true)

        if (checkResult.length > 0) {
            throw errorTypes.BadRequestData.withDetail(checkResult.join("\n"))
        }
        //############### END Validate input ###############


        const result = new BatchOperationResult()


        //######## BEGIN Iterate over list of uploaded entities and try to update them ########
        for (const entity of entities_expanded) {

            // NOTE: We need to stringify the entity here to pass it to appendEntityAttributes():
            const entityJsonString = JSON.stringify(entity)


            // TODO: 2 Really call another top-level API method here?
            const appendResult = await this.api_5_6_3_appendEntityAttributes(entity['@id'], entityJsonString, contextUrl, true).catch((problem) => {
                result.errors.push(new BatchEntityError(entity['@id'], problem))
            })

            if (appendResult) {
                result.success.push(entity['@id'])
            }
        }
        //######## END Iterate over list of uploaded entities and try to update them ########


        return new Promise<BatchOperationResult>((resolve, reject) => {
            resolve(result)
        })
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
            const deleteResult = await this.psql.deleteEntity(id).catch((e) => {
                result.errors.push(new BatchEntityError(id, new ProblemDetails("", "Failed to delete entity.", "No entity with the provided ID exists.", 404)))
            })

            if (deleteResult == true) {
                result.success.push(id)
            }
        }
        //################ END Iterate over entity IDs and delete the respective entities ###############

        return new Promise<BatchOperationResult>((resolve, reject) => {
            resolve(result)
        })
    }


    // Spec 5.6.11
    async api_5_6_11_createOrUpdateTemporalEntity(temporalEntityString: string, contextUrl: string | undefined): Promise<number> {

        const entity_compacted = parseJson(temporalEntityString)

        const nonNormalizedContext = (contextUrl != undefined) ? contextUrl : entity_compacted['@context']
        const actualContext = appendCoreContext(nonNormalizedContext)
        const context = await getNormalizedContext(actualContext)

        const entity_expanded = expandObject(entity_compacted, context)


        //################# BEGIN Validate input ##################
        const entityCheckResults = checkEntity(entity_expanded, false)

        if (entityCheckResults.length > 0) {
            throw errorTypes.InvalidRequest.withDetail("The submitted data is not a valid NGSI-LD temporal entity: " + entityCheckResults.join(" "))
        }
        //################# END Validate input ##################

        // NOTE: This currently returns a HTTP status code. This is perhaps not ideal.
        return await this.psql.createOrUpdateTemporalEntity(entity_expanded)
    }


    // Spec 5.6.12
    async api_5_6_12_addAttributesToTemporalEntity(entityId: string, fragmentString: string, contextUrl: string | undefined) {

        const fragment_compacted = parseJson(fragmentString)

        const nonNormalizedContext = (contextUrl != undefined) ? contextUrl : fragment_compacted['@context']
        const actualContext = appendCoreContext(nonNormalizedContext)
        const context = await getNormalizedContext(actualContext)

        const fragment_expanded = expandObject(fragment_compacted, context)


        const entityCheckResults = checkEntity(fragment_expanded, false)

        if (entityCheckResults.length > 0) {
            throw errorTypes.InvalidRequest.withDetail("The submitted data is not a valid NGSI-LD entity: " + entityCheckResults.join(" "))
        }


        //###################### BEGIN Try to fetch existing entity ########################
        const entityMetadata = await this.psql.getEntityMetadata(entityId, true)

        if (!entityMetadata) {
            throw errorTypes.ResourceNotFound.withDetail("No entity with the passed ID exists: " + entityId)
        }
        //###################### END Try to fetch existing entity ########################


        await this.psql.addAttributesToEntity(entityMetadata.id, fragment_expanded)
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


        await this.deleteAttribute(entityId, true, attributeId_compacted, useDatasetId_compacted, undefined, contextUrl)
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

        let patchFragmentAttribute = fragment_expanded[attributeId_expanded]

        if (patchFragmentAttribute == undefined) {
            throw errorTypes.BadRequestData.withDetail("Provided entity fragment does not contain an attribute with the id " + attributeId_expanded)
        }

        if (!(patchFragmentAttribute instanceof Array)) {
            throw errorTypes.ResourceNotFound.withDetail(`The attribute to patch ("${attributeId_expanded}") is not an array in the provided entity fragment`)
        }

        if (patchFragmentAttribute.length != 1) {
            throw errorTypes.BadRequestData.withDetail(`The attribute to patch ("${attributeId_expanded}") does not have the expected amount of exactly 1 attribute instances in the submitted NGSI-LD fragment`)
        }
        //########################### END Use-Case-specific NGSI-LD Input validation #########################

        const instance = patchFragmentAttribute[0]

        console.log(instance)
        await this.psql.updateAttributeInstanceOfTemporalEntity(entityId, attributeId_expanded, instanceId_expanded, instance)
                
        

    }


    // Spec 5.6.15
    async api_5_6_15_deleteAttributeInstanceOfTemporalEntity(entityId: string, attributeId_compacted: string, instanceId_compacted: string, contextUrl: string | undefined) {

        await this.deleteAttribute(entityId, true, attributeId_compacted, undefined, instanceId_compacted, contextUrl)
    }


    // Spec 5.6.16
    async api_5_6_16_deleteTemporalEntity(entityId: string) {

        if (!isUri(entityId)) {
            throw errorTypes.BadRequestData.withDetail("Passed Entity ID is not a valid URI: " + entityId)
        }

        // TODO: 2 Catch SQL exceptions here instead of returning them
        const result = await this.psql.deleteEntity(entityId).catch((e) => {
            throw errorTypes.ResourceNotFound.withDetail("No entity with the passed ID exists: " + entityId)
        })
    }



    // Spec 5.7.1
    async api_5_7_1_retrieveEntity(entityId: string,
        attrs_compacted: Array<string> | undefined,
        geometryProperty_compacted: string | undefined,
        datasetId: string | undefined,
        options: Array<string> | undefined,
        contextUrl: string | undefined): Promise<any | Feature> {

        const actualContext = appendCoreContext(contextUrl)
        const context = await getNormalizedContext(actualContext)


        const attrs_expanded = expandObject(attrs_compacted, context)

        let includeSysAttrs = false
        let keyValues = false

        if (options instanceof Array) {
            keyValues = options.includes("keyValues")
            includeSysAttrs = options.includes("sysAttrs")
        }

        const entity_expanded = await this.psql.getEntity(entityId, false, attrs_expanded, undefined, includeSysAttrs)



        // NOTE: If something unexpected happens during retrieval of the entity from the database
        // (e.g. no entity with the passed ID exists), an exception is thrown and the program never
        // continues to this point. I.e. whenever we reach this point here, we can be sure that
        // the variable 'entity' does actually contain an entity.

        let result = entity_expanded

        if (keyValues) {
            result = util.simplifyEntity(result)
        }

        // Return GeoJSON representation if requested:
        if (geometryProperty_compacted != undefined) {

            const geometryProperty_expanded = expandObject(geometryProperty_compacted, context)

            result = compactedEntityToGeoJsonFeature(result, geometryProperty_expanded, datasetId)
        }


        const result_compacted = compactObject(result, context)


        result_compacted['@context'] = actualContext

        return result_compacted
    }


    // Spec 5.7.2
    async api_5_7_2_queryEntities(query: Query, contextUrl: string | undefined): Promise<Array<any> | FeatureCollection> {

        let includeSysAttrs = false
        let keyValues = false

        if (query.options instanceof Array) {
            includeSysAttrs = query.options.includes("sysAttrs")
            keyValues = query.options.includes("keyValues")
        }


        const actualContext = appendCoreContext(contextUrl)

        const context = await getNormalizedContext(actualContext)


        // Fetch entities
        let entities_expanded = await this.psql.queryEntities(query, false, includeSysAttrs, context)


        if (keyValues) {

            //#################### BEGIN Create simplified representation ##################

            let result = []

            // TODO: Move to helper function
            for (const entity of entities_expanded) {
                result.push(util.simplifyEntity(entity))
            }

            entities_expanded = result
            //#################### END Create simplified representation ##################
        }



        // NOTE: Here, we enable GeoJSON output if the query parameter 'geometryProperty' is defined.
        // This does not follow the NGSI-LD spec. Correctly, GeoJSON output is enabled through setting
        // of the request accept header "application/geo+json" (see spec 6.3.15):

        let result: any = null

        // If no GeoJSON output is requested, return normal NGSI-LD:
        if (query.geometryProperty == undefined) {

            result = Array<any>()

            for (const ex of entities_expanded) {
                let ec = compactObject(ex, context)
                ec['@context'] = actualContext
                result.push(ec)
            }
        }
        else {
            // Otherwise, return GeoJSON:
            result = new FeatureCollection()

            //const geometryProperty_expanded = expandObject(query.geometryProperty, context)

            const geometryProperty_compacted = query.geometryProperty

            for (const entity_expanded of entities_expanded) {

                const entity_compacted = compactObject(entity_expanded, context)
                entity_compacted['@context'] = actualContext

                const feature = compactedEntityToGeoJsonFeature(entity_compacted, geometryProperty_compacted, query.datasetId)
                result.features.push(feature)
            }
        }

        return result
    }


    // Spec 5.7.3
    async api_5_7_3_retrieveTemporalEntity(
        entityId: string,
        attrs_compacted: Array<string> | undefined,
        // NOTE: The parameter "lastN" is part of TemporalQuery
        temporalQ: TemporalQuery | undefined,
        contextUrl: string | undefined) {

        // TODO: Which request option enables system attributes?
        const includeSysAttrs = false

        //#################### BEGIN Validation ###################
        if (!isUri(entityId)) {
            throw errorTypes.BadRequestData.withDetail(`'${entityId}' is not a valid NGSI-LD entity ID.`)
        }
        //#################### END Validation ###################


        const actualContext = appendCoreContext(contextUrl)
        const context = await getNormalizedContext(actualContext)



        const attrs_expanded = expandObject(attrs_compacted, context)

        const returnedEntity_expanded = await this.psql.getEntity(entityId, true, attrs_expanded, temporalQ, includeSysAttrs)


        const returnedEntity_compacted = compactObject(returnedEntity_expanded, context)

        return returnedEntity_compacted
    }


    // Spec 5.7.4
    async api_5_7_4_queryTemporalEntities(query: Query, contextUrl: string | undefined) {

        const actualContext = appendCoreContext(contextUrl)
        const context = await getNormalizedContext(actualContext)


        if (query.temporalQ == undefined) {
            throw errorTypes.BadRequestData.withDetail("No temporal query provided in request.")
        }


        let includeSysAttrs = false

        if (query.options instanceof Array) {
            includeSysAttrs = query.options.includes("sysAttrs")
        }


        // Fetch entities
        const entities_expanded = await this.psql.queryEntities(query, true, includeSysAttrs, context)

        // NOTE: Here, we enable GeoJSON output if the query parameter 'geometryProperty' is defined.
        // This does not follow the NGSI-LD spec. Correctly, GeoJSON output is enabled through setting
        // of the request accept header "application/geo+json" (see spec 6.3.15):

        // If no GeoJSON output is requested, return normal NGSI-LD:
        if (query.geometryProperty == undefined) {

            const entities_compacted = compactObject(entities_expanded, context)

            for (const ec of entities_compacted) {
                ec['@context'] = actualContext
            }

            return entities_compacted
        }


        // Otherwise, return GeoJSON:

        let geojsonResult = new FeatureCollection()

        //############ BEGIN Iterate over returned entities to build GeoJSON response ############
        for (const entity_expanded of entities_expanded) {

            const entity_compacted = compactObject(entities_expanded, context)
            entity_compacted['@context'] = actualContext
            const feature = compactedEntityToGeoJsonFeature(entity_compacted, query.geometryProperty, query.datasetId)
            geojsonResult.features.push(feature)
        }
        //############ END Iterate over returned entities to build GeoJSON response ############

        return geojsonResult
    }


    // Spec 5.7.5
    async api_5_7_5_retrieveAvailableEntityTypes() {
        return await this.psql.getEntityTypes()
    }


    // Spec 5.7.6
    async api_5_7_6_retrieveAvailableEntityTypeDetails() {
        return await this.psql.getDetailsOfEntityTypes()
    }


    // Spec 5.7.7
    async api_5_7_7_retrieveAvailableEntityTypeInformation(type: string) {
        return await this.psql.getEntityTypeInformation(type)
    }


    // Spec 5.7.8 
    async api_5_7_8_retrieveAvailableAttributes() {
        return await this.psql.getDetailsOfAvailableAttributes()
    }


    // Spec 5.7.9
    async api_5_7_9_retrieveAvailableAttributeDetails() {
        return await this.psql.getAvailableAttributes()
    }


    // Spec 5.7.10
    async api_5_7_10_retrieveAvailableAttributeInformation(attrType_compacted: string, contextUrl: string | undefined) {

        const actualContext = appendCoreContext(contextUrl)
        const context = await getNormalizedContext(actualContext)

        const attrType_expanded = expandObject(attrType_compacted, context)

        return await this.psql.getAttributeInfo(attrType_expanded)
    }


    // TODO: 4 Implement 5.8 - 5.11

    //################################# END Official API methods ######################################



    //############################ BEGIN Inofficial API methods ############################
    async inofficial_deleteAllEntities() {
        await this.psql.deleteAllEntities()
    }


    async inofficial_temporalEntityOperationsUpsert(jsonString: any, contextUrl: string | undefined) {

        // TODO: 3 "createOrUpdate" isn't really an upsert! 
        // It only *adds* attributes, it doesn't replace the entire entity!

        const entities_compacted = parseJson(jsonString)

        for (const ec of entities_compacted) {
            await this.api_5_6_11_createOrUpdateTemporalEntity(JSON.stringify(ec), contextUrl)
        }
    }
    //############################ END Inofficial API methods ############################



    async deleteAttribute(entityId: string, temporal: boolean, attributeId_compacted: string, datasetId_compacted: string | null | undefined, instanceId_expanded: string | undefined, contextUrl: any) {

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

        if (instanceId_expanded != undefined && !isUri(instanceId_expanded)) {
            throw errorTypes.BadRequestData.withDetail("Passed instance ID is not a valid URI.")
        }
        //######################## END Input validation ##############################


        //######## BEGIN Read target entity from database to get its internal ID, which is required for the delete call ##########
        const entityMetadata = await this.psql.getEntityMetadata(entityId, temporal)

        if (!entityMetadata) {
            throw errorTypes.ResourceNotFound.withDetail("No entity with the passed ID exists: " + entityId)
        }

        const entityInternalId = entityMetadata.id
        //######## END Read target entity from database to get its internal ID, which is required for the delete call ##########

        const rowCount = await this.psql.deleteAttribute(entityInternalId, attributeId_expanded, instanceId_expanded, datasetId_expanded)

        if (rowCount == 0) {
            throw errorTypes.ResourceNotFound.withDetail(`Failed to delete attribute instance. No attribute instance with the following properties exists: Entity ID = '${entityId}', Attribute ID ='${attributeId_expanded}', Instance ID = '${instanceId_expanded}'.`)
        }
    }
}