// TODO: 3 Move everything PSQL-related to PsqlBackend and introduce a "neutral" interface / base class
// for backends. The ContextBroker class should not deal with back end implementation details.

import { BatchEntityError } from "./dataTypes/BatchEntityError"
import { BatchOperationResult } from "./dataTypes/BatchOperationResult"
import { Feature } from "./dataTypes/Feature"
import { FeatureCollection } from "./dataTypes/FeatureCollection"
import { NotUpdatedDetails } from "./dataTypes/NotUpdatedDetails"
import { ProblemDetails } from "./dataTypes/ProblemDetails"
import { Query } from "./dataTypes/Query"
import { TemporalQuery } from "./dataTypes/TemporalQuery"
import { UpdateResult } from "./dataTypes/UpdateResult"
import { errorTypes } from "./errorTypes"
import { PsqlBackend } from "./psqlBackend/PsqlBackend"
import { checkArrayOfEntities, checkArrayOfUris, checkReifiedAttribute, checkEntity, isUri, isReifiedAttribute } from "./validate"
import { appendCoreContext, compactObject, expandObject, getNormalizedContext, NGSI_LD_CORE_CONTEXT_URL } from "./jsonld"
import { parseJson, compactedEntityToGeoJsonFeature as compactedEntityToGeoJsonFeature } from "./util"
import { threadId } from "node:worker_threads"
import { isConstructorTypeNode } from "typescript"



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


        let resultCode = await this.psql.createEntity(entity_expanded, false).catch((errorCode) => {

            if (errorCode == "23505") {
                throw errorTypes.AlreadyExists.withDetail(`An Entity with the ID '${entity_expanded['@id']}' already exists.`)
            }
        })
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


        return await this.psql.appendEntityAttributes(entityId, fragment_expanded,overwrite)
      
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

        const entityCheckResults = checkEntity(fragment_compacted, true)

        if (entityCheckResults.length > 0) {
            throw errorTypes.InvalidRequest.withDetail("The submitted data is not a valid NGSI-LD entity: " + entityCheckResults.join(" "))
        }


        let attribute_expanded = fragment_expanded[attributeId_expanded]

        // Convert attribute to array representation if it isn't yet:
        if (!(attribute_expanded instanceof Array)) {
            attribute_expanded = [attribute_expanded]
        }

        const attributeCheckResults = checkReifiedAttribute(attribute_expanded, attributeId_expanded, undefined, true)

        if (attributeCheckResults.length != 0) {
            throw errorTypes.BadRequestData.withDetail(`The field '${attributeId_expanded}' in the uploaded entity fragment is not a valid NGSI-LD attribute: ${attributeCheckResults.join("\n")}`)
        }
        //################### END Input validation ##################

        this.psql.partialAttributeUpdate(entityId, attributeId_expanded, attribute_expanded)
      
    }


    // Spec 5.6.5
    async api_5_6_5_deleteEntityAttribute(entityId: string, attributeId_compacted: string, datasetId: string | undefined, contextUrl: string | undefined, deleteAll: boolean) {

        const actualContext = appendCoreContext(contextUrl)
        const context = await getNormalizedContext(actualContext)


        const attributeId_expanded = expandObject(attributeId_compacted, context)


        if (!isUri(attributeId_expanded)) {
            throw errorTypes.BadRequestData.withDetail("Attribute ID is not a valid URI: " + attributeId_expanded)
        }

        let useDatasetId: string | null | undefined = datasetId

        // If datasetId is undefined, but 'deleteAll' is not set, this means that the default instance
        // should be deleted, which is characterized by having datasetId = null:

        if (datasetId == undefined && !deleteAll) {
            useDatasetId = null
        }

        await this.deleteAttribute(entityId, false, attributeId_expanded, useDatasetId)
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

            const entity = expandObject(ec, context)

            const creationResultCode = await this.psql.createEntity(entity, false).catch((errorCode) => {

                if (errorCode == "23505") {
                    result.errors.push(new BatchEntityError(entity['@id'], new ProblemDetails("", "Entity creation failed.", "An entity with the same ID already exists.", 409)))
                }
            })

            if (creationResultCode == 1) {
                result.success.push(entity['@id'])
            }
        }
        //######## END Iterate over list of uploaded entities and try to write them to the database ########

        return new Promise<BatchOperationResult>((resolve, reject) => {
            resolve(result)
        })
    }


    // Spec 5.6.8
    async api_5_6_8_batchEntityUpsert(jsonString: string, options: string, contextUrl: string | undefined): Promise<BatchOperationResult> {


        const entities_compacted = parseJson(jsonString)

        //############### BEGIN Validate input ###############
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


        let result = new BatchOperationResult()


        //######## BEGIN Iterate over list of uploaded entities and try to upsert them ########
        for (const entity_expanded of entities_expanded) {


            // ############## BEGIN Create the Entity if it does not exist ###############            
            if (!this.psql.getEntityMetadata(entity_expanded['@id'], false)) {

                let creationResultCode = await this.psql.createEntity(entity_expanded, false).catch((errorCode) => {

                    if (errorCode == "23505") {
                        result.errors.push(new BatchEntityError(entity_expanded['@id'], new ProblemDetails("", "Entity creation failed.", "An entity with the same ID already exists.", 409)))
                    }
                })
            }
            // ############## END Create the Entity if it does not exist ###############

            // ############## BEGIN Otherwise, update existing entity ###############
            else {
                //############ BEGIN "replace" mode (delete existing and create new) ##############
                if (options == "replace") {

                    // First delete the existing entity:
                    let deleteResult = await this.psql.deleteEntity(entity_expanded['@id']).catch((e) => {
                        // NOTE: If the entity does not exist, deleteEntity() throws an exception.
                        // We can and must ignore this exception. Non-existence of an entity
                        // with the same ID is not a problem here, since this is an UPSERT.

                        // If the delete failed due to another error that is not handled here,
                        // the creation of the replacement entity (next step) will fail, and the
                        // error is handled then.
                    })

                    // NOTE: No need to process return value
                    const creationResultCode = await this.psql.createEntity(entity_expanded, false).catch((errorCode) => {

                        if (errorCode == "23505") {
                            result.errors.push(new BatchEntityError(entity_expanded['@id'], new ProblemDetails("", "Entity creation failed.", "An entity with the same ID already exists.", 409)))
                        }
                    })
                }
                //############ END "replace" mode (delete existing and create new) ##############


                //############ BEGIN "update" mode (update existing) ##############
                else if (options == "update") {

                    // NOTE: No need to process return value
                    const updateResult = await this.api_5_6_3_appendEntityAttributes(entity_expanded['@id'], jsonString, contextUrl, true)
                }
                //############ END "update" mode (update existing) ##############
            }
            // ############## END Otherwise, update existing entity ###############
        }
        //######## END Iterate over list of uploaded entities and try to upsert them ########


        return new Promise<BatchOperationResult>((resolve, reject) => {
            resolve(result)
        })
    }


    // Spec 5.6.9
    async api_5_6_9_batchEntityUpdate(jsonString: string, contextUrl: string | undefined, overwrite: boolean): Promise<BatchOperationResult> {

        // TODO: 1 What to do with the overwrite parameter?

        const entities_compacted = parseJson(jsonString)


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

        return await this.psql.addAttributesToTemporalEntity(entityId, fragment_expanded)
    }


    // Spec 5.6.13
    async api_5_6_13_deleteAttributeFromTemporalEntity(entityId: string, attributeId_compacted: string, datasetId: string | undefined, contextUrl: string | undefined, deleteAll: boolean) {

        const actualContext = appendCoreContext(contextUrl)
        const context = await getNormalizedContext(actualContext)

        const attributeId_expanded = expandObject(attributeId_compacted, context)

        let useDatasetId: string | null | undefined = datasetId

        // If datasetId is undefined, but 'deleteAll' is not set, this means that the default instance
        // should be deleted, which is characterized by having datasetId = null:

        if (datasetId == undefined && !deleteAll) {
            useDatasetId = null
        }

        await this.deleteAttribute(entityId, true, attributeId_expanded, useDatasetId)
    }


    // Spec 5.6.14
    async api_5_6_14_updateAttributeInstanceOfTemporalEntity(entityId: string, attributeId_compacted: string, 
        instanceId_compacted: string, fragmentString_compacted: string, contextUrl: string | undefined) {

        // TODO: 2 What if there are multiple attributes in the fragment?

        const fragment_compacted = parseJson(fragmentString_compacted)

        const nonNormalizedContext = (contextUrl != undefined) ? contextUrl : fragment_compacted['@context']
        const actualContext = appendCoreContext(nonNormalizedContext)
        const context = await getNormalizedContext(actualContext)

        const fragment_expanded = expandObject(fragment_compacted, context)

        const attributeId_expanded = expandObject(attributeId_compacted, context)
        const instanceId_expanded = expandObject(instanceId_compacted, context)

        //########################### BEGIN Input validation #########################
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
        //########################### END Input validation #########################

        if (!(attributeId_expanded in fragment_expanded)) {
            throw errorTypes.BadRequestData.withDetail("Provided entity fragment does not contain an attribute with the id " + attributeId_expanded)
        }

        let attribute = fragment_expanded[attributeId_expanded]

        if (!(attribute instanceof Array)) {
            attribute = [attribute]
        }

        for(const instance of attribute) {
            if (instance["https://uri.etsi.org/ngsi-ld/instanceId"] == instanceId_expanded) {
                await this.psql.updateAttributeInstanceOfTemporalEntity(entityId, attributeId_expanded, instanceId_expanded, instance)        
                return
            }
        }

        if (!(attributeId_expanded in fragment_expanded)) {
            throw errorTypes.BadRequestData.withDetail("Provided entity fragment does not contain an attribute instance with the instanceId " + instanceId_expanded)
        }
    }


    // Spec 5.6.15
    async api_5_6_15_deleteAttributeInstanceOfTemporalEntity(entityId: string, attributeId_compacted: string, instanceId: string, contextUrl: string | undefined) {

        const actualContext = appendCoreContext(contextUrl)
        const context = await getNormalizedContext(actualContext)

        const attributeId_expanded = expandObject(attributeId_compacted, context)

        //########################### BEGIN Input validation #########################
        if (!isUri(entityId)) {
            throw errorTypes.BadRequestData.withDetail(`'${entityId}' is not a valid URI.`)
        }

        if (!isUri(attributeId_expanded)) {
            throw errorTypes.BadRequestData.withDetail(`'${attributeId_expanded}' is not a valid URI.`)
        }

        if (!isUri(instanceId)) {
            throw errorTypes.BadRequestData.withDetail(`'${instanceId}' is not a valid URI.`)
        }
        //########################### END Input validation #########################

        const entityMetadata = await this.psql.getEntityMetadata(entityId, true)

        if (entityMetadata == undefined) {
            throw errorTypes.ResourceNotFound.withDetail(`No entity with ID '${entityId}' exists.`)
        }


        // TODO: Call this.deleteAttribute() here?
        const numDeletedRows = await this.psql.deleteAttribute(entityMetadata.id, attributeId_expanded, instanceId, undefined)

        if (numDeletedRows == 0) {
            throw errorTypes.ResourceNotFound.withDetail("No attribute instance with the specified properties exists.")
        }
    }


    // Spec 5.6.16
    async api_5_6_16_deleteTemporalEntity(entityId: string) {

        if (!isUri(entityId)) {
            throw errorTypes.BadRequestData.withDetail("Passed Entity ID is not a valid URI: " + entityId)
        }

        // TODO: 2 Catch SQL exceptions here instead of returning them
        let result = await this.psql.deleteEntity(entityId).catch((e) => {
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

        if (options instanceof Array) {
            includeSysAttrs = options.includes("sysAttrs")
        }

        const entity_expanded = await this.psql.getEntity(entityId, false, attrs_expanded, undefined, includeSysAttrs)



        // NOTE: If something unexpected happens during retrieval of the entity from the database
        // (e.g. no entity with the passed ID exists), an exception is thrown and the program never
        // continues to this point. I.e. whenever we reach this point here, we can be sure that
        // the variable 'entity' does actually contain an entity.

        let result = entity_expanded

        // Return GeoJSON representation if requested:
        if (geometryProperty_compacted != undefined) {

            const geometryProperty_expanded = expandObject(geometryProperty_compacted, context)

            result = compactedEntityToGeoJsonFeature(entity_expanded, geometryProperty_expanded, datasetId)
        }


        const result_compacted = compactObject(result, context)


        result_compacted['@context'] = actualContext

        return result_compacted
    }


    // Spec 5.7.2
    async api_5_7_2_queryEntities(query: Query, contextUrl: string | undefined): Promise<Array<any> | FeatureCollection> {

        const includeSysAttrs = (query.options instanceof Array) ? query.options.includes("sysAttrs") : false

        const actualContext = appendCoreContext(contextUrl)
        const context = await getNormalizedContext(actualContext)

        // Fetch entities
        const entities_expanded = await this.psql.queryEntities(query, false, includeSysAttrs, context)


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

            const geometryProperty_expanded = expandObject(query.geometryProperty, context)

            
            for (const entity_expanded of entities_expanded) {

                const entity_compacted = compactObject(entity_expanded, context)
                entity_compacted['@context'] = actualContext

                const feature = compactedEntityToGeoJsonFeature(entity_compacted, query.geometryProperty, query.datasetId)
                result.features.push(feature)
            }
        }
        
        return result
    }


    // Spec 5.7.3
    async api_5_7_3_retrieveTemporalEntity(
        entityId: string,
        attrs_compacted: Array<string> | undefined,
        temporalQ: TemporalQuery | undefined,
        contextUrl: string | undefined) {

        const includeSysAttrs = false

        if (!isUri(entityId)) {
            throw errorTypes.BadRequestData.withDetail(`'${entityId}' is not a valid NGSI-LD entity ID.`)
        }

        const actualContext = appendCoreContext(contextUrl)
        const context = await getNormalizedContext(actualContext)

        const attrs_expanded = expandObject(attrs_compacted, context)

        return await this.psql.getEntity(entityId, false, attrs_expanded, temporalQ, includeSysAttrs)
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
    async api_5_7_10_retrieveAvailableAttributeInformation(attrType_compacted: string, contextUrl : string|undefined) {
        
        const actualContext = appendCoreContext(contextUrl)
        const context = await getNormalizedContext(actualContext)

        const attrType_expanded = expandObject(attrType_compacted, context)
        
        return await this.psql.getAttributeInfo(attrType_expanded)
    }


    // TODO: 4 Implement 5.8 - 5.11

    //################################# END Official API methods ######################################



    async deleteAttribute(entityId: string, temporal: boolean, attributeId_expanded: string, datasetId: string | null | undefined) {

        // TODO: 4 Is there a difference between this and api_5_6_15_deleteAttributeInstanceOfTemporalEntity?
        // Probably we can share most code.

        //######################## BEGIN Input validation ##############################
        if (!isUri(entityId)) {
            throw errorTypes.BadRequestData.withDetail("Passed entity ID is not a valid URI.")
        }

        if (!isUri(attributeId_expanded)) {
            throw errorTypes.BadRequestData.withDetail("Passed attribute ID is not a valid URI.")
        }
        //######################## END Input validation ##############################


        //############# BEGIN Try to fetch target entity #################
        const metadata = await this.psql.getEntityMetadata(entityId, temporal)

        if (!metadata) {
            throw errorTypes.ResourceNotFound.withDetail("No entity with the passed ID exists: " + entityId)
        }
        //############# END Try to fetch target entity #################


        let rowCount = await this.psql.deleteAttribute(metadata.id, attributeId_expanded, undefined, datasetId)

        if (rowCount == 0) {
            throw errorTypes.ResourceNotFound.withDetail(`The target entity '${entityId} does not contain an attribute with ID '${attributeId_expanded}.`)
        }
    }


    async inofficial_deleteAllEntities() {
        this.psql.deleteAllEntities()
    }
}