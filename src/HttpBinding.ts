// NGSI-LD Version 1.3.1
// https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.03.01_60/gs_CIM009v010301p.pdf

// TODO: 1 Test whether NGSI-LD queries still work properly with temporal entities: For normal requests,
// temporal attribute instances should be ignored!
// TODO: 1 Test PostgreSQL connection at broker startup
// TODO: 2 Implement 4.6.2
// TODO: 3 Spec 6.3.4 vervollständigen (v.a. check von Accept Headers)
// TODO: 3 Spec 6.3.5 (Extract context from request)
// TODO: 3 Spec 6.3.6 (Response context representation)
// TODO: 3 Support GeoJSON for GeoProperty as string
// TODO: 3 Correct implementation for what to expand and what not

// TODO: 3 Automatically add "createdAt" and "modifiedAt" to all Attributes in JSON 
// so that these fields can be queried with the query language. Remove them from output if not explicitly requested.



// TODO: 3 GeoJSON response headers-Gedöns (spec 6.3.6)
// TODO: 3 5.7.2.4 Match ID patterns
// TODO: 4 Complete criteria in Spec 5.5.4 (context and null)
// TODO: 4 Spec 4.5.9
// TODO: 4 Print context parse errors in response
// TODO: 4 Spec 5.7.2.4 Context header in GeoJSON response?
// TODO 4: Implement "limit" / pagination (spec 6.3.10)
// TODO: Spec 5.5.5
// TODO: Spec 5.5.6
// TODO: 5 Spec 5.5.9 (pagination)
// TODO: 5 Spec 6.3.12
// TODO: 5 Spec 6.3.13 (results count header)



import * as Koa from "koa"
import * as compress from "koa-compress"
import * as Router from "koa-router"
import * as klogger from "koa-logger"
import * as kbodyparser from "koa-bodyparser"
import { ContextBroker } from "./ContextBroker"
import { GeoQuery } from "./dataTypes/GeoQuery"
import { errorTypes } from "./errorTypes"
import { EntityInfo } from "./dataTypes/EntityInfo"
import { Query } from "./dataTypes/Query"
import { TemporalQuery } from "./dataTypes/TemporalQuery"
import { getNormalizedContext, NGSI_LD_CORE_CONTEXT_URL } from "./jsonld"
import { PsqlBackend } from "./psqlBackend/PsqlBackend"
import * as fs from 'fs'
import * as auth from 'basic-auth'
//import createStatsCollector = require("mocha/lib/stats-collector")


export class HttpBinding {

    // Spec 6.2:
    private readonly apiName = "ngsi-ld"
    private readonly apiVersion = "v1"
    private readonly apiBase = "/" + this.apiName + "/" + this.apiVersion + "/"

    private readonly app = new Koa()
    private readonly router = new Router()
    private broker!: ContextBroker

    private readonly ERROR_MSG_NOT_IMPLEMENTED_YET = "This operation is not implemented yet."

    private readonly catchExceptions = false

    // NOTE: The HTTP handler methods must be defined as arrow functions in order to work!

    config: any

    // Spec 6.4.3.1
    // Binding for spec 5.6.1
    http_6_4_3_1_POST_createEntity = async (ctx: any, next: any) => {

        if (this.getUser(auth(ctx.request)) == null) {
            throw errorTypes.BadRequestData.withDetail("Operation not allowed with the provided user credentials.")        
        }

        const contextUrl = this.resolveRequestJsonLdContext(ctx.request)

        await this.broker.api_5_6_1_createEntity(ctx.request.rawBody, contextUrl)
        ctx.status = 201


        await next()
    }


    // Spec 6.4.3.2
    // Binding for Spec 5.7.2
    http_6_4_3_2_GET_queryEntities = async (ctx: any, next: any) => {

        // NOTE: This is the HTTP GET version of entities query.

        const contextUrl = this.resolveRequestJsonLdContext(ctx.request) as string

        const options = (typeof (ctx.request.query.options) == "string") ? (ctx.request.query.options as string).split(",") : []

        //############## BEGIN Build EntityInfo array ###############
        const entityIds = (typeof (ctx.request.query.id) == "string") ? (ctx.request.query.id as string).split(",") : []
        const entityTypes = (typeof (ctx.request.query.type) == "string") ? (ctx.request.query.type as string).split(",") : []
        const idPattern = (typeof (ctx.request.query.idPattern) == "string") ? ctx.request.query.idPattern : undefined

        const entities = Array<EntityInfo>()

        for (const id of entityIds) {
            entities.push(new EntityInfo(id, undefined, undefined))
        }

        for (const type of entityTypes) {
            entities.push(new EntityInfo(undefined, undefined, type))
        }

        if (idPattern != undefined) {
            entities.push(new EntityInfo(undefined, idPattern, undefined))
        }
        //############## END Build EntityInfo array ###############


        // ############### BEGIN Create GeoQuery object ##############
        const georel = ctx.request.query.georel
        const geometry = ctx.request.query.geometry
        const geoproperty = ctx.request.query.geoproperty

        let coordinates = undefined

        if (ctx.request.query.coordinates) {
            try {
                coordinates = JSON.parse(ctx.request.query.coordinates)
            }
            catch (e) {
                throw errorTypes.InvalidRequest.withDetail("Invalid geo query: 'coordinates' is not a JSON array.")
            }
        }


        let geoQ = undefined

        // If at least one of the geo query parameters are provided, we make this a geo query
        // by creating a GeoQuery object, even if the other parameters are undefined. 
        // Later in the PsqlBackend, the GeoQuery object will validated:

        if (geometry || coordinates || georel || geoproperty) {
            geoQ = new GeoQuery(geometry, coordinates, georel, geoproperty)
        }
        // ############### END Create GeoQuery object ##############


        const attrs = (typeof (ctx.request.query.attrs) == "string") ? (ctx.request.query.attrs as string).split(",") : []

        const q = ctx.request.query.q
        const csf = ctx.request.query.csf

        // NOTE: The parameters 'geometryProperty' and 'datasetId' are defined in spec 6.3.15:

        const datasetId = ctx.request.query.datasetId

        let geometryProperty = ctx.request.query.geometryProperty

        if (ctx.request.headers["accept"] == "application/geo+json" && geometryProperty == undefined) {
            geometryProperty = "location"
        }

        // Create main Query object:        
        const query = new Query(entities, attrs, q, geoQ, csf, undefined, geometryProperty, datasetId, options)

        // Perform query:
        ctx.body = await this.broker.api_5_7_2_queryEntities(query, contextUrl)
        ctx.status = 200
        await next()
    }


    // Spec 6.5.3.1
    // Binding for spec 5.7.1
    http_6_5_3_1_GET_retrieveEntity = async (ctx: any, next: any) => {

        const contextUrl = this.resolveRequestJsonLdContext(ctx.request) as string

        const options = (typeof (ctx.request.query.options) == "string") ? (ctx.request.query.options as string).split(",") : []


        const attrs = (ctx.request.query.attrs) ? ctx.request.query.attrs.split(",") : undefined        

        // NOTE: The parameters 'geometryProperty' and 'datasetId' are defined in spec 6.3.15:

        let geometryProperty = ctx.request.query.geometryProperty

        if (ctx.request.headers["accept"] == "application/geo+json" && geometryProperty == undefined) {
            geometryProperty = "location"
        }


        ctx.body = await this.broker.api_5_7_1_retrieveEntity(ctx.params.entityId, attrs, geometryProperty, ctx.request.query.datasetId, options, contextUrl)
        ctx.status = 200
        await next()
    }


    // Spec 6.5.3.2
    // Binding for spec 5.6.6
    http_6_5_3_2_DELETE_deleteEntity = async (ctx: any, next: any) => {

        if (this.getUser(auth(ctx.request)) == null) {
            throw errorTypes.BadRequestData.withDetail("Operation not allowed with the provided user credentials.")
        }

        ctx.body = await this.broker.api_5_6_6_deleteEntity(ctx.params.entityId)
        ctx.status = 204

        await next()
    }


    // Spec 6.6.3.1
    // Binding for spec 5.6.3
    http_6_6_3_1_POST_appendEntityAttributes = async (ctx: any, next: any) => {

      
        if (this.getUser(auth(ctx.request)) == null) {
            throw errorTypes.BadRequestData.withDetail("Operation not allowed with the provided user credentials.")
        }

        const contextUrl = this.resolveRequestJsonLdContext(ctx.request)

        const options = (typeof (ctx.request.query.options) == "string") ? (ctx.request.query.options as string).split(",") : []

        const overwrite = !options.includes("noOverwrite")

        let result = await this.broker.api_5_6_3_appendEntityAttributes(ctx.params.entityId, ctx.request.rawBody, contextUrl, overwrite)       

        if (result.notUpdated.length > 0) {
            ctx.body = result
            ctx.status = 207            
        }
        else {
            ctx.status = 204
        }

        await next()
    }


    // Spec 6.6.3.2
    // Binding for spec 5.6.2
    http_6_6_3_2_PATCH_updateEntityAttributes = async (ctx: any, next: any) => {

        if (this.getUser(auth(ctx.request)) == null) {
            throw errorTypes.BadRequestData.withDetail("Operation not allowed with the provided user credentials.")
        }

        const contextUrl = this.resolveRequestJsonLdContext(ctx.request)

        const result = await this.broker.api_5_6_2_updateEntityAttributes(ctx.params.entityId, ctx.request.rawBody, contextUrl)

        if (result.notUpdated.length == 0) {
            ctx.status = 204
        }
        else {
            ctx.body = result
            ctx.status = 207
        }

        await next()
    }


    // Spec 6.7.3.1
    // Binding for spec 5.6.4
    http_6_7_3_1_PATCH_partialAttributeUpdate = async (ctx: any, next: any) => {

        if (this.getUser(auth(ctx.request)) == null) {
            throw errorTypes.BadRequestData.withDetail("Operation not allowed with the provided user credentials.")
        }

        const contextUrl = this.resolveRequestJsonLdContext(ctx.request)

        const result = await this.broker.api_5_6_4_partialAttributeUpdate(ctx.params.entityId, ctx.params.attrId, ctx.request.rawBody, contextUrl)
        ctx.status = 204

        await next()
    }


    // Spec 6.7.3.2
    // Binding for spec 5.6.5
    http_6_7_3_2_DELETE_deleteEntityAttribute = async (ctx: any, next: any) => {

        if (this.getUser(auth(ctx.request)) == null) {
            throw errorTypes.BadRequestData.withDetail("Operation not allowed with the provided user credentials.")
        }

        const contextUrl = this.resolveRequestJsonLdContext(ctx.request)

        const deleteAll = (ctx.request.query.deleteAll == "true")

        ctx.body = await this.broker.api_5_6_5_deleteEntityAttribute(ctx.params.entityId, ctx.params.attrId, ctx.request.query.datasetId, contextUrl, deleteAll)
        ctx.status = 204
        await next()
    }


    // Spec 6.8.3.1
    // Binding for spec 5.9.2
    http_6_8_3_1_registerContextSource = async (ctx: any, next: any) => {

        if (this.getUser(auth(ctx.request)) == null) {
            throw errorTypes.BadRequestData.withDetail("Operation not allowed with the provided user credentials.")
        }

        throw errorTypes.OperationNotSupported.withDetail(this.ERROR_MSG_NOT_IMPLEMENTED_YET)
        await next()
    }


    // Spec 6.8.3.2
    // Binding for spec 5.10.2
    http_6_8_3_2_queryContextSourceRegistrations = async (ctx: any, next: any) => {

        throw errorTypes.OperationNotSupported.withDetail(this.ERROR_MSG_NOT_IMPLEMENTED_YET)
        await next()
    }


    // Spec 6.9.3.1
    // Binding for spec 5.10.1
    http_6_9_3_1_retrieveContextSourceRegistration = async (ctx: any, next: any) => {

        throw errorTypes.OperationNotSupported.withDetail(this.ERROR_MSG_NOT_IMPLEMENTED_YET)
        await next()
    }


    // Spec 6.9.3.2
    // Binding for spec 5.9.3
    http_6_9_3_2_updateContextSourceRegistration = async (ctx: any, next: any) => {

        if (this.getUser(auth(ctx.request)) == null) {
            throw errorTypes.BadRequestData.withDetail("Operation not allowed with the provided user credentials.")
        }

        throw errorTypes.OperationNotSupported.withDetail(this.ERROR_MSG_NOT_IMPLEMENTED_YET)
        await next()
    }

    // Spec 6.9.3.3
    // Binding for spec 5.9.4
    http_6_9_3_3_deleteContextSourceRegistration = async (ctx: any, next: any) => {

        if (this.getUser(auth(ctx.request)) == null) {
            throw errorTypes.BadRequestData.withDetail("Operation not allowed with the provided user credentials.")
        }

        throw errorTypes.OperationNotSupported.withDetail(this.ERROR_MSG_NOT_IMPLEMENTED_YET)
        await next()
    }


    // Spec 6.10.3.1
    // Binding for spec 5.8.1
    http_6_10_3_1_createSubscription = async (ctx: any, next: any) => {

        if (this.getUser(auth(ctx.request)) == null) {
            throw errorTypes.BadRequestData.withDetail("Operation not allowed with the provided user credentials.")
        }

        throw errorTypes.OperationNotSupported.withDetail(this.ERROR_MSG_NOT_IMPLEMENTED_YET)
        await next()
    }


    // Spec 6.10.3.3
    // Binding for spec 5.8.4
    http_6_10_3_2_querySubscriptions = async (ctx: any, next: any) => {

        if (this.getUser(auth(ctx.request)) == null) {
            throw errorTypes.BadRequestData.withDetail("Operation not allowed with the provided user credentials.")
        }

        throw errorTypes.OperationNotSupported.withDetail(this.ERROR_MSG_NOT_IMPLEMENTED_YET)
        await next()
    }


    // Spec 6.14.3.1
    // Binding for spec 5.6.7
    http_6_14_3_1_POST_batchEntityCreation = async (ctx: any, next: any) => {

        if (this.getUser(auth(ctx.request)) == null) {
            throw errorTypes.BadRequestData.withDetail("Operation not allowed with the provided user credentials.")
        }

        const contextUrl = this.resolveRequestJsonLdContext(ctx.request)


        const result = await this.broker.api_5_6_7_batchEntityCreation(ctx.request.rawBody, contextUrl)

        if (result.errors.length == 0) {
            ctx.status = 201
            ctx.body = result.success
        }
        else {
            ctx.status = 207
            ctx.body = result
        }

        await next()
    }


    // Spec 6.15.3.1
    // Binding for spec 5.6.8
    http_6_15_3_1_POST_batchEntityUpsert = async (ctx: any, next: any) => {

        if (this.getUser(auth(ctx.request)) == null) {
            throw errorTypes.BadRequestData.withDetail("Operation not allowed with the provided user credentials.")
        }

        const contextUrl = this.resolveRequestJsonLdContext(ctx.request)

        // ATTENTION: Apparently, "options" is NOT a comma separated list here, but just *one* string
        // which can either be "replace" or "update"
        let options = ctx.request.query.options

        if (options != "update") {
            options = "replace"
        }



        const result = await this.broker.api_5_6_8_batchEntityUpsert(ctx.request.rawBody, options, contextUrl)


        if (result.errors.length == 0) {

            if (result.success.length == 0) {
                ctx.status = 204
            }
            else {
                ctx.status = 201
                // Response body is the list of IDs of created entities:
                ctx.body = result.success
            }
        }
        else {
            ctx.status = 207
            // Response body is the entire BatchOperationResult:
            ctx.body = result
        }

        await next()
    }


    // Spec 6.16.3.1
    // Binding for spec 5.6.9
    http_6_16_3_1_POST_batchEntityUpdate = async (ctx: any, next: any) => {

        if (this.getUser(auth(ctx.request)) == null) {
            throw errorTypes.BadRequestData.withDetail("Operation not allowed with the provided user credentials.")
        }

        const contextUrl = this.resolveRequestJsonLdContext(ctx.request)

        const overwrite = (ctx.request.query.options != "noOverwrite")

        const result = await this.broker.api_5_6_9_batchEntityUpdate(ctx.request.rawBody, contextUrl, overwrite)

        if (result.errors.length == 0) {
            ctx.status = 204
            ctx.body = result.success
        }
        else {
            ctx.status = 207
            ctx.body = result
        }

        await next()
    }


    // Spec 6.17.3.1
    // Binding for spec 5.6.10
    http_6_17_3_1_POST_batchEntityDelete = async (ctx: any, next: any) => {

        if (this.getUser(auth(ctx.request)) == null) {
            throw errorTypes.BadRequestData.withDetail("Operation not allowed with the provided user credentials.")
        }

        const result = await this.broker.api_5_6_10_batchEntityDelete(ctx.request.rawBody)

        if (result.errors.length == 0) {
            ctx.status = 204
        }
        else {
            ctx.status = 207
            ctx.body = result
        }

        await next()
    }


    // Spec 6.18.3.1
    // Binding for spec 5.6.11
    http_6_18_3_1_POST_createOrUpdateTemporalEntity = async (ctx: any, next: any) => {

        if (this.getUser(auth(ctx.request)) == null) {
            throw errorTypes.BadRequestData.withDetail("Operation not allowed with the provided user credentials.")
        }

        const contextUrl = this.resolveRequestJsonLdContext(ctx.request)

        // TODO 2: If entity is newly created, return HTTP location header

        ctx.status = await this.broker.api_5_6_11_createOrUpdateTemporalEntity(ctx.request.rawBody, contextUrl)

        await next()
    }


    // Spec 6.18.3.2
    // Binding for spec 5.7.4
    http_6_18_3_2_GET_queryTemporalEntities = async (ctx: any, next: any) => {

        // TODO: Share code here with non-temporal query method

        const contextUrl = this.resolveRequestJsonLdContext(ctx.request) as string

        const options = (typeof (ctx.request.query.options) == "string") ? (ctx.request.query.options as string).split(",") : []

        //############## BEGIN Build EntityInfo array ###############
        const entityIds = (typeof (ctx.request.query.id) == "string") ? (ctx.request.query.id as string).split(",") : []
        const entityTypes = (typeof (ctx.request.query.type) == "string") ? (ctx.request.query.type as string).split(",") : []
        const idPattern = (typeof (ctx.request.query.idPattern) == "string") ? ctx.request.query.idPattern : undefined

        const entities = Array<EntityInfo>()

        for (const id of entityIds) {
            entities.push(new EntityInfo(id, undefined, undefined))
        }

        for (const type of entityTypes) {
            entities.push(new EntityInfo(undefined, undefined, type))
        }

        if (idPattern != undefined) {
            entities.push(new EntityInfo(undefined, idPattern, undefined))
        }
        //############## END Build EntityInfo array ###############


        // ############### BEGIN Create GeoQuery object ##############
        const georel = ctx.request.query.georel
        const geometry = ctx.request.query.geometry
        const geoproperty = ctx.request.query.geoproperty

        let coordinates = undefined

        if (ctx.request.query.coordinates) {
            try {
                coordinates = JSON.parse(ctx.request.query.coordinates)
            }
            catch (e) {
                throw errorTypes.InvalidRequest.withDetail("Invalid geo query: 'coordinates' is not a JSON array.")
            }
        }


        let geoQ = undefined

        // If at least one of the geo query parameters are provided, we make this a geo query
        // by creating a GeoQuery object, even if the other parameters are undefined. 
        // Later in the PsqlBackend, the GeoQuery object will validated:

        if (geometry || coordinates || georel || geoproperty) {
            geoQ = new GeoQuery(geometry, coordinates, georel, geoproperty)
        }
        // ############### END Create GeoQuery object ##############


        const attrs = (typeof (ctx.request.query.attrs) == "string") ? (ctx.request.query.attrs as string).split(",") : []

        const q = ctx.request.query.q
        const csf = ctx.request.query.csf

        // NOTE: The parameters 'geometryProperty' and 'datasetId' are defined in spec 6.3.15:
        const geometryProperty = ctx.request.query.geometryProperty
        const datasetId = ctx.request.query.datasetId


        //############### BEGIN Create temporal query ################
        let temporalQ = undefined

        const timerel = ctx.request.query.timerel
        const timeAt = ctx.request.query.timeAt
        const endTimeAt = ctx.request.query.endTimeAt
        const timeproperty = ctx.request.query.timeproperty
        const lastN = parseFloat(ctx.request.query.lastN)


        temporalQ = new TemporalQuery(timerel, timeAt, endTimeAt, timeproperty, lastN)

        //############### END Create temporal query ################


        // Create main Query object:        
        const query = new Query(entities, attrs, q, geoQ, csf, temporalQ, geometryProperty, datasetId, options)

        // Perform query:
        ctx.body = await this.broker.api_5_7_4_queryTemporalEntities(query, contextUrl)

        await next()
    }


    // Spec 6.19.3.1
    // Binding for spec 5.7.3
    http_6_19_3_1_GET_retrieveTemporalEntity = async (ctx: any, next: any) => {

        const contextUrl = this.resolveRequestJsonLdContext(ctx.request) as string

        const options = (typeof (ctx.request.query.options) == "string") ? (ctx.request.query.options as string).split(",") : []

        const attrs = (ctx.request.query.attrs) ? ctx.request.query.attrs.split(",") : undefined

        // NOTE: The parameters 'geometryProperty' and 'datasetId' are defined in spec 6.3.15:
        const geometryProperty = ctx.request.query.geometryProperty
        const datasetId = ctx.request.query.datasetId

        const timerel = ctx.request.query.timerel
        const timeAt = ctx.request.query.timeAt
        const endTimeAt = ctx.request.query.endTimeAt
        const timeproperty = ctx.request.query.timeproperty
        const lastN = parseFloat(ctx.request.query.lastN)

        let temporalQ = undefined

        if (timerel || timeAt || endTimeAt) {

        }

        temporalQ = new TemporalQuery(timerel, timeAt, endTimeAt, timeproperty, lastN)


        // TODO: 2 Pass GeoJSON parameters

        ctx.body = await this.broker.api_5_7_3_retrieveTemporalEntity(ctx.params.entityId, attrs, temporalQ, contextUrl, options)
        ctx.status = 200

        await next()
    }


    // Spec 6.19.3.2
    // Binding for spec 5.6.16
    http_6_19_3_2_DELETE_deleteTemporalEntity = async (ctx: any, next: any) => {

        if (this.getUser(auth(ctx.request)) == null) {
            throw errorTypes.BadRequestData.withDetail("Operation not allowed with the provided user credentials.")
        }

        await this.broker.api_5_6_16_deleteTemporalEntity(ctx.params.entityId)

        ctx.status = 204
        await next()
    }


    // Spec 6.20.3.1
    // Binding for spec 5.6.12
    http_6_20_3_1_POST_addAttributesToTemporalEntity = async (ctx: any, next: any) => {

        if (this.getUser(auth(ctx.request)) == null) {
            throw errorTypes.BadRequestData.withDetail("Operation not allowed with the provided user credentials.")
        }

        const contextUrl = this.resolveRequestJsonLdContext(ctx.request)

        await this.broker.api_5_6_12_addAttributesToTemporalEntity(ctx.params.entityId, ctx.request.rawBody, contextUrl)
        ctx.status = 204

        await next()
    }


    // Spec 6.21.3.1
    // Binding for spec 5.6.13
    http_6_21_3_1_DELETE_deleteAttributeFromTemporalEntity = async (ctx: any, next: any) => {

        if (this.getUser(auth(ctx.request)) == null) {
            throw errorTypes.BadRequestData.withDetail("Operation not allowed with the provided user credentials.")
        }

        const contextUrl = this.resolveRequestJsonLdContext(ctx.request) as string

        const deleteAll = (ctx.request.query.deleteAll == "true")

        await this.broker.api_5_6_13_deleteAttributeFromTemporalEntity(ctx.params.entityId, ctx.params.attrId, ctx.request.query.datasetId, contextUrl, deleteAll)
        ctx.status = 204

        await next()
    }


    // Spec 6.22.3.1
    // Binding for spec 5.6.14
    http_6_22_3_1_PATCH_modifyAttributeInstanceOfTemporalEntity = async (ctx: any, next: any) => {
       
        if (this.getUser(auth(ctx.request)) == null) {
            throw errorTypes.BadRequestData.withDetail("Operation not allowed with the provided user credentials.")
        }

        const contextUrl = this.resolveRequestJsonLdContext(ctx.request)

        await this.broker.api_5_6_14_updateAttributeInstanceOfTemporalEntity(ctx.params.entityId, ctx.params.attrId, ctx.params.instanceId, ctx.request.rawBody, contextUrl)
        ctx.status = 204

        await next()
    }


    // Spec 6.22.3.2
    // Binding for spec 5.6.15
    http_6_22_3_2_DELETE_deleteAttributeInstanceOfTemporalEntity = async (ctx: any, next: any) => {

        if (this.getUser(auth(ctx.request)) == null) {
            throw errorTypes.BadRequestData.withDetail("Operation not allowed with the provided user credentials.")
        }

        const contextUrl = this.resolveRequestJsonLdContext(ctx.request) as string

        await this.broker.api_5_6_15_deleteAttributeInstanceOfTemporalEntity(ctx.params.entityId, ctx.params.attrId, ctx.params.instanceId, contextUrl)
        ctx.status = 204

        await next()
    }


    // Spec 6.23.3.1
    // Binding for spec 5.7.2
    http_6_23_3_1_POST_entityOperationsQuery = async (ctx: any, next: any) => {

        // NOTE: This is the POST version of the "query entities" operation.

        //############### BEGIN Try to create Query object from request payload ###############
        let query = undefined

        try {
            query = JSON.parse(ctx.request.rawBody) as Query
        }
        catch (e) {
            throw errorTypes.InvalidRequest.withDetail("Request payload is not a valid JSON string.")
        }
        //############### END Try to create Query object from request payload ###############

        // NOTE: We need to set geoQ.location if it is not defined because the GeoQuery object
        // is created by the JSON.parse() method and not through the GeoQuery constructor which
        // would automatically set the value to "location" if it is undefined:

        if (query.geoQ && query.geoQ.geoproperty == undefined) {
            query.geoQ.geoproperty = "location"
        }


        if (ctx.request.headers["accept"] == "application/geo+json" && query.geometryProperty == undefined) {
            query.geometryProperty = "location"
        }


        //##################### BEGIN Resolve context ##################
        let contextUrl = this.resolveRequestJsonLdContext(ctx.request)

        if (ctx.request.headers["content-type"] == "application/ld+json") {
            contextUrl = (query as any)['@context']
        }

        if (contextUrl == undefined) {
            contextUrl = ""
        }
        //##################### END Resolve context ##################

        ctx.body = await this.broker.api_5_7_2_queryEntities(query, contextUrl)
        ctx.status = 200
        await next()
    }


    // Spec 6.24.3.1
    // Binding for spec 5.7.4
    http_6_24_3_1_POST_temporalEntityOperationsQuery = async (ctx: any, next: any) => {

        // NOTE: This is the POST version of the "query temporal entities" operation.

        //############### BEGIN Try to create Query object from request payload ###############
        let query = undefined

        try {
            query = JSON.parse(ctx.request.rawBody) as Query
        }
        catch (e) {
            throw errorTypes.InvalidRequest.withDetail("Request payload is not a valid JSON string.")
        }
        //############### END Try to create Query object from request payload ###############

        // NOTE: We need to set geoQ.location if it is not defined because the GeoQuery object
        // is created by the JSON.parse() method and not through the GeoQuery constructor which
        // would automatically set the value to "location" if it is undefined:

        if (query.geoQ && query.geoQ.geoproperty == undefined) {
            query.geoQ.geoproperty = "location"
        }


        //##################### BEGIN Resolve context ##################


        let contextUrl = this.resolveRequestJsonLdContext(ctx.request)

        if (ctx.request.headers["content-type"] == "application/ld+json") {
            contextUrl = (query as any)['@context']
        }

        if (contextUrl == undefined) {
            contextUrl = ""
        }
        //##################### END Resolve context ##################

        ctx.body = await this.broker.api_5_7_4_queryTemporalEntities(query, contextUrl)
        ctx.status = 200
        await next()
    }


    // Spec 6.25.3.1
    // Binding for spec 5.7.5 and 5.7.6
    http_6_25_3_1_GET_retrieveAvailableEntityTypes = async (ctx: any, next: any) => {

        if (ctx.request.query.details == "true") {
            ctx.body = await this.broker.api_5_7_5_retrieveAvailableEntityTypes()
        }
        else {
            ctx.body = await this.broker.api_5_7_6_retrieveAvailableEntityTypeDetails()
        }

        ctx.status = 200

        await next()
    }


    // Spec 6.26.3.1
    // Binding for spec 5.7.7
    http_6_26_3_1_GET_retrieveAvailableEntityTypeInformation = async (ctx: any, next: any) => {

        ctx.body = await this.broker.api_5_7_7_retrieveAvailableEntityTypeInformation(ctx.params.type)
        ctx.status = 200

        await next()
    }


    // Spec 6.27.3.1
    // Binding for spec 5.7.8 and 5.7.9
    http_6_27_3_1_GET_retrieveAvailableAttributes = async (ctx: any, next: any) => {

        if (ctx.request.query.details == "true") {
            ctx.body = await this.broker.api_5_7_8_retrieveAvailableAttributes()
        }
        else {
            ctx.body = await this.broker.api_5_7_9_retrieveAvailableAttributeDetails()
        }

        ctx.status = 200

        await next()
    }


    // Spec 6.28.3.1
    // Binding for spec 5.7.10
    http_6_28_3_1_GET_retrieveAvailableAttributeInformation = async (ctx: any, next: any) => {

        const contextUrl = this.resolveRequestJsonLdContext(ctx.request)

        ctx.body = await this.broker.api_5_7_10_retrieveAvailableAttributeInformation(ctx.params.attrId, contextUrl)
        ctx.status = 200

        await next()
    }


    http_inofficial_DELETE_deleteAllEntities = async (ctx: any, next: any) => {

        if (this.getUser(auth(ctx.request)) == null) {
            throw errorTypes.BadRequestData.withDetail("Operation not allowed with the provided user credentials.")
        }


        ctx.body = await this.broker.inofficial_deleteAllEntities()

        await next()
    }


    http_inofficial_POST_temporalEntityOperationsUpsert = async (ctx: any, next: any) => {

        if (this.getUser(auth(ctx.request)) == null) {
            throw errorTypes.BadRequestData.withDetail("Operation not allowed with the provided user credentials.")
        }

        const contextUrl = this.resolveRequestJsonLdContext(ctx.request)

        ctx.body = await this.broker.inofficial_temporalEntityOperationsUpsert(ctx.request.rawBody, contextUrl)

        await next()
    }


    async init() {

        const config_string = fs.readFileSync("./cassiopeia_config.json").toString()

        this.config = JSON.parse(config_string)

        const ngsiLdCoreContext = await getNormalizedContext([NGSI_LD_CORE_CONTEXT_URL])

        const psql = new PsqlBackend(this.config, ngsiLdCoreContext)

        this.broker = new ContextBroker(psql)

    

        this.setUpRoutes()

        //############## BEGIN Catch exceptions and return ProblemDetails ################
        if (this.catchExceptions) {
            this.app.use(async (ctx, next) => {
                try {
                    await next();
                } catch (error) {

                    ctx.status = error.status || 500;

                    // NOTE: Checking for error.noBody assumes that error is a ProblemDetail object.
                    if (!error.noBody) {
                        ctx.body = error;
                    }
                }
            });
        }
        //############## END Catch exceptions and return ProblemDetails ################

        const bodyParserConfig: kbodyparser.Options = {
            enableTypes: ['json', 'text'],
            extendTypes: { 'json': ['application/ld+json'] },
            jsonLimit: "100mb"
        }


        this.app.use(klogger())
        this.app.use(kbodyparser(bodyParserConfig))

        //############# BEGIN Set CORS Headers ###############        
        this.app.use(async (ctx, next) => {
            ctx.set('Access-Control-Allow-Origin', '*');
            ctx.set('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept');
            ctx.set('Access-Control-Allow-Methods', 'POST, GET, PUT, DELETE, PATCH');
            await next();
        });
        //############# END Set CORS Headers ###############

        this.app.use(this.router.routes()).use(this.router.allowedMethods())


        //################ BEGIN Enable response payload compression ################
        if (this.config.compressOutput) {
  
            this.app.use(compress({
                threshold: 2048,
            }))
        }
        //################ END Enable response payload compression ################

        // Start broker:
        this.app.listen(this.config.port, () => {
            console.log("Cassiopeia NGSI-LD Context Broker started. Listening on port " + this.config.port + ".")
            console.log("NGSI-LD version 1.3.1. (partial implementation)")
        })
    }


    setUpRoutes() {

        //##################### BEGIN "entities" endpoints #############################
        {
            this.router.post(this.apiBase + "entities/", this.http_6_4_3_1_POST_createEntity)
            this.router.get(this.apiBase + "entities/", this.http_6_4_3_2_GET_queryEntities)

            // Inofficial:
            this.router.delete(this.apiBase + "entities/", this.http_inofficial_DELETE_deleteAllEntities)

            this.router.get(this.apiBase + "entities/:entityId", this.http_6_5_3_1_GET_retrieveEntity)
            this.router.delete(this.apiBase + "entities/:entityId", this.http_6_5_3_2_DELETE_deleteEntity)

            this.router.post(this.apiBase + "entities/:entityId/attrs/", this.http_6_6_3_1_POST_appendEntityAttributes)
            this.router.patch(this.apiBase + "entities/:entityId/attrs/", this.http_6_6_3_2_PATCH_updateEntityAttributes)

            this.router.patch(this.apiBase + "entities/:entityId/attrs/:attrId", this.http_6_7_3_1_PATCH_partialAttributeUpdate)
            this.router.delete(this.apiBase + "entities/:entityId/attrs/:attrId", this.http_6_7_3_2_DELETE_deleteEntityAttribute)
        }
        //##################### END "entities" endpoints #############################

        

        //################ BEGIN "csourceRegistrations" endpoints #######################
        {
            this.router.post(this.apiBase + "csourceRegistrations/:registrationId", this.http_6_8_3_1_registerContextSource)
            this.router.get(this.apiBase + "csourceRegistrations/:registrationId", this.http_6_8_3_2_queryContextSourceRegistrations)
            this.router.get(this.apiBase + "csourceRegistrations/:registrationId", this.http_6_9_3_1_retrieveContextSourceRegistration)
            this.router.patch(this.apiBase + "csourceRegistrations/:registrationId", this.http_6_9_3_2_updateContextSourceRegistration)
            this.router.delete(this.apiBase + "csourceRegistrations/:registrationId", this.http_6_9_3_3_deleteContextSourceRegistration)
        }
        //################ END "csourceRegistrations" endpoints #######################



        //################ BEGIN "subscriptions" endpoints #######################
        {
            this.router.post(this.apiBase + "subscriptions/", this.http_6_10_3_1_createSubscription)
            this.router.get(this.apiBase + "subscriptions/", this.http_6_10_3_2_querySubscriptions)
        }
        //################ END "subscriptions" endpoints #######################



        //################ BEGIN "entityOperations" endpoints #######################
        {
            this.router.post(this.apiBase + "entityOperations/create", this.http_6_14_3_1_POST_batchEntityCreation)
            this.router.post(this.apiBase + "entityOperations/upsert", this.http_6_15_3_1_POST_batchEntityUpsert)
            this.router.post(this.apiBase + "entityOperations/update", this.http_6_16_3_1_POST_batchEntityUpdate)
            this.router.post(this.apiBase + "entityOperations/delete", this.http_6_17_3_1_POST_batchEntityDelete)
        }
        //################ END "entityOperations" endpoints #######################



        // ##################### BEGIN Temporal endpoints ######################
        {
            this.router.post(this.apiBase + "temporal/entities/", this.http_6_18_3_1_POST_createOrUpdateTemporalEntity)
            this.router.get(this.apiBase + "temporal/entities/", this.http_6_18_3_2_GET_queryTemporalEntities)

            this.router.get(this.apiBase + "temporal/entities/:entityId", this.http_6_19_3_1_GET_retrieveTemporalEntity)
            this.router.delete(this.apiBase + "temporal/entities/:entityId", this.http_6_19_3_2_DELETE_deleteTemporalEntity)

            this.router.post(this.apiBase + "temporal/entities/:entityId/attrs/", this.http_6_20_3_1_POST_addAttributesToTemporalEntity)

            this.router.delete(this.apiBase + "temporal/entities/:entityId/attrs/:attrId", this.http_6_21_3_1_DELETE_deleteAttributeFromTemporalEntity)

            this.router.patch(this.apiBase + "temporal/entities/:entityId/attrs/:attrId/:instanceId", this.http_6_22_3_1_PATCH_modifyAttributeInstanceOfTemporalEntity)
            this.router.delete(this.apiBase + "temporal/entities/:entityId/attrs/:attrId/:instanceId", this.http_6_22_3_2_DELETE_deleteAttributeInstanceOfTemporalEntity)
        }
        // ##################### END Temporal endpoints ######################



        //#################### BEGIN entityOperations query endpoints ##################
        {
            this.router.post(this.apiBase + "entityOperations/query", this.http_6_23_3_1_POST_entityOperationsQuery)
            this.router.post(this.apiBase + "temporal/entityOperations/query", this.http_6_24_3_1_POST_temporalEntityOperationsQuery)
            // ATTENTION: The following endpoint is not officially part of the NGSI-LD specification:
            this.router.post(this.apiBase + "temporal/entityOperations/upsert", this.http_inofficial_POST_temporalEntityOperationsUpsert)
        }
        //#################### END entityOperations query endpoints ##################



        //################ BEGIN Entity type information endpoints #######################
        {
            this.router.get(this.apiBase + "types/", this.http_6_25_3_1_GET_retrieveAvailableEntityTypes)
            this.router.get(this.apiBase + "types/:type", this.http_6_26_3_1_GET_retrieveAvailableEntityTypeInformation)
        }
        //################ END Entity type information endpoints #######################



        //################ BEGIN Attribute type information endpoints #######################
        {
            this.router.get(this.apiBase + "attributes/", this.http_6_27_3_1_GET_retrieveAvailableAttributes)
            this.router.get(this.apiBase + "attributes/:attrId", this.http_6_28_3_1_GET_retrieveAvailableAttributeInformation)
        }
        //################ END Attribute type information endpoints #######################
    }


    resolveRequestJsonLdContext(request: any): string | undefined {


        // Spec 6.3.5:

        // According to spec 6.3.5, the context URL should only be extracted from the header
        // if

        // a) the HTTP method is GET or DELETE or
        // b) the HTTP method is POST or PATCH and the Content-Type header is "application/json".

        // If the HTTP method ist POST or PATCH and the Content-Type header is "application/ld+json", 
        // then the context should be extracted from the payload JSON.

        // NOTE: We do not add the implicit default NGSI-LD core context here. This happens in the method
        // "appendCoreContext()" of the "jsonld" module, which is called by the API methods of the ContextBroker class.

        let result = undefined

        const rm = request.method
        let contentType = request.headers["content-type"]
        const linkHeader = request.headers["link"]

        let linkHeaderContext = undefined

        if (linkHeader != undefined) {

            const pieces = linkHeader.split(";")

            linkHeaderContext = pieces[0].substr(1, pieces[0].length - 2)
        }


        if (rm == "GET" || rm == "DELETE") {
            result = (linkHeaderContext != undefined) ? linkHeaderContext : NGSI_LD_CORE_CONTEXT_URL
            result = linkHeaderContext
        }
        else if (rm == "POST" || rm == "PATCH") {

            if (rm == "PATCH" && contentType == "application/merge-patch+json") {
                contentType = "application/json"
            }

            if (contentType == "application/json") {

                result = (linkHeaderContext != undefined) ? linkHeaderContext : NGSI_LD_CORE_CONTEXT_URL
                result = linkHeaderContext

                // TODO: If the request payload body (as JSON) contains a "@context" term, then an HTTP error response of type BadRequestData shall be raised.
            }
            else if (contentType == "application/ld+json") {

                // NOTE: If the context needs to be extracted from the payload, 
                // this is done in the respective API method of the ContextBroker class

                if (linkHeaderContext != undefined) {
                    throw errorTypes.BadRequestData.withDetail("Requests with Content-Type:application/ld+json must not contain a JSON-LD link header")
                }
            }
            else {
                // Spec 6.3.4:
                const error = errorTypes.BadRequestData.withDetail("Invalid request content-type: " + contentType)
                error.noBody = true
                error.status = 415

                throw error
            }
        }
        else {
            throw errorTypes.InvalidRequest.withDetail("Invalid request method: " + rm)
        }

        return result
    }


    getUser(credentials: any): any {

        if (credentials == undefined || credentials == null) {
            throw errorTypes.BadRequestData.withDetail("No user credentials provided")
        }

        for (const user of this.config.users) {
            if (user.username == credentials.name && user.password == credentials.pass) {
                return user
            }
        }

        return null
    }
}
