// Spec 5.5.4

import { GeoQuery } from "./dataTypes/GeoQuery"
import { Query } from "./dataTypes/Query"


export const attributeTypes = ["https://uri.etsi.org/ngsi-ld/Property", "https://uri.etsi.org/ngsi-ld/GeoProperty", "https://uri.etsi.org/ngsi-ld/Relationship"]

// These are all members that are common for Property, GeoProperty and Relationship:
const defaultAttributeMembers = ["@type", "https://uri.etsi.org/ngsi-ld/createdAt", "https://uri.etsi.org/ngsi-ld/modifiedAt", "https://uri.etsi.org/ngsi-ld/observedAt", 'https://uri.etsi.org/ngsi-ld/datasetId', 'https://uri.etsi.org/ngsi-ld/instanceId']

// Additional special members for Property:
const defaultPropertyMembers = ["https://uri.etsi.org/ngsi-ld/hasValue", "https://uri.etsi.org/ngsi-ld/unitCode"]

// Additional special members for GeoProperty:
const defaultGeoPropertyMembers = ["https://uri.etsi.org/ngsi-ld/hasValue"]

// Additional special members for Relationship:
const defaultRelationshipMembers = ["https://uri.etsi.org/ngsi-ld/hasObject"]

const defaultEntityMembers = ["@type", "@id", "@context", "https://uri.etsi.org/ngsi-ld/createdAt", "https://uri.etsi.org/ngsi-ld/modifiedAt"]

// RegExp for datetime with fixed UTC time zone, e.g. "2018-08-07T12:00:00Z"
// As specified by spec 4.6.3
// Source: https://www.oreilly.com/library/view/regular-expressions-cookbook/9781449327453/ch04s07.html
const regexp_dateTimeUtc = "^(-?(?:[1-9][0-9]*)?[0-9]{4})-(1[0-2]|0[1-9])-(3[01]|0[1-9]|[12][0-9])T(2[0-3]|[01][0-9]):([0-5][0-9]):([0-5][0-9])(\.[0-9]+)?Z"

const regexp_timeUtc = "^(2[0-3]|[01][0-9]):([0-5][0-9]):([0-5][0-9])(\.[0-9]+)?Z"


// These members of Entity are expected to be GeoProperties:
// TODO: 4 Replace this with type check using the context definition?
const defaultEntityGeoProperties = ["location", "observationSpace", "operationSpace"]


const geometryTypes_expanded = ['https://purl.org/geojson/vocab#Point',
    'https://purl.org/geojson/vocab#MultiPoint',
    'https://purl.org/geojson/vocab#LineString',
    'https://purl.org/geojson/vocab#MultiLineString',
    'https://purl.org/geojson/vocab#Polygon',
    'https://purl.org/geojson/vocab#MultiPolygon']


export const geometryTypes_compacted = ['Point',
    'MultiPoint',
    'LineString',
    'MultiLineString',
    'Polygon',
    'MultiPolygon']

// TODO: 3 Frage: Expect expanded or compated from for GeoQuery geometry types?

const invalidCharacters = ['<', '>', '"', "'", '=', ';', '(', ')']



export function checkDatasetIdUniqueness(attribute: any, key: string): Array<string> {

    let result = Array<string>()

    const existingDatasetIds = Array<string | undefined>()

    if (!(attribute instanceof Array)) {
        attribute = [attribute]
    }

    for (const instance of attribute) {

        if (existingDatasetIds.includes(instance['https://uri.etsi.org/ngsi-ld/datasetId'])) {
            result.push(key + ": Multiple attribute instances with same dataset ID: " + instance['https://uri.etsi.org/ngsi-ld/datasetId'])
        }
        else {
            existingDatasetIds.push(instance['https://uri.etsi.org/ngsi-ld/datasetId'])
        }

    }

    return result
}


function checkInstanceMembers(instance: any, exclude: Array<string>): Array<string> {

    let result = Array<string>()

    for (const [key, value] of Object.entries(instance)) {

        if (!defaultAttributeMembers.includes(key) && !exclude.includes(key)) {
            result = result.concat(checkReifiedAttribute(instance[key], key, undefined, false))
        }
    }

    return result
}


// Spec 4.6.4
export function checkInvalidCharacters(value: any): Array<string> {

    let result = Array<string>()

    if (typeof (value) == "string") {

        for (const char of invalidCharacters) {
            if (value.includes(char)) {
                result.push(`Key or value string '${value}' contains invalid character: '${char}'`)
            }
        }
    }
    else {
        for (const key in value) {
            result = result.concat(checkInvalidCharacters(key))
            result = result.concat(checkInvalidCharacters(value[key]))
        }
    }

    return result
}




export function checkArrayOfEntities(entities: Array<any>, checkNonZeroLength: boolean, expectUniqueDatasetIds: boolean): Array<string> {

    let result = Array<string>()

    if (!(entities instanceof Array)) {
        result.push("Type check for 'Array of Entities' failed: Passed data is not an array")
        return result
    }

    if (checkNonZeroLength && entities.length == 0) {
        result.push("Type check for 'Array of Entities' failed.", "Passed array contains zero elements.")
        return result
    }

    for (const entity of entities) {

        if (entity == null) {
            result.push("Array of Entities contains 'null' element")
        }

        result = result.concat(checkEntity(entity, expectUniqueDatasetIds))
    }

    return result
}


export function checkArrayOfUris(uris: Array<string>, checkNonZeroLength: boolean): Array<string> {

    const result = Array<string>()

    if (!(uris instanceof Array)) {
        result.push("Type check for 'Array of URIs' failed.", "Passed data is not an array.")
        return result
    }

    if (checkNonZeroLength && uris.length == 0) {
        result.push("Type check for 'Array of URIs' failed.", "Passed array contains zero elements.")
        return result
    }


    for (const uri of uris) {

        if (uri == null) {
            result.push("Type check for 'Array of URIs' failed.", "Passed array contains one or more 'null' elements.")
            return result
        }

        if (!isUri(uri)) {
            result.push("Type check for 'Array of URIs' failed.", "Array contains an element which is not a valid NGSI-LD entity.")
            return result
        }
    }

    return result
}


export function checkReifiedAttribute(attribute: any, key: string, expectedType: string | undefined, expectUniqueDatasetIds: boolean): Array<string> {

    let result = Array<string>()

    // Check attribute name:
    if (!isUri(key)) {
        result.push(`Attribute name '${key}' is not a valid URI.`)
    }



    if (!(attribute instanceof Array)) {
        attribute = [attribute]
    }


    let foundType = undefined

    // ATTENTION: Generally, both "normal" and temporal Attributes can 
    // have multiple instances with the same datasetId.

    // However, when Entities are created or updated through the API, the following rules apply:

    // - If entities are created/updated through the "normal" endpoints (i.e. "/entities/..."),
    //   datasetId should be unique for each instance, including 1 instance with no defined datasetId at most.

    // - If entities are created/updated through the "temporal" endpoints (i.e. "/temporal/..."),
    // - multiple instances can have the same datasetId.

    // Because of this difference, checking of datasetId uniqueness is optional here

    if (expectUniqueDatasetIds) {
        result = result.concat(checkDatasetIdUniqueness(attribute, key))
    }


    //###################### BEGIN Iterate over attribute instances ######################
    for (const instance of attribute) {

        //################# BEGIN Check JS data type of instance object ################
        if (instance == null || instance == undefined) {
            result.push(key + ": Attribute instance is null or undefined")
            continue
        }

        else if (instance instanceof Array) {
            result.push(key + ": Attribute instance is array: " + JSON.stringify(instance))
            continue
        }

        else if (typeof (instance) == "string") {
            result.push(key + ": Attribute instance is string")
            continue
        }

        else if (typeof (instance) == "boolean") {
            result.push(key + ": Attribute instance is boolean")
            continue
        }

        else if (typeof (instance) == "number") {
            result.push(key + ": Attribute instance is number")
            continue
        }
        //################# END Check JSdata type of instance object ################



        //########### BEGIN Check if instance has expected attribute type ##############
        if (expectedType != undefined && instance['@type'] != expectedType) {
            result.push(key + `: Invalid instance type: '${instance['@type']}'`)
        }
        //########### END Check if instance has expected attribute type ##############



        //############ BEGIN Make sure that all instances have the same type #############
        if (foundType == undefined) {
            foundType = instance['@type']
        }

        if (instance['@type'] != foundType) {
            result.push(key + ": Different instance types in one attribute")
        }
        //############ END Make sure that all instances have the same type #############

        const value_expanded = instance['https://uri.etsi.org/ngsi-ld/hasValue']


        //################# BEGIN Check types of common members ####################
        if (instance["https://uri.etsi.org/ngsi-ld/observedAt"] != undefined && !isDateTimeUtcString(instance["https://uri.etsi.org/ngsi-ld/observedAt"])) {
            result.push(key + ": 'observedAt' is not a valid ISO 8601 UTC DateTime string.")
        }

        if (instance['https://uri.etsi.org/ngsi-ld/datasetId'] != undefined && !isUri(instance['https://uri.etsi.org/ngsi-ld/datasetId'])) {
            result.push(key + ": 'datasetId' is not a valid URI.")
        }
        //################# END Check types of common members ####################



        //################### BEGIN Perform Property-type-specific checks #################

        // TODO: 2 Exception to the "no null value" rule: Fragment with order to delete an instance!

        if (instance['@type'] == "https://uri.etsi.org/ngsi-ld/Property") {

            if (value_expanded == undefined || value_expanded == null) {
                result.push(key + ": Attribute instance value is null or undefined.")
            }

            if (instance.unitCode != undefined && typeof (instance.unitCode) != "string") {
                result.push(key + ": 'unitCode' not a string.")
            }

            result = result.concat(checkInstanceMembers(instance, defaultPropertyMembers))
        }
        else if (instance['@type'] == "https://uri.etsi.org/ngsi-ld/GeoProperty") {

            // Spec 4.5.2
            // Spec 5.2.7

            if (value_expanded == undefined || value_expanded == null) {
                result.push(key + ": Attribute instance value is null or undefined.")
            }
            else {
                // ATTENTION: We disabled this because JSON members with key "value" are no longer expanded
                /*
                if (!geometryTypes_expanded.includes(value_expanded['@type'])) {
                    result.push(key + ": Invalid GeoProperty value type: " + value_expanded['@type'])
                }
                

                // TODO: 4 More strict check of GeoJSON coordinates structure
                if (!(value_expanded["https://purl.org/geojson/vocab#coordinates"] instanceof Array)) {
                    result.push(key + ": Invalid GeoProperty value coordinates: " + JSON.stringify(value_expanded.coordinates))
                }
                */
            }

            result = result.concat(checkInstanceMembers(instance, defaultGeoPropertyMembers))
        }
        else if (instance['@type'] == "https://uri.etsi.org/ngsi-ld/Relationship") {

            if (!isUri(instance["https://uri.etsi.org/ngsi-ld/hasObject"])) {
                result.push(key + ": Relationship 'object' not an URI: " + instance["https://uri.etsi.org/ngsi-ld/hasObject"])
            }

            result = result.concat(checkInstanceMembers(instance, defaultRelationshipMembers))
        }
        else {
            result.push(key + ": Invalid type: '" + instance['@type'] + '"')
        }
        //################### END Perform Property-type-specific checks #################
    }
    //###################### END Iterate over attribute instances ######################

    return result
}


export function checkEntity(entity: any, expectUniqueDatasetIds: boolean): Array<string> {

    let result = Array<string>()

    if (entity == null || entity == undefined) {
        result.push("Entity is null or undefined")
        return result
    }

    // Check entity for invalid characters (spec 4.6.4):

    // TODO: 1 Ask NEC about invalid characters check
    /*
    const invalidCharResult = checkInvalidCharacters(entity)

    if (invalidCharResult.length > 0) {
        return invalidCharResult
    }
    */

    // Spec 4.5.1
    if (!isUri(entity['@id'])) {
        result.push("Entity ID is not a URI")
    }

    if (!isUri(entity['@type'])) {
        result.push("Entity type is not a URI")
    }


    // Spec 4.5.2
    // Spec 5.2.2
    // Spec 5.2.4

    for (const key in entity) {

        const attr = entity[key]

        if (defaultEntityGeoProperties.includes(key)) {
            result = result.concat(checkReifiedAttribute(attr, key, "https://uri.etsi.org/ngsi-ld/GeoProperty", expectUniqueDatasetIds))
        }

        if (!defaultEntityMembers.includes(key)) {
            result = result.concat(checkReifiedAttribute(attr, key, undefined, expectUniqueDatasetIds))
        }
    }

    return result
}


export function checkGeoQuery(geoQuery: GeoQuery): Array<string> {

    let result = Array<string>()

    if (!(geoQuery.coordinates instanceof Array)) {
        result.push("Invalid geo query: 'coordinates' is not an array.")
    }

    // ATTENTION: We check against the compacted geometry type names here.
    // TODO: 5 Is this correct?
    if (!(geometryTypes_compacted.includes(geoQuery.geometry))) {
        result.push(`Invalid geo query: 'geometry' is '${geoQuery.geometry}', but must be one of: 'Point', 'LineString', 'Polygon', 'MultiPoint', 'MultiLineString', 'MultiPolygon'.`)
    }

    if (typeof (geoQuery.georel) != "string") {
        result.push("Invalid geo query: 'georel' is not string .")
    }

    return result
}


export function checkQuery(query: Query): Array<string> {

    let result = Array<string>()

    // TODO: 4 Disabled for testing. Re-enable for release.


    // Spec 5.2.25 (Data Model definition)


    // Spec 5.7.2.4:

    // "It is not possible to retrieve a set of entities by only specifying desired identifiers, 
    // without further specifying restrictions on the entities' types or attributes, 
    // either explicitly, via lists of Entity types or of Attribute names, or implicitly, 
    // within an NGSI-LD query or geo-query.":

    /*
    if ((!(query.entities instanceof Array) || query.entities.length == 0)

        && (!(query.attrs instanceof Array) || query.attrs.length == 0)
        && (typeof (query.q) != "string"

        && query.geoQ == undefined)) {

        result.push("At least one of the parameters 'type', 'attrs', 'q' or 'georel' must be specified.")
    }
    */

    return result
}


export function isReifiedAttribute(attribute: any, attributeName : string): boolean {
    return (checkReifiedAttribute(attribute, attributeName, undefined, false).length == 0)
}


export function isDateTimeUtcString(datetime: string): boolean {

    if (typeof (datetime) != "string") {
        return false
    }

    return (datetime.match(regexp_dateTimeUtc) != null)
}


export function isTimeUtcString(datetime: string): boolean {

    if (typeof (datetime) != "string") {
        return false
    }

    return (datetime.match(regexp_timeUtc) != null)
}



export function isDateString(date: string): boolean {

    if (typeof (date) != "string") {
        return false
    }

    return (date.match(/(\d{4})-(\d{2})-(\d{2})/) != null)
}


// See RFC 3968
// See https://en.wikipedia.org/wiki/Uniform_Resource_Identifier
export function isUri(uri: string): boolean {

    if (typeof (uri) != "string") {
        return false
    }

    if (!uri.includes(":")) {
        return false
    }

    if (uri.includes(" ")) {
        return false
    }

    return true
}