import { Feature } from "./dataTypes/Feature"
import { errorTypes } from "./errorTypes"
import * as validate from "./validate"


// NOTE: Other than the built-in method JSON.parse(), 
// this one does not throw an exception if parsing fails:
export function parseJson(jsonString: string): any {

    try {
        return JSON.parse(jsonString)
    }
    catch (e) {
        return undefined
    }
}



export function compactedEntityToGeoJsonFeature(entity_compacted: any, geometryProperty_compacted: string | undefined = "location", datasetId: string | undefined): Feature {

    let geometry = undefined


    let geometryAttribute = entity_compacted[geometryProperty_compacted]

    if (geometryAttribute != undefined) {

        if (!(geometryAttribute instanceof Array)) {
            geometryAttribute = [geometryAttribute]
        }


        // NOTE that we also check against the value "undefined" here,
        // which represents the default instance!
        let instanceToUse: any = undefined

        //######### BEGIN Iterate over instances to find the one with the provided datasetId ########
        for (const instance of geometryAttribute) {
            if (instance['datasetId'] == datasetId) {
                instanceToUse = instance
                break
            }
        }
        //######### END Find the GeoProperty instance which has the provided datasetId ########

        if (instanceToUse != undefined) {
            //throw errorTypes.ResourceNotFound.withDetail("No GeoProperty attribute instance with the requested dataset ID could be found: '" + datasetId + "'.")

            if (instanceToUse.type == "GeoProperty") {
                geometry = instanceToUse['value']
            }

            // ATTENTION: This is somewhat ugly. We need it to handle simplified entity representations.
            // This should probably be solved in a different way.
            else if (validate.geometryTypes_compacted.includes(instanceToUse.type)) {
                geometry = instanceToUse
            }
        }
    }

    // ATTENTION: For now, we just put the entire entity as "properties" here.
    // If we strictly follow the spec, "properties" should only contain the entity 
    // type and the attributes (see spec 5.2.31).
    const properties = JSON.parse(JSON.stringify(entity_compacted))

    return new Feature(entity_compacted['@id'], geometry, properties)
}


export function unpackGeoPropertyStringValues(entity_expanded : any) {

    let result = JSON.parse(JSON.stringify(entity_expanded))

    for(const key in result) {
        let attribute_expanded = result[key]        

        
        if (!validate.isReifiedAttribute(attribute_expanded,key)) {
            continue
        }


        if (!(attribute_expanded instanceof Array)) {
            attribute_expanded = [attribute_expanded]
        }


        for(const instance_expanded of attribute_expanded) {
            if (instance_expanded["@type"] != "https://uri.etsi.org/ngsi-ld/GeoProperty") {
                continue
            }

            const value = instance_expanded["https://uri.etsi.org/ngsi-ld/hasValue"]
            
            if (typeof value == "string") {
                const value_unpacked = parseJson(value)

                if (value_unpacked == undefined) {
                    throw errorTypes.BadRequestData.withDetail("The value of GeoProperty " + key + " is not a valid GeoJSON string")
                }
                else {
                    instance_expanded["https://uri.etsi.org/ngsi-ld/hasValue"] = value_unpacked
                }
            }
        }
    }

    return result
}


export function simplifyEntity(entity: any): any {

    let result: any = {}

    for (const key in entity) {

        let attribute = entity[key]

        let simplifiedValues = []

        if (attribute instanceof Array) {
            for (let instance of attribute) {

                if (instance["@type"] == "https://uri.etsi.org/ngsi-ld/Property" || instance["@type"] == "https://uri.etsi.org/ngsi-ld/GeoProperty") {
                    simplifiedValues.push(instance["https://uri.etsi.org/ngsi-ld/hasValue"])
                }
                else if (instance["@type"] == "https://uri.etsi.org/ngsi-ld/Relationship") {
                    simplifiedValues.push(instance["https://uri.etsi.org/ngsi-ld/hasObject"])
                }
            }

            if (simplifiedValues.length > 1) {
                result[key] = simplifiedValues
            }
            else if (simplifiedValues.length == 1) {
                result[key] = simplifiedValues[0]
            }
        }

        else {
            result[key] = attribute
        }
    }

    return result
}