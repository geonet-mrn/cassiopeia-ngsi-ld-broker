// NOTE: Other than the built-in method JSON.parse(), 

import { Feature } from "./dataTypes/Feature"
import { errorTypes } from "./errorTypes"
import { isReifiedAttribute } from "./validate"

// this one does not throw an exception if parsing fails:
export function parseJson(jsonString: string): any {

    try {
        return JSON.parse(jsonString)
    }
    catch (e) {
        return undefined
    }
}


export function compactedEntityToGeoJsonFeature(entity_compacted: any, geometryProperty: string | undefined = "location", datasetId: string | undefined): Feature {

    // TODO: 2 Is it correct to work with compacted entities here? How about uniqueness of the attribute keys?

    const attribute = entity_compacted[geometryProperty]

    // NOTE that we also check against the value "undefined" here,
    // which represents the default instance!
    let instanceToUse: any = undefined

    //######### BEGIN Iterate over instances to find the one with the provided datasetId ########
    for (const instance of attribute) {
        if (instance['datasetId'] == datasetId) {
            instanceToUse = instance
            break
        }
    }
    //######### END Find the GeoProperty instance which has the provided datasetId ########

    if (instanceToUse == undefined) {
        throw errorTypes.ResourceNotFound.withDetail("No GeoProperty attribute instance with the requested dataset ID could be found: '" + datasetId + "'.")
    }

    // ATTENTION: For now, we just put the entire entity as "properties" here.
    // If we strictly follow the spec, "properties" should only contain the entity 
    // type and the attributes (see spec 5.2.31).
    const properties = JSON.parse(JSON.stringify(entity_compacted))

    return new Feature(entity_compacted['@id'], instanceToUse['value'], properties)
}


// Spec 4.5.4:
export function getSimplifiedRepresentation(entity : any) : any {
    // NOTE: This expects an expanded entity

    let result : any = {}

    if (entity["@context"] != undefined) {
        result["@context"] = entity["@context"]
    }

    result["@id"] = entity["@id"]
    result["@type"] = entity["@type"]

    

    for (const attributeId in entity) {

        let attribute = (entity as any)[attributeId]

        if (!isReifiedAttribute(attribute)) {
            result[attributeId] = entity[attributeId]
            continue
        }

        if (attribute.type == "Property" || attribute.type == "GeoProperty") {

            if (entity[attributeId] instanceof Array) {

                if (entity[attributeId].length == 1) {
                    result[attributeId] = entity[attributeId][0].value        
                }
                else {
                    let valuesArray = []

                    for(const instance of entity[attributeId]) {
                        valuesArray.push(instance.value)
                    }

                    result[attributeId] = valuesArray
                }
            }
            else {
                result[attributeId] = entity[attributeId].value
            }
        }
        else if (attribute.type == "Relationship") {
            
            if (entity[attributeId] instanceof Array) {

                if (entity[attributeId].length == 1) {
                    result[attributeId] = entity[attributeId][0].object        
                }
                else {
                    let valuesArray = []

                    for(const instance of entity[attributeId]) {
                        valuesArray.push(instance.object)
                    }

                    result[attributeId] = valuesArray
                }
            }
            else {
                result[attributeId] = entity[attributeId].object
            }
        }
        
    }
    
}