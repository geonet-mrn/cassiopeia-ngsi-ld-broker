import { JsonLdContextNormalized } from "jsonld-context-parser"
import { GeoQuery } from "./dataTypes/GeoQuery"
import { errorTypes } from "./errorTypes"
import { expandObject } from "./jsonld"

import { PsqlTableConfig } from "./PsqlTableConfig"
import { checkGeoQuery } from "./validate"

const spatialQueryFunctions: any = {
    "within": "ST_Within",
    "contains": "ST_Contains",
    "intersects": "ST_Intersects",
    "equals": "ST_Equals",
    "disjoint": "ST_Disjoint",
    "overlaps": "ST_Overlaps"
}



export function makeGeoQueryCondition(geoQuery: GeoQuery, context : JsonLdContextNormalized, tableCfg : PsqlTableConfig, attrTable : string): string {

    //############# BEGIN Validate ###############
    const geoQueryCheck = checkGeoQuery(geoQuery)

    if (geoQueryCheck.length > 0) {
        throw errorTypes.BadRequestData.withDetail(geoQueryCheck.join(". "))
    }
    //############# END Validate #################


    const geoRelParts = geoQuery.georel.split(';')

    const geoFilterFunc = geoRelParts[0]

    //#################### BEGIN Try to parse query coordinates string ##################

    // geoQuery.coordinates can be either a GeoJSON string or a GeoJSON JavaScript object.
    // If it is a *JSON string*, we try to parse it to an object. 
    // If it already is an object, we skip this step.            

    let queryCoordinates = null

    if (typeof (geoQuery.coordinates) == "string") {

        try {
            queryCoordinates = JSON.parse(geoQuery.coordinates)
        }
        catch (e) {
            throw errorTypes.InvalidRequest.withDetail("Invalid geo query: Query geometry is not a valid JSON string.")
        }
    }
    else {
        queryCoordinates = geoQuery.coordinates
    }
    //#################### END Try to parse query coordinates string ##################


    // Build query geometry object:
    const queryGeom = {
        "type": geoQuery.geometry,
        "coordinates": queryCoordinates
    }

    // Parse the query geometry object to a string for inculsion into the SQL query:
    const queryGeomString = JSON.stringify(queryGeom)


    // Make sure that the filter is connected to the specific attribute we want to check:

    const geoProperty_expanded = expandObject(geoQuery.geoproperty, context)

    let result = `(SELECT eid FROM ${attrTable} WHERE ${attrTable}.${tableCfg.COL_ATTR_NAME} = '${geoProperty_expanded}' AND `


    // NOTE: "ST_DWithin" has another call signature than the other spatial query functions.
    // That's why we need to differentiate here:

    if (geoFilterFunc in spatialQueryFunctions) {
        result += `${spatialQueryFunctions[geoFilterFunc]}(${attrTable}.geom, ST_SetSRID(ST_GeomFromGeoJSON('${queryGeomString}'), 4326))`
    }
    else if (geoFilterFunc == "near" && geoRelParts.length == 2) {

        const distancePartsString = geoRelParts[1]

        const distanceParts = distancePartsString.split("==")

        if (distanceParts.length != 2) {
            throw errorTypes.InvalidRequest.withDetail("Invalid 'near' geo query condition string. It must have the form '<maxDistance/minDistance>==<distance in meters>'")
        }

        //############# BEGIN Evaluate 'near' mode (minDistance or maxDistance) ###############
        if (distanceParts[0] == 'maxDistance') {

        }
        else if (distanceParts[0] == 'minDistance') {
            result += "NOT "
        }
        else {
            throw errorTypes.InvalidRequest.withDetail(`Invalid 'near' geo query: Left side is '${distanceParts[0]}', but must be either 'maxDistance' or 'minDistance'.`)
        }
        //############# END Evaluate 'near' mode (minDistance or maxDistance) ###############


        //############### BEGIN Try to parse distance value ##############
        const distance = parseFloat(distanceParts[1])

        if (isNaN(distance)) {
            throw errorTypes.InvalidRequest.withDetail("Invalid 'near' geo query: Right side (distance) is not a number.")
        }
        //############### END Try to parse distance value ##############

        result += `ST_DWithin(${attrTable}.geom::geography, ST_SetSRID(ST_GeomFromGeoJSON('${queryGeomString}'), 4326)::geography, ${distance}, true)`
    }
    else {
        throw errorTypes.InvalidRequest.withDetail("Invalid geo query: 'georel' does not match a supported pattern.")
    }

    result += ")"

    return result
}

