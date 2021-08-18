import * as ldcp from 'jsonld-context-parser'
import * as fs from 'fs'
import axios from 'axios'
import * as md5 from 'md5'
import { errorTypes } from './errorTypes'


export const contextParser = new ldcp.ContextParser()
export const NGSI_LD_CORE_CONTEXT_URL = "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.3.jsonld"

const contextCacheDir = "contextCache/"


export function appendCoreContext(nonNormalizedContext: any): Array<any> {

    let result = nonNormalizedContext

    if (result == null || result == undefined) {
        result = []
    }
    else if (!(result instanceof Array)) {
        result = [result]
    }


    result.push(NGSI_LD_CORE_CONTEXT_URL)


    return result
}


export function compactObject(obj: any, normalizedContext: ldcp.JsonLdContextNormalized): any {

    if (obj === null) {
        return null
    }

    else if (typeof (obj) === 'string') {
        return normalizedContext.compactIri(obj, true)
    }

    else if (typeof (obj) === 'number') {
        return obj
    }

    else if (typeof (obj) === 'boolean') {
        return obj
    }

    else if (obj instanceof Array) {
        const result = []

        for (const item of obj) {
            result.push(compactObject(item, normalizedContext))
        }

        return result
    }
    //############## BEGIN If member is an object ##############
    else if (typeof obj === 'object') {

        // NOTE: Cloning the object is necessary!
        let clone = JSON.parse(JSON.stringify(obj))

        let result: any = {}


        //############ BEGIN Compact values ###########
        for (const key in clone) {
            clone[key] = compactObject(clone[key], normalizedContext)
        }
        //############ END Compact values ###########


        //############ BEGIN Compact keys ###########
        for (const key in clone) {

            const newKey = normalizedContext.compactIri(key, true)

            result[newKey] = clone[key]
        }
        //############ END Compact keys ###########

        return result
    }

    //############## END If member is an object ##############   
}


export function expandObject(obj: any, normalizedContext: ldcp.JsonLdContextNormalized): any {

    if (obj === null) {
        return null
    }

    else if (typeof (obj) === 'string') {
        return normalizedContext.expandTerm(obj, true)
    }

    else if (typeof (obj) === 'number') {
        return obj
    }

    else if (typeof (obj) === 'boolean') {
        return obj
    }

    else if (obj instanceof Array) {
        const result = []

        for (let item of obj) {
            result.push(expandObject(item, normalizedContext))
        }

        return result
    }
    //############## BEGIN If member is an object ##############
    else if (typeof obj === 'object') {

        const result: any = {}

        //############ BEGIN Iterate over all keys ###########
        for (const key in obj) {

            let newKey = normalizedContext.expandTerm(key, true)

            if (newKey == null) {
                // TODO: 1 What to do in this case?
                newKey = key
            }

            result[newKey] = obj[key]
        }
        //############ END Iterate over all keys ###########


        //############ BEGIN Iterate over all keys ###########
        for (const key in result) {

            // ATTENTION: Excluding Property values from expansion IS correct, but hard-coding it this way probably
            // isn't the best solution. We should try to implement this according to the JSON-LD + NGSI-LD specifications
            // and take into account rules defined in context definitions.

            // TODO: 2 Properly decide what is expanded and what not.

//            if (key != "value" && key != "https://uri.etsi.org/ngsi-ld/hasValue") {

                result[key] = expandObject(result[key], normalizedContext)
  //          }
        }
        //############ END Iterate over all keys ###########

        return result
    }

    //############## END If member is an object ##############   
}




export async function getNormalizedContext(nonNormalizedContext: any): Promise<ldcp.JsonLdContextNormalized> {

    const nnc = await httpFetchContexts(nonNormalizedContext)

    return await contextParser.parse(nnc)
}


export async function httpFetchContexts(context: any): Promise<Array<any>> {


    let result: Array<any> = []

    if (!(context instanceof Array)) {
        context = [context]
    }





    for (let entry of context) {

        result.push(getContextForContextArrayEntry(entry))
      
    }

    return result
}


// 'entry' can be either a URL or a local context
export async function getContextForContextArrayEntry(entry: any): Promise<any> {

    let result = undefined

    if (!fs.existsSync(contextCacheDir)) {
        fs.mkdirSync(contextCacheDir);
    }

    if (typeof (entry) == "string" && ((entry.startsWith("https://") || entry.startsWith("http://")))) {

        const url = entry
        const fileName = contextCacheDir + md5(url) + ".jsonld"


        if (!fs.existsSync(fileName)) {

            const response = await axios.get(url).catch((e) => {
                throw errorTypes.LdContextNotAvailable.withDetail("Failed to retrieve context from URL: " + url)
            })

            if (response != undefined) {
                fs.writeFileSync(fileName, JSON.stringify(response.data))
            }
        }

        result = JSON.parse(fs.readFileSync(fileName).toString())
    }
    else if (typeof (entry) == "object") {
        result = entry
    }
    else {
        throw errorTypes.LdContextNotAvailable.withDetail("Invalid context: " + JSON.stringify(entry))
        //console.log("Invalid context entry: " + entry)
    }

    return result
}
