import * as ldcp from 'jsonld-context-parser'
import * as fs from 'fs'
import axios from 'axios'
import * as md5 from 'md5'


export const contextParser = new ldcp.ContextParser()
export const NGSI_LD_CORE_CONTEXT_URL = "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.3.jsonld"


export function appendCoreContext(nonNormalizedContext: any): Array<any> {

    let result = nonNormalizedContext

    if (result == null || result == undefined) {
        result = []
    }
    else if (!(result instanceof Array)) {
        result = [result]
    }

    if (!result.includes(NGSI_LD_CORE_CONTEXT_URL)) {
        result.push(NGSI_LD_CORE_CONTEXT_URL)
    }

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

            // TODO: 1 Is this correct?
            if (key != "value" && key != "https://uri.etsi.org/ngsi-ld/hasValue") {

                result[key] = expandObject(result[key], normalizedContext)
            }          
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

    const contextCacheDir = "contextCache/"

    let result: Array<any> = []

    if (!(context instanceof Array)) {
        context = [context]
    }


    if (!fs.existsSync(contextCacheDir)) {
        fs.mkdirSync(contextCacheDir);
    }


    for (let entry of context) {

        if (typeof (entry) == "string" && ((entry.startsWith("https://") || entry.startsWith("http://")))) {

            const url = entry
            const fileName = contextCacheDir + md5(url) + ".jsonld"


            if (!fs.existsSync(fileName)) {

                const response = await axios.get(url).catch((e) => {
                    console.log("ERROR when trying to fetch context document")
                    console.log(e)
                })

                if (response != undefined) {
                    fs.writeFileSync(fileName, JSON.stringify(response.data))
                }
            }

            const contextItem = JSON.parse(fs.readFileSync(fileName).toString())

            result.push(contextItem)

        }
        else if (typeof (entry) == "object") {
            result.push(entry)
        }
        else {
            // throw error
            console.log("Invalid context entry: " + entry)
        }
    }

    return result
}
