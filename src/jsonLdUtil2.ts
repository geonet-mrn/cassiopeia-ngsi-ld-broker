import * as jsonld from 'jsonld'
import { getContextForContextArrayEntry } from './jsonldUtil';


export async function getContextLoader(context: any) {
    //################# BEGIN Define custom context loader ##################
    const contexts: any = {}

    for (const entry of context) {
        contexts[entry] = await getContextForContextArrayEntry(entry)
    }

    //@ts-ignore
    const nodeDocumentLoader = jsonld.documentLoaders.node();

    const customLoader = async (url: string, options: any) => {


        if (url in contexts) {

            return {
                contextUrl: null, // this is for a context via a link header
                document: contexts[url], // this is the actual document that was loaded
                documentUrl: url // this is the actual context URL after redirects
            };
        }
        // call the default documentLoader
        return nodeDocumentLoader(url);
    };
    //################# END Define custom context loader ##################

    return customLoader
}



export async function expandIri(iri : string, context : any) : Promise<string> {
    const customLoader = await getContextLoader(context)

    
    const obj : any = {
        "@context": context,
    }

    obj[iri] = ""

    let cc = await jsonld.expand(obj, { documentLoader: customLoader })

   
    for(const key in cc[0]) {
        if (key != "@context") {
            return key
        }
    }
    
    return iri
    
}





export async function compactIri(iri : string, context : any) : Promise<string> {
    const customLoader = await getContextLoader(context)

    const obj : any = {
        "@context": context,
    }

    obj[iri] = "lala"

    let cc = await jsonld.compact(obj)

    for(const key in cc) {
        if (key != "@context") {
            return key
        }
    }
    
    return iri
    
}
