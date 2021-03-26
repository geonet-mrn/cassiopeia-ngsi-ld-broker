// Spec 5.2.25

import * as uuid from 'uuid'


export class EntityType {
    
    readonly type = "EntityType"

    readonly id = "urn:" + uuid.v4()

    constructor(public typeName : string, public attributeNames : Array<string> = []) {
        
    }
}