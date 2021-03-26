// Spec 5.2.24

import * as uuid from 'uuid'

export class EntityTypeList {
    
    readonly type = "EntityTypeList"

    readonly id = "urn:" + uuid.v4()

    constructor(public typeList : Array<string> = []) {

    }
}