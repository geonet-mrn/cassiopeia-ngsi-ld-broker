// Spec 5.2.27

import * as uuid from 'uuid'

export class AttributeList {

    readonly type = "AttributeList"

    readonly id = "urn:" + uuid.v4()

    constructor(public attributeList : Array<string> = []) {

    }
}