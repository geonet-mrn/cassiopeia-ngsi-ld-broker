// Spec 5.2.26

import { Attribute } from "./Attribute";
import * as uuid from 'uuid'

export class EntityTypeInfo {

    readonly type = "EntityTypeInformation"

    readonly id = "urn:" + uuid.v4()


    constructor(public typeName : string, 
    public entityCount : number, public attributeDetails : Array<Attribute> = []) {

    }
}