// Spec 5.2.30

import { Feature } from "./Feature"

export class FeatureCollection {

    readonly type = "FeatureCollection"

    constructor(public features : Array<Feature> = []) {


        (this as any)["@context"] = ""
    }
}