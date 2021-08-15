// Spec 5.2.23

import { EntityInfo } from "./EntityInfo";
import { GeoQuery } from "./GeoQuery";
import { TemporalQuery } from "./TemporalQuery";

export class Query {

    readonly type = "Query"

    constructor(
        public entities : Array<EntityInfo>|undefined,
        public attrs : Array<string>|undefined,
        public q : string|undefined,
        public geoQ : GeoQuery|undefined,
        public csf : string|undefined,
        public temporalQ : TemporalQuery|undefined 
    ) {}
}