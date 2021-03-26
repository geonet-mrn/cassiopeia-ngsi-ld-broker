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
        public temporalQ : TemporalQuery|undefined,

        // NOTE: 'geometryProperty', 'datasetId', 'options' and '@context' are not official members 
        // of the Query class, but it makes sense to add them.
        // TODO: Ask NEC about this.
        public geometryProperty : string|undefined,
        public datasetId : string|undefined,
        public options : Array<string>|undefined,
        
    ) {}
}