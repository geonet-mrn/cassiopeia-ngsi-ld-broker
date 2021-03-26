// Spec 5.2.21
// Spec 4.11

export class TemporalQuery {
    constructor(
        public timerel : "before"|"after"|"between",
        public timeAt : string, // Should be a DateTime
        public endTimeAt : string|undefined, // Should be a DateTime
        public timeproperty : "observedAt"|"createdAt"|"modifiedAt" = "observedAt",

        public lastN : number|undefined // NOTE: Putting lastN into the TemporalQuery class is not official NGSI-LD.
    ) {}
}