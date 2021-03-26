// Spec 5.2.29

export class Feature {

    readonly type = "Feature"

    constructor(public id: string, // Entity ID, should be URI
        public geometry: any,
        public properties: any) {

        (this as any)["@context"] = ""
    }
}