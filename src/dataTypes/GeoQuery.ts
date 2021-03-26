// Spec 5.2.13
// Spec 4.10

export class GeoQuery {
    constructor(
        public geometry : string, 
        public coordinates : Array<any>|string,
        public georel : "near"|"within"|"contains"|"intersects"|"equals"|"disjoints"|"overlaps",
        public geoproperty : string = "location"

    ) {}
}