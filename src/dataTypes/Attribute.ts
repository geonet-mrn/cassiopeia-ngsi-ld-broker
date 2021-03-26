// Spec 5.2.28

export class Attribute {

    readonly type = "Attribute"

    constructor(public readonly id : string, 
                public readonly attributeName : string, 
                public readonly attributeCount : number, 
                public readonly attributeTypes : Array<string> = [], 
                public readonly typeNames : Array<string> = []) {
            
        }
}