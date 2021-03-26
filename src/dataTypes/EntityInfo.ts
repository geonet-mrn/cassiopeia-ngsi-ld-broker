// Spec 5.2.8

export class EntityInfo {

    // ATTENTION: Officially, "type" is mandatory and can not be undefined. However, this conflicts
    // with other parts of the specification, so we allow it to be undefined for now.
    // TODO: Ask NEC about this.

    constructor(public id: string | undefined,
                public idPattern: string|undefined,
                public type: string|undefined) {

    }
}