import { BatchEntityError } from "./BatchEntityError";

// Spec 5.2.16

export class BatchOperationResult {
    constructor(

        public success : Array<string> = [], 
        public errors : Array<BatchEntityError> = []

    ) { }
}