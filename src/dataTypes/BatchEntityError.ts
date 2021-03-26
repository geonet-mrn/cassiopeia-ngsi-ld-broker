// Spec 5.2.17

import { ProblemDetails } from "./ProblemDetails";

export class BatchEntityError {
    constructor(
        public entityId : string, 
        public error : ProblemDetails
    ) {}
}