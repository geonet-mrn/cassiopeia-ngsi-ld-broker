// Spec 5.2.18

import { NotUpdatedDetails } from "./NotUpdatedDetails";

export class UpdateResult {
    constructor(
        public updated: Array<string> = [],
        public notUpdated: Array<NotUpdatedDetails> = []
    ) { }
}