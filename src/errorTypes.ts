// Spec 6.3.2

import { ProblemDetails } from "./dataTypes/ProblemDetails";

export const errorTypes = {

    "InvalidRequest": new ProblemDetails("https://uri.etsi.org/ngsi-ld/errors/InvalidRequest",
        "The request associated to the operation is syntactically invalid or includes wrong content",
        "", 400),


    "BadRequestData": new ProblemDetails("https://uri.etsi.org/ngsi-ld/errors/BadRequestData",
        "The request includes input data which does not meet the requirements of the operation",
        "", 400),


    "AlreadyExists": new ProblemDetails("https://uri.etsi.org/ngsi-ld/errors/AlreadyExists",
        "The referred element already exists",
        "", 409),


    "OperationNotSupported": new ProblemDetails("https://uri.etsi.org/ngsi-ld/errors/OperationNotSupported",
        "The operation is not supported",
        "", 422),


    "ResourceNotFound": new ProblemDetails("https://uri.etsi.org/ngsi-ld/errors/ResourceNotFound",
        "The referred resource has not been found",
        "", 404),

    "InternalError": new ProblemDetails("https://uri.etsi.org/ngsi-ld/errors/InternalError",
        "There has been an error during the operation execution",
        "", 500),

    "TooComplexQuery": new ProblemDetails("https://uri.etsi.org/ngsi-ld/errors/TooComplexQuery",
        "The query associated to the operation is too complex and cannot be resolved",
        "", 403),

    "TooManyResults": new ProblemDetails("https://uri.etsi.org/ngsi-ld/errors/TooManyResults",
        "The query associated to the operation is producing so many results that can exhaust client or server resources. It should be made more restrictive",
        "", 403),

    "LdContextNotAvailable": new ProblemDetails("https://uri.etsi.org/ngsi-ld/errors/LdContextNotAvailable",
        "A remote JSON-LD @context referenced in a request cannot be retrieved by the NGSI-LD Broker and expansion or compaction cannot be performed",
        "", 503),

    "NoMultiTenantSupport": new ProblemDetails("https://uri.etsi.org/ngsi-ld/errors/NoMultiTenantSupport",
        "The NGSI-LD API implementation does not support multiple tenants.",
        "", 501),


    "NonexistentTenant": new ProblemDetails("https://uri.etsi.org/ngsi-ld/errors/NonexistentTenant",
        "The addressed tenant does not exist.",
        "", 404)
}