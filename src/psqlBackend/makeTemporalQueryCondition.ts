import { TemporalQuery } from "../dataTypes/TemporalQuery"
import { errorTypes } from "../errorTypes"
import { isDateTimeUtcString } from "../validate"
import { PsqlTableConfig } from "./PsqlTableConfig"


export function makeTemporalQueryCondition(temporalQ: TemporalQuery, tableCfg : PsqlTableConfig): string {

    if (!temporalQ.timerel || !temporalQ.timeAt) { 
        return ""
    }

    if (!isDateTimeUtcString(temporalQ.timeAt)) {
        throw errorTypes.InvalidRequest.withDetail(`Invalid 'timeAt' value: '${temporalQ.timeAt}'. Must be a ISO-8601 DateTime UTC string.`)
    }


    //################## BEGIN Figure out temporal table column to query ####################
    const temporalFields = {
        'observedAt': tableCfg.COL_ATTR_OBSERVED_AT,
        'modifiedAt': tableCfg.COL_ATTR_MODIFIED_AT,
        'createdAt': tableCfg.COL_ATTR_CREATED_AT
    }

    if (!(temporalQ.timeproperty in temporalFields)) {
        throw errorTypes.InvalidRequest.withDetail(`Invalid 'timeproperty' value: '${temporalQ.timeproperty}'. Must be one of: 'creatdAt', 'modifiedAt', 'observedAt'`)
    }

    const temporal_field = temporalFields[temporalQ.timeproperty]
    //################## END Figure out temporal table column to query ####################


    // Make sure that the filter is connected to the specific attribute we want to check:        
    let result = " AND " + temporal_field

    switch (temporalQ.timerel) {
        case "before": {
            result += ` < '${temporalQ.timeAt}'`
            break;
        }
        case "between": {

            if (temporalQ.endTimeAt == undefined || !isDateTimeUtcString(temporalQ.endTimeAt)) {
                throw errorTypes.InvalidRequest.withDetail(`Invalid 'endTimeAt' value: '${temporalQ.endTimeAt}'. Must be a ISO-8601 DateTime UTC string.`)
            }

            result += ` > '${temporalQ.timeAt}' AND ${temporal_field} < '${temporalQ.endTimeAt}'`
            break;
        }
        case "after": {

            result += ` > '${temporalQ.timeAt}'`
            break;
        }
        default: {
            throw errorTypes.InvalidRequest.withDetail(`Invalid 'timerel' value: '${temporalQ.timerel}'. Must be one of 'before', 'between', 'after'.`)
        }
    }

    return result
}
