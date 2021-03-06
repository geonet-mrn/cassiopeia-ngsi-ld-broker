// TODO: Complete implementation of ValueList

// TODO: patternOp / noPatternOp

// TODO: "Is included in the target value, if the latter is an array 
// (e.g. matches ["blue","red","green"]). ("inverse value list", so to say)

// TODO: Perhaps create new query parser object for each query and make context a class member

// TODO: " If the data type of the target value and the data type of the Query Term value are different, 
// then they shall be considered unequal.""

import { Query } from "./dataTypes/Query"
import { errorTypes } from "./errorTypes"

import { isDateString, isDateTimeUtcString, isTimeUtcString, isUri } from "./validate"
import { PsqlTableConfig } from "./PsqlTableConfig"
import * as ldcp from 'jsonld-context-parser'
import { JsonLdContextNormalized } from "jsonld-context-parser"




enum CompareValueType {
    UNKNOWN = "unknown",
    BOOLEAN = "boolean",
    DATE = "date",
    DATETIME = "datetime",
    NUMBER = "number",
    QUOTEDSTR = "quotedstr",    
    TIME = "time",
    URI = "uri"
}

export class NgsiLdQueryParser {

    // ATTENTION: Changing the order of items in attributeTypes corrupts the database!
    private readonly attributeTypes = ["https://uri.etsi.org/ngsi-ld/Property", "https://uri.etsi.org/ngsi-ld/GeoProperty", "https://uri.etsi.org/ngsi-ld/Relationship"]


    // ATTENTION: For correct matching by the tokenizer, it is required that the symbols are ordered by decreasing length!    
    private readonly tokenizerDetectableSymbols = ['!~=', '==', '!=', '>=', '<=', '~=', '>', '<', ';', '|', '(', ')']

    // ATTENTION: The order of the logical operators in this list defines their priority (e.g. AND ";" over OR "|") !
    private readonly operators = ['!~=', '==', '!=', '>=', '<=', '~=', '>', '<', ';', '|']


    private readonly ERROR_STRING_INTRO = "Invalid NGSI-LD query string: "

    private readonly nonReifiedDefaultProperties = ["https://uri.etsi.org/ngsi-ld/createdAt",
        "https://uri.etsi.org/ngsi-ld/modifiedAt",
        "https://uri.etsi.org/ngsi-ld/observedAt",
        "https://uri.etsi.org/ngsi-ld/datasetId",
        "https://uri.etsi.org/ngsi-ld/unitCode"]



    constructor(private tableCfg: PsqlTableConfig) { }


    makeQuerySql(query: Query, context: JsonLdContextNormalized, attr_table: string): string {

        if (query.q == undefined) {
            return ""
        }

        const tokens = this.tokenize(query.q)

        const ast = this.buildAst(tokens)


        return this.build(ast, context, attr_table)
    }


    private buildAst(tokens: Array<string>) {

        const items = this.parseParantheses(tokens, 0).group

        for (let ii = 0; ii < items.length; ii++) {

            if (items[ii] instanceof Array) {
                items[ii] = this.buildAst(items[ii])
            }
        }


        let result = items

        for (let operator of this.operators) {
            result = this.processOperator(result, operator)
        }

        return result
    }


    private build(ast: Array<any>, context: ldcp.JsonLdContextNormalized, attrTable: string): string {

        let result = "("

        // Check for existence of attribute (regardless of its value):
        if (typeof (ast) == "string") {

            // "When a Query Term only defines an attribute path (production rule named Attribute), 
            // the matching Entities shall be those which define the target element (Property or a Relationship),"
            // regardless of any target value or object":

            const attrPath_compacted = (ast as string).split(".")

            // ATTENTION: We assume here that the return values of context.expandTerm() can never be null.
            // This is of course not correct and should be handled appropriately.

            const firstPathPiece_compacted = attrPath_compacted[0]

            if (firstPathPiece_compacted == undefined) {
                throw errorTypes.InvalidRequest.withDetail("Invalid query path: " + attrPath_compacted.toString())
            }

            const firstPathPiece_expanded = context.expandTerm(firstPathPiece_compacted, true)
            const lastPathPiece_expanded = context.expandTerm(attrPath_compacted[attrPath_compacted.length - 1], true)!

            // Remove first piece of attribute path. We do this because the first path piece is not part of
            // the JSON database field. It exists in separate form in the ATTR_NAME column:
            attrPath_compacted.shift()


            //##################### BEGIN Build expanded attribute path SQL expression #################
            let attrPathSql = ""

            for (const propName of attrPath_compacted) {

                const propNameExpanded = context.expandTerm(propName, true)!

                if (this.nonReifiedDefaultProperties.includes(propNameExpanded)) {

                    attrPathSql += `->>'${propNameExpanded}'`
                    break
                }
                else {
                    attrPathSql += `->'${propNameExpanded}'`
                }
            }
            //##################### END Build expanded attribute path SQL expression #################


            result += `SELECT eid FROM ${attrTable} WHERE ${attrTable}.attr_name = '${firstPathPiece_expanded}' AND `

            result += "("

            // Check existence of non-reified property:
            if (this.nonReifiedDefaultProperties.includes(lastPathPiece_expanded)) {
                result += `${attrTable}.${this.tableCfg.COL_INSTANCE_JSON}${attrPathSql} is not null `
            }

            // Check existence of reified Property or Relationship:
            else {

                //########### BEGIN Check existence of Property ##############
                result += "("
                result += `${attrTable}.${this.tableCfg.COL_ATTR_TYPE} = ${this.attributeTypes.indexOf('https://uri.etsi.org/ngsi-ld/Property')}`

                result += " AND "
                result += `${attrTable}.${this.tableCfg.COL_INSTANCE_JSON}${attrPathSql}->'https://uri.etsi.org/ngsi-ld/hasValue' is not null`
                result += ")"
                //########### END Check existence of Property ##############

                result += " OR "

                //########### BEGIN Check existence of Relationship ##############
                result += "("
                result += `${attrTable}.${this.tableCfg.COL_ATTR_TYPE} = ${this.attributeTypes.indexOf('https://uri.etsi.org/ngsi-ld/Relationship')}`
                result += " AND "
                result += `${attrTable}.${this.tableCfg.COL_INSTANCE_JSON}${attrPathSql}->'https://uri.etsi.org/ngsi-ld/hasObject' is not null`
                result += ")"
                //########### END Check existence of Relationship ##############                                   
            }


            result += ")"
        }

        // Check for existence of attribute with compare condition:
        else if (ast instanceof Array && ast.length == 3) {

            let left = ast[0]
            let op = ast[1]
            let right = ast[2]


            switch (op) {

                // TODO: Maybe use different methods for equality comparators (==,!=) and the others
                case "==": {
                    result += this.blubb(left, "=", right, context, attrTable)
                    break
                }
                case "!=": {
                    result += this.blubb(left, "!=", right, context, attrTable)
                    break
                }
                case ">=": {
                    result += this.blubb(left, ">=", right, context, attrTable)
                    break
                }
                case ">": {
                    result += this.blubb(left, ">", right, context, attrTable)
                    break
                }
                case "<=": {
                    result += this.blubb(left, "<=", right, context, attrTable)
                    break
                }
                case "<": {
                    result += this.blubb(left, "<", right, context, attrTable)
                    break
                }
                case "|": {
                    result += this.build(left, context, attrTable) + " UNION " + this.build(right, context, attrTable)
                    break
                }
                case ";": {
                    result += this.build(left, context, attrTable) + " INTERSECT " + this.build(right, context, attrTable)
                    break
                }
                default: {
                    throw errorTypes.InvalidRequest.withDetail(this.ERROR_STRING_INTRO + "Unknown query term operator: '" + op + "'.")
                }
            }
        }
        else {
            throw errorTypes.InvalidRequest.withDetail(this.ERROR_STRING_INTRO + `Invalid query term: '${ast.toString()}'`)
        }


        result += ")"

        return result
    }


    // Spec 4.9
    private blubb(leftSide: string, op: string, rightSide: string, context: ldcp.JsonLdContextNormalized, attrTable: string): string {


        // TODO: ValueList

        // TODO: patternOp / noPatternOp

        // TODO: "Is included in the target value, if the latter is an array 
        // (e.g. matches ["blue","red","green"]). ("inverse value list", so to say)

        // (also for other operators)

        // Split complete attribute path in main part and trailing path:
        const mainPathAndTrailingPath = leftSide.split("[")


        const attrPath = mainPathAndTrailingPath[0].split(".")

        if (attrPath.length == 0) {
            throw errorTypes.InvalidRequest.withDetail("Invalid query string: Attribute path has length 0.")
        }


        let jsonFullPathSql_property = ""

        //############### BEGIN Build main attribute path expression (without trailing path) ##############

        let jsonAttrPathSql = `${attrTable}.${this.tableCfg.COL_INSTANCE_JSON}`

        // NOTE: We skip the first element of the attribute path here, 
        // since it is the key of the attribute and not included in the JSON field in the database:
        for (const key of attrPath.slice(1)) {

            const expandedKey = context.expandTerm(key, true)!

            if (this.nonReifiedDefaultProperties.includes(expandedKey)) {

                jsonAttrPathSql += `->>'${expandedKey}'`
                break
                // TODO: Throw error if path continues after non-reified element
            }
            else {
                jsonAttrPathSql += `->'${expandedKey}'`
            }
        }
        //############### END Build main attribute path expression (without trailing path) ##############


        //############# BEGIN Build Complete attribute path expression (with trailing path) ##############           
        let trailingPath = null

        if (mainPathAndTrailingPath.length == 2) {
            trailingPath = mainPathAndTrailingPath[1].substr(0, mainPathAndTrailingPath[1].length - 1).split(".")
        }

        // We begin with the main attribute path which we have already built:
        jsonFullPathSql_property = jsonAttrPathSql

        

        // If we have a trailing path, let's add it to the main path:
        // NOTE: Trailing paths don't exist in relationships!
        if (trailingPath != null) {

            // ATTENTION: Note that we access "value" as a JSON OBJECT here ("->" operator), 
            // and not as its direct value ("->>" operator)!!

            jsonFullPathSql_property += `->'https://uri.etsi.org/ngsi-ld/hasValue'`

            for (let ii = 0; ii < trailingPath.length; ii++) {
                const key = trailingPath[ii]

                // For the last element, we change the JSON accessor to "->>" to access its text content:
                // ATTENTION: Property values are not expanded accoring to M. Bauer 2021-04-30!

                if (ii == trailingPath.length - 1) {
                    jsonFullPathSql_property += `->>'${key}'`
                }
                else {
                    jsonFullPathSql_property += `->'${key}'`
                }
            }
        }

        // If we have no trailing path, we access the value of the last path element directly:
        else {
            // ATTENTION: As opposed to the case above where we have a trailing path,
            // we access "value" as its direct value here ("->>" Operator)!         

            if (!(this.nonReifiedDefaultProperties.includes(attrPath[attrPath.length - 1]))) {
                jsonFullPathSql_property += `->>'https://uri.etsi.org/ngsi-ld/hasValue'`
            }
        }
        //############# END Build Complete attribute path expression (with trailing path) ##############           



        // Start the SQL query with making sure that the the attribute name matches:
        const firstPathPiece = context.expandTerm(attrPath[0], true)
        let result = `SELECT eid FROM ${attrTable} WHERE ${attrTable}.attr_name = '${firstPathPiece}' `

        // ... then continue with the value condition:


        let range: Array<string> | null = rightSide.split("..")

        let valueList: Array<string> | null = rightSide.split(",")

        valueList = rightSide.match(/(?:[^,"]+|"[^"]*")+/g)
        // range = rightSide.match(/(?:[^(..)"]+|"[^"]*")+/g) 

        if (valueList == null) {
            throw errorTypes.InvalidRequest.withDetail(this.ERROR_STRING_INTRO + "Invalid value list: " + rightSide)
        }

        if (range == null) {
            throw errorTypes.InvalidRequest.withDetail(this.ERROR_STRING_INTRO + "Invalid range: " + rightSide)
        }

        //############## BEGIN Validation ##############
        if (range.length > 1 && valueList.length > 1) {
            throw errorTypes.InvalidRequest.withDetail(this.ERROR_STRING_INTRO + "ranges and value lists must not be mixed")
        }

        if (range.length > 2) {
            throw errorTypes.InvalidRequest.withDetail(this.ERROR_STRING_INTRO + "A range must contain only one instance of '..'.")
        }
        //############## END Validation ##############

        result += " AND ("

        // Single value compare:
        if (range.length == 1 && valueList.length == 1) {
            result += this.buildSingleValueCompare(range[0], op, jsonFullPathSql_property, jsonAttrPathSql, attrTable)
        }
        // Range compare:
        else if (range.length == 2) {
            result += this.buildRangeCompare(range, op, jsonFullPathSql_property)
        }

        // Value list compare:
        else if (valueList.length > 1) {
            result += this.buildValueListCompare(valueList, op, jsonFullPathSql_property, jsonAttrPathSql, attrTable)
        }
        else {
            throw errorTypes.InvalidRequest.withDetail(this.ERROR_STRING_INTRO + "Could not determine structure of compare value: " + rightSide)
        }

        result += ")"

        return result
    }




    private buildValueListCompare(valueList: Array<string>, op: String, jsonFullPathSql: string, jsonAttrPathSql: string, attrTable: string) {

        let result = "("

        const compareType = this.figureOutValueType(valueList)

        if (compareType == CompareValueType.UNKNOWN) {
            throw errorTypes.InvalidRequest.withDetail(this.ERROR_STRING_INTRO + "Failed to determine compare value type for value list: " + JSON.stringify(valueList))
        }

        //################ BEGIN Compare expresision ################      

        switch (compareType) {

            // TODO: 3 Complete this
            /*
            case CompareValueType.DATE: {
                result += `(${jsonFullPathSql})::timestamp ${op} '${compareValue}'`
                break
            }
            case CompareValueType.TIME: {
                result += `(${jsonFullPathSql})::timestamp::time ${op} '${compareValue}'`
                break
            }
            case CompareValueType.DATETIME: {
                result += `(${jsonFullPathSql})::timestamp ${op} '${compareValue}'`
                break
            }
            */
            case CompareValueType.NUMBER: {

                const sql_op = (op = "==") ? "IN" : "NOT IN"

                result += `(${jsonFullPathSql})::numeric ${sql_op} (${valueList.join(",")})`
                break
            }

            case CompareValueType.QUOTEDSTR: {

                const sql_op = (op = "==") ? "IN" : "NOT IN"

                let pieces = Array<string>()

                // Replace double quotes with single quotes at beginning and end:
                for (const item of valueList) {
                    pieces.push("'" + item.substr(1, item.length - 2) + "'")
                }

                // TODO: 2 Implement:
                // "The target value includes any of the Query Term values, if the target value is an array (e.g. matches ["red","blue"]).""

                // Spec 4.9: "The target value is identical or equivalent to any of the list values (e.g. matches "red")."
                result += `(${jsonFullPathSql})::text ${sql_op} (${pieces.join(",")})`
                break
            }

            // TODO: Implement value list support for relationships:
            /*
            case CompareValueType.URI: {
                // NOTE: Compare expression for Relationships is different, so we don't set test1 here and
                // write the Relationship expression below if test1 == null.
                // NOTE: For Relationship queries, the trailing path does not play a role:
                
                result += `${attrTable}.${this.tableCfg.COL_ATTR_TYPE} = ${this.attributeTypes.indexOf('https://uri.etsi.org/ngsi-ld/Relationship')} AND ${jsonAttrPathSql}->>'https://uri.etsi.org/ngsi-ld/hasObject' = '${compareValue}'`
                break
            }
            */
            default: {
                throw errorTypes.InvalidRequest.withDetail(this.ERROR_STRING_INTRO + "Unsupported compare type: " + compareType)
            }
        }

        result += ")"

        return result
    }


    private buildSingleValueCompare(compareValue: string, op: String, jsonFullPathSql_property: string, jsonAttrPathSql: string, attrTable: string) {

        let result = ""

        const compareType = this.figureOutValueType([compareValue])

        if (compareType == CompareValueType.UNKNOWN) {
            throw errorTypes.InvalidRequest.withDetail(this.ERROR_STRING_INTRO + "Failed to determine compare value type for value: " + JSON.stringify(compareValue))
        }

        //################ BEGIN Compare expresision ################      

        switch (compareType) {
            case CompareValueType.BOOLEAN: {
                result += `(${jsonFullPathSql_property})::boolean ${op} ${compareValue}`
                break
            }
            case CompareValueType.DATE: {
                result += `(${jsonFullPathSql_property})::timestamp ${op} '${compareValue}'`
                break
            }
            case CompareValueType.TIME: {
                result += `(${jsonFullPathSql_property})::timestamp::time ${op} '${compareValue}'`
                break
            }
            case CompareValueType.DATETIME: {
                result += `(${jsonFullPathSql_property})::timestamp ${op} '${compareValue}'`
                break
            }
            case CompareValueType.NUMBER: {
                result += `(${jsonFullPathSql_property})::numeric ${op} ${compareValue}`
                break
            }
            
            case CompareValueType.QUOTEDSTR: {

                // ATTENTION: According to the NGSI-LD specification section 4.9, a relationship URI 
                // in an NGSI-LD query must not be enclosed in double quotes. This case is handled in 
                // the next 'case' block. However, apparently, at least the Orion broker supports 
                // relationship queries with quoted URIs. For compatibility, we support this syntax, too. 
                // In order to do so, we must compare all quoted string compare queries both against 
                // possible instances of "Property.value" and of of "Relationship.object":

                // NOTE: With the substr(), we remove the beginning and end quotes: 
                
                const stringWithoutQuotes = compareValue.substr(1, compareValue.length - 2)

                //result += `(${jsonFullPathSql_property})::text ${op} '${stringWithoutQuotes}'`

                // NOTE: We use jsonFullPathSql_property to access Property 'value' and jsonAttrPathSql to access Relationship 'object' 
                // because Propery value queries can have a trailing path, while Relationship object queries can't have a trailing path

                result += `(${attrTable}.${this.tableCfg.COL_ATTR_TYPE} = ${this.attributeTypes.indexOf('https://uri.etsi.org/ngsi-ld/Property')} AND (${jsonFullPathSql_property})::text ${op} '${stringWithoutQuotes}')`
                
                result += " OR "

                result += `(${attrTable}.${this.tableCfg.COL_ATTR_TYPE} = ${this.attributeTypes.indexOf('https://uri.etsi.org/ngsi-ld/Relationship')} AND ${jsonAttrPathSql}->>'https://uri.etsi.org/ngsi-ld/hasObject' ${op} '${stringWithoutQuotes}')`
                break
            }
            case CompareValueType.URI: {
               
                // NOTE: For Relationship queries, the trailing path does not play a role:

                result += `${attrTable}.${this.tableCfg.COL_ATTR_TYPE} = ${this.attributeTypes.indexOf('https://uri.etsi.org/ngsi-ld/Relationship')} AND ${jsonAttrPathSql}->>'https://uri.etsi.org/ngsi-ld/hasObject' = '${compareValue}'`
                break
            }
            default: {
                throw errorTypes.InvalidRequest.withDetail(this.ERROR_STRING_INTRO + "Unable to determine type of compare value: " + compareValue)
            }
        }

        return result
    }



    private buildRangeCompare(range: Array<string>, op: String, jsonFullPathSql: string) {

        // TODO: What to do if right end of range is smaller than left end?

        const compareType = this.figureOutValueType(range)

        if (compareType == CompareValueType.UNKNOWN) {
            throw errorTypes.InvalidRequest.withDetail(this.ERROR_STRING_INTRO + "Failed to determine compare value type for range: " + JSON.stringify(range))
        }


        let result = ""

        //########## BEGIN Apply operator #############
        if (op == "!=") {
            result += " NOT "
        }
        else if (op == "=") {
            // Nothing to do:
        }
        else {
            throw errorTypes.InvalidRequest.withDetail("Operator not supported for range comparisons: " + op)
        }
        //########## END Apply operator #############

        result += "("

        //################ BEGIN Compare expresision ################        

        switch (compareType) {

            case CompareValueType.DATE: {
                result += `(${jsonFullPathSql})::timestamp >= '${range[0]}' AND (${jsonFullPathSql})::timestamp <= '${range[1]}`
                break
            }
            case CompareValueType.TIME: {
                result += `(${jsonFullPathSql})::timestamp::time >= '${range[0]}' AND (${jsonFullPathSql})::timestamp::time <= '${range[1]}`
                break
            }
            case CompareValueType.DATETIME: {
                result += `(${jsonFullPathSql})::timestamp >= '${range[0]}' AND (${jsonFullPathSql})::timestamp <= '${range[1]}`
                break
            }
            case CompareValueType.NUMBER: {
                result += `(${jsonFullPathSql})::numeric >= ${range[0]} AND (${jsonFullPathSql})::numeric <= ${range[1]}`
                break
            }
            case CompareValueType.QUOTEDSTR: {
                // NOTE: With the substr(), we remove the beginning and end quotes:
                const cv0 = range[0].substr(1, range[0].length - 2)
                const cv1 = range[1].substr(1, range[1].length - 2)

                result += `(${jsonFullPathSql})::text >= '${cv0}' AND (${jsonFullPathSql})::text <= '${cv1}'`
                break
            }
            case CompareValueType.URI: {
                // NOTE: Compare expression for Relationships is different, so we don't set test1 here and
                // write the Relationship expression below if test1 == null.
                break
            }
            default: {
                throw errorTypes.InvalidRequest.withDetail(this.ERROR_STRING_INTRO + "Unable to determine type of compare value: " + range[1])
            }
        }

        result += ")"

        return result
    }



    private figureOutValueType(range: Array<string>): CompareValueType {

        // NOTE: He, we do two things:
        // 1. Determine the value type of the range expression
        // 2. Check whether both the min and max value of the range have the same type

        // Also not that the type determined here is used for individual compare values as well 
        // (i.e. consider individual compare values as "ranges with same min and max").

        let previousType = CompareValueType.UNKNOWN

        for (const item of range) {

            let newType = CompareValueType.UNKNOWN

            if (item == "true" || item == "false") {
                newType = CompareValueType.BOOLEAN
            }
            else if (isDateString(item)) {
                newType = CompareValueType.DATE
            }
            else if (isTimeUtcString(item)) {
                newType = CompareValueType.TIME
            }
            else if (isDateTimeUtcString(item)) {
                newType = CompareValueType.DATETIME
            }
            else if (!isNaN(Number(item)) && !isNaN(parseFloat(item))) {
                newType = CompareValueType.NUMBER
            }
            else if (item.match('"[^"]*"')) {
                newType = CompareValueType.QUOTEDSTR
            }
            else if (isUri(item)) {
                newType = CompareValueType.URI
            }
            else {
                throw errorTypes.InternalError.withDetail(this.ERROR_STRING_INTRO + "Failed to determine value type")
            }

            /*
            if (item.match('"[^"]*"')) {
                const itemWithoutEnclosingQuotes = item.substr(1, item.length - 2) 
              
                if (isUri(itemWithoutEnclosingQuotes)) {
                    newType = CompareValueType.QUOTEDURI
                }
            }
            */

            if (previousType != CompareValueType.UNKNOWN && newType != previousType) {
                throw errorTypes.InvalidRequest.withDetail(this.ERROR_STRING_INTRO + "Types of invidivual values in ranges and value lists must be equal.")
            }

            previousType = newType
        }

        return previousType
    }


    private processOperator(items: Array<any>, operator: string): Array<any> {

        if (!(items instanceof Array)) {
            return items
        }

        let result = Array<any>()

        let index = 0

        let didReplace = false

        while (index < items.length) {

            if (index < items.length - 1 && items[index + 1] == operator) {

                result.push([items[index], items[index + 1], items[index + 2]])
                index += 2
                didReplace = true
            }
            else {
                result.push(items[index])
            }

            index++
        }


        // NOTE: In order to make sure that we process all operators, we need to repeat
        // until no further change is made:

        if (didReplace) {
            result = this.processOperator(result, operator)
        }

        // Remove unneccessary double-nested parantheses:
        if (result.length == 1) {
            result = result[0]
        }

        return result
    }



    private parseParantheses(tokens: Array<string>, index: number): { group: Array<any>, index: number } {

        const result = Array<any>()

        // TODO: 4 Validate parantheses structure (do we close as many parantheses as we open?)

        //################# BEGIN Iterate over tokens #######################
        while (index < tokens.length) {

            const token = tokens[index]

            if (token == '(') {

                const pp = this.parseParantheses(tokens, index + 1)

                // Skip over the tokens processed in the recursive call above:
                index = pp.index

                // Add the token which were grouped in the recursive call to the result:
                result.push(pp.group)
            }
            else if (token == ')') {

                // Exit recursion level:
                return { group: result, index: index }
            }
            else {
                result.push(token)
            }

            index++
        }
        //################# END Iterate over tokens #######################

        return { group: result, index: index }
    }


    private tokenize(query: string): Array<string> {

        const result = Array<string>()

        let collected = ""

        let insideQuotedString = false

        while (query.length > 0) {

            let symbolFound = null

            //########### BEGIN Test for known symbol #########

            if (query.substr(0, 1) == '"') {
                insideQuotedString = !insideQuotedString
            }

            // ATTENTION: The following for loop only works correctly if self.symbols is ordered by item string length!

            if (!insideQuotedString) {
                for (const symbol of this.tokenizerDetectableSymbols) {

                    if (query.substr(0, symbol.length) == symbol) {
                        symbolFound = symbol
                        break
                    }
                }
            }
            //########### END Test for known symbol #########

            if (symbolFound != null) {

                if (collected.length > 0) {
                    result.push(collected)
                }

                collected = ""

                result.push(symbolFound)
                query = query.substr(symbolFound.length)
            }
            else {
                collected += query.substr(0, 1)
                query = query.substr(1)
            }
        }

        if (insideQuotedString) {
            throw errorTypes.InvalidRequest.withDetail(this.ERROR_STRING_INTRO + "The query string contains invalid quotes strings: " + query)
        }

        // Add last token to result:
        if (collected.length > 0) {
            result.push(collected)
        }

        return result
    }
}
