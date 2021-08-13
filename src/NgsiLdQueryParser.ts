// TODO: ValueList

// TODO: patternOp / noPatternOp

// TODO: "Is included in the target value, if the latter is an array 
// (e.g. matches ["blue","red","green"]). ("inverse value list", so to say)

// TODO: Perhaps create new query parser object for each query and make context a class member

import { Query } from "./dataTypes/Query"
import { errorTypes } from "./errorTypes"

import { isDateString, isDateTimeUtcString, isTimeUtcString, isUri } from "./validate"
import { PsqlTableConfig } from "./PsqlTableConfig"
import * as ldcp from 'jsonld-context-parser'
import { JsonLdContextNormalized } from "jsonld-context-parser"

const contextParser = new ldcp.ContextParser()


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

    // ATTENTION: For correct matching by the tokenizer, it is required that the symbols are ordered by decreasing length!    
    private readonly tokenizerDetectableSymbols = ['!~=', '==', '!=', '>=', '<=', '~=', '>', '<', ';', '|', '(', ')']

    // ATTENTION: The order of the logical operators in this list defines their priority (e.g. AND ";" over OR "|") !
    private readonly operators = ['!~=', '==', '!=', '>=', '<=', '~=', '>', '<', ';', '|']


    private readonly ERROR_STRING_INTRO = "Invalid query string: "

    private readonly nonReifiedDefaultProperties = ["https://uri.etsi.org/ngsi-ld/createdAt", 
                                                    "https://uri.etsi.org/ngsi-ld/modifiedAt", 
                                                    "https://uri.etsi.org/ngsi-ld/observedAt", 
                                                    "https://uri.etsi.org/ngsi-ld/datasetId",                                                     
                                                    "https://uri.etsi.org/ngsi-ld/unitCode"]



    constructor(private tableCfg: PsqlTableConfig) {}


    makeQuerySql(query: Query, context : JsonLdContextNormalized, attr_table : string): string {

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


    private build(ast: Array<any>, context : ldcp.JsonLdContextNormalized, attrTable : string): string {
        
        let result = "("

        // Check for existence of attribute (regardless of its value):
        if (typeof(ast) == "string") {
    
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
            //result += `SELECT instance_id FROM ${this.tableCfg.TBL_ATTR} WHERE ${this.tableCfg.TBL_ATTR}.attr_name = '${firstPathPiece_expanded}' AND `
            
            
            result += "("

            // Check existence of non-reified property:
            if (this.nonReifiedDefaultProperties.includes(lastPathPiece_expanded)) {
               result += `${attrTable}.${this.tableCfg.COL_INSTANCE_JSON}${attrPathSql} is not null `
               
            }

            // Check existence of reified Property ot Relationship:
            else {
                
                //########### BEGIN Check existence of Property ##############
                result += "("
                result += `${attrTable}.${this.tableCfg.COL_INSTANCE_JSON}${attrPathSql}->>'@type' = 'https://uri.etsi.org/ngsi-ld/Property'`
                result += " AND "
                result += `${attrTable}.${this.tableCfg.COL_INSTANCE_JSON}${attrPathSql}->'https://uri.etsi.org/ngsi-ld/hasValue' is not null`
                result += ")"
                //########### END Check existence of Property ##############

                result += " OR "

                //########### BEGIN Check existence of Relationship ##############
                result += "("
                result += `${attrTable}.${this.tableCfg.COL_INSTANCE_JSON}${attrPathSql}->>'@type' = 'https://uri.etsi.org/ngsi-ld/Relationship'`
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
                    throw errorTypes.InvalidRequest.withDetail("Invalid query string: Query term operator unknown: '" + op + "'.")
                }
            }
        }
        else {
            throw errorTypes.InvalidRequest.withDetail(`Invalid query string: Invalid query term: '${ast.toString()}'`)
        }

     
        result += ")"

        return result
    }


    // Spec 4.9
    private blubb(leftSide: string, op: string, rightSide: string, context : ldcp.JsonLdContextNormalized, attrTable : string): string {


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


        let jsonFullPathSql = ""

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
        jsonFullPathSql = jsonAttrPathSql

        // If we have a trailing path, let's add it to the main path:
        if (trailingPath != null) {

            // ATTENTION: Note that we access "value" as a JSON OBJECT here ("->" operator), 
            // and not as its direct value ("->>" operator)!!

            jsonFullPathSql += `->'https://uri.etsi.org/ngsi-ld/hasValue'`

            for (let ii = 0; ii < trailingPath.length; ii++) {
                const key = trailingPath[ii]

                const expandedKey = context.expandTerm(key, true)


                // For the last element, we change the JSON accessor to "->>" to access its text content:
                if (ii == trailingPath.length - 1) {
                    jsonFullPathSql += `->>'${expandedKey}'`
                }
                else {
                    jsonFullPathSql += `->'${expandedKey}'`
                }
            }
        }

        // If we have no trailing path, we access the value of the last path element directly:
        else {
            // ATTENTION: As opposed to the case above where we have a trailing path,
            // we access "value" as its direct value here ("->>" Operator)!         

            if (!(this.nonReifiedDefaultProperties.includes(attrPath[attrPath.length - 1]))) {
                jsonFullPathSql += `->>'https://uri.etsi.org/ngsi-ld/hasValue'`
            }
        }
        //############# END Build Complete attribute path expression (with trailing path) ##############           

        const firstPathPiece = context.expandTerm(attrPath[0], true)

        // Begin construction of SQL query string:
        let result = `SELECT eid FROM ${attrTable} WHERE ${attrTable}.attr_name = '${firstPathPiece}' `
         

    

        const range = rightSide.split("..")

        if (range.length == 1) {
            result += this.buildSingleValueCompare(range, op, jsonFullPathSql, jsonAttrPathSql)
        }
        else if (range.length == 2) {
            result += this.buildRangeCompare(range, op, jsonFullPathSql)
        }
        else {
            throw errorTypes.InvalidRequest.withDetail(this.ERROR_STRING_INTRO + "A range must contain only one instance of '..'.")
        }

        //################ END Compare expression ################

        return result
    }


    private buildSingleValueCompare(range: Array<any>, op: String, jsonFullPathSql: string, jsonAttrPathSql: string) {

        let result = " AND "

        const compareType = this.figureOutValueType(range)

        //################ BEGIN Compare expresision ################      

        switch (compareType) {
            case CompareValueType.BOOLEAN: {
                result += `(${jsonFullPathSql})::boolean ${op} ${range[0]}`
                break
            }
            case CompareValueType.DATE: {
                result += `(${jsonFullPathSql})::timestamp ${op} '${range[0]}'`
                break
            }
            case CompareValueType.TIME: {
                result += `(${jsonFullPathSql})::timestamp::time ${op} '${range[0]}'`
                break
            }
            case CompareValueType.DATETIME: {
                result += `(${jsonFullPathSql})::timestamp ${op} '${range[0]}'`
                break
            }
            case CompareValueType.NUMBER: {
                result += `(${jsonFullPathSql})::numeric ${op} ${range[0]}`
                break
            }
            case CompareValueType.QUOTEDSTR: {
                // NOTE: With the substr(), we remove the beginning and end quotes:
                result += `(${jsonFullPathSql})::text ${op} '${range[0].substr(1, range[0].length - 2)}'`
                break
            }
            case CompareValueType.URI: {
                // NOTE: Compare expression for Relationships is different, so we don't set test1 here and
                // write the Relationship expression below if test1 == null.
                // NOTE: For Relationship queries, the trailing path does not play a role:
                result += `${jsonAttrPathSql}->>'@type' = 'https://uri.etsi.org/ngsi-ld/Relationship' AND ${jsonAttrPathSql}->>'https://uri.etsi.org/ngsi-ld/hasObject' = '${range[0]}'`
                break
            }
            default: {
                throw errorTypes.InvalidRequest.withDetail(this.ERROR_STRING_INTRO + "Unable to determine type of compare value: " + range[0])
            }
        }

        return result
    }



    private buildRangeCompare(range: Array<string>, op: String, jsonFullPathSql: string) {

        // TODO: What to do if right end of range is smaller than left end?

        const compareType = this.figureOutValueType(range)

        let result = " AND "

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
                const cv1 = range[0].substr(1, range[0].length - 2)
                const cv2 = range[1].substr(1, range[1].length - 2)

                result += `(${jsonFullPathSql})::text >= '${cv1}' AND (${jsonFullPathSql})::text <= '${cv2}`
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



    private figureOutValueType(range: Array<string>) {

        // NOTE: He, we do two things:
        // 1. Determine the value type of the range expression
        // 2. Check whether both the min and max value of the range have the same type

        // Also not that the type determined here is used for individual compare values as well 
        // (i.e. consider individual compare values as "ranges with same min and max").

        let rightSideType = CompareValueType.UNKNOWN

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

            if (rightSideType != CompareValueType.UNKNOWN && newType != rightSideType) {
                throw errorTypes.InvalidRequest.withDetail(this.ERROR_STRING_INTRO + "Types of range min and max values must be equal.")
            }

            rightSideType = newType
        }

        return rightSideType
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

        while (query.length > 0) {

            let symbolFound = null

            //########### BEGIN Test for known symbol #########

            // ATTENTION: The following for loop only works correctly if self.symbols is ordered by item string length!

            for (let symbol of this.tokenizerDetectableSymbols) {

                if (query.substr(0, symbol.length) == symbol) {
                    symbolFound = symbol
                    break
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

        // Add last token to result:
        if (collected.length > 0) {
            result.push(collected)
        }

        return result
    }
}
