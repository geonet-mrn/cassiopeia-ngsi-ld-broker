export class InsertQueryBuilder {

    private fields = Array<string>()
    private values = Array<any>()

    

    add(field: string, value: any, noQuotes: boolean = false) {

        this.fields.push(field)

        // TODO: 4 How to handle unexpected/unsupported values?
        let valueString = "null"

        if (typeof (value) == "number" || (typeof (value) == "string" && noQuotes)) {
            valueString = `${value}`
        }

        else if (typeof (value) == "string") {
            valueString = `'${value}'`
        }

        this.values.push(valueString)
    }


    getStringForTable(tableName: string, returnField : string |undefined = undefined): string {
        let result =  `INSERT INTO ${tableName} (${this.fields.join(",")}) VALUES (${this.values.join(",")})`

        if (returnField != undefined){
            result += " returning " + returnField
        }

        //result += ";"
        
        return result
    }
}