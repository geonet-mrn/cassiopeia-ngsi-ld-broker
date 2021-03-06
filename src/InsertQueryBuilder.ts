export class SqlQueryBuilder {

    private fields = Array<string>()
    private values = Array<any>()

    

    add(field: string, value: any, noQuotes: boolean = false) {

        if (value === undefined) {
            return
        }

        // TODO: 4 How to handle unexpected/unsupported values?
        let valueString : string|undefined = undefined

        if (typeof (value) == "number" || (typeof (value) == "string" && noQuotes)) {
            valueString = `${value}`
        }

        else if (typeof (value) == "string") {
            valueString = `'${value}'`
        }
        else if (value === null) {
            valueString = 'null'
        }

        this.fields.push(field)
        this.values.push(valueString)
    }


    getInsertQueryForTable(tableName: string, returnField : string |undefined = undefined): string {
        let result =  `INSERT INTO ${tableName} (${this.fields.join(",")}) VALUES (${this.values.join(",")})`

        if (returnField != undefined){
            result += " returning " + returnField
        }

        return result
    }



    getUpdateQueryForTable(tableName: string): string {
        
        let assignments = Array<any>()

        for(let ii = 0; ii < this.fields.length;ii++) {
            assignments.push(this.fields[ii] + " = " + this.values[ii])
        }

        let result =  `UPDATE ${tableName} SET ${assignments.join(", ")}`

        return result
    }


    getCommaPairs(): string {
        let assignments = Array<any>()

        for(let ii = 0; ii < this.fields.length;ii++) {
            assignments.push(this.fields[ii] + " = " + this.values[ii])
        }

        return assignments.join(" , ")
    }
}