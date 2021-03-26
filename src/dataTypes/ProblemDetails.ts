// See https://tools.ietf.org/html/rfc7807

export class ProblemDetails extends Error {

    private instance = ""

    public noBody = false
    
    constructor(

        public type: string,
        public title: string,
        public detail: string,
        public status: number) {

        super()
    }


    withDetail(detail: string): ProblemDetails {
        return new ProblemDetails(this.type, this.title, detail, this.status)
    }
}