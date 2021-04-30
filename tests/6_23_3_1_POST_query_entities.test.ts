import { expect, assert } from "chai";
import axios, { AxiosRequestConfig, AxiosResponse } from 'axios'
import * as uuid from 'uuid'
import * as prep from "./testPreparation"
import { testConfig } from './testConfig'


async function createEntity() {



    const entity = {


        "id": "urn:xdatatogo:TrafficRestriction:test",
        "type": "TrafficRestriction",

        "dateValidFrom": {
            "type": "Property",
            "value": {
                "@type": "DateTime",
                "@value": "2021-02-08T06:00:00.000Z"
            }
        },
        "dateValidUntil": {
            "type": "Property",
            "value": {
                "@type": "DateTime",
                "@value": "2021-02-08T06:00:00.000Z"
            }
        },
        "location": {
            "type": "GeoProperty",
            "value": {
                "type": "Point",
                "coordinates": [50, 50]
            }
        },
        "maxSpeed": {
            "type": "Property",
            "value": 123
        },
        "maxVehicleAxleLoad": {
            "type": "Property",
            "value": 123
        },
        "maxVehicleHeight": {
            "type": "Property",
            "value": 123
        },
        "maxVehicleWeight": {
            "type": "Property",
            "value": 123
        },
        "maxVehicleWidth": {
            "type": "Property",
            "value": 123
        }
    }

    let config: AxiosRequestConfig = {
        headers: { "content-type": "application/ld+json" },
        auth: testConfig.auth
    }

    // Create entity:


    let response = await axios.post(testConfig.base_url + "entities/", entity, config).catch((e) => {
        console.log(e)
    })

    return new Promise<void>((resolve, reject) => {
        resolve()
    })
}


describe('POST entityOperations/query', function () {

    before(async () => {
        await prep.deleteAllEntities()

        await createEntity()

        return new Promise<void>((resolve, reject) => {
            resolve()
        })
    })


    after(async () => {
        await prep.deleteAllEntities()

        return new Promise<void>((resolve, reject) => {
            resolve()
        })
    })





    it("should return the entities that match the property query", async function () {

        const query = {
            "@context": "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.3.jsonld",
            "q": "maxSpeed==123",
            "type": "Query"

        }

        let config = {
            headers: {
                "content-type": "application/ld+json"
            }
        }


        let response = await axios.post(testConfig.base_url + "entityOperations/query", query, config).catch((e) => {
            console.log(e)
        }) as AxiosResponse



        expect(response.data.length).equals(1)
        //console.log(response.data)

        expect(response.data[0].id).equals("urn:xdatatogo:TrafficRestriction:test")

        return new Promise<void>((resolve, reject) => {
            resolve()
        })
    })



    it("should return an empty array because no existing entities match the query", async function () {
        const query = {
            "@context": "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.3.jsonld",

            geoQ: {
                "georel": "near;maxDistance==1000",
                "geometry": "Point",
                // NOTE: Actual coordinates of existing entity are [50,50]
                "coordinates": [0, 0],
                "geoproperty": "location"
            },
            "type": "Query"
        }

        let config = {
            headers: {

                "content-type": "application/ld+json"
            },
            auth: testConfig.auth
        }


        let response = await axios.post(testConfig.base_url + "entityOperations/query/", query, config).catch((e) => {
            console.log(e)
        }) as AxiosResponse


        assert(response)

        expect(response.data).instanceOf(Array)
        expect(response.data.length).equals(0)

        return new Promise<void>((resolve, reject) => {
            resolve()
        })
    })


    it("should return and array with one existing entity that matches the geo query", async function () {

        const query = {
            "@context": "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.3.jsonld",

            geoQ: {
                "georel": "near;maxDistance==0",
                "geometry": "Point",
                // NOTE: Actual coordinates of existing entity are [50,50]
                "coordinates": [50, 50],
                "geoproperty": "location"
            },
            "type": "Query"
        }

        let config = {
            headers: {
                "content-type": "application/ld+json"
            },
            auth: testConfig.auth
        }


        let response = await axios.post(testConfig.base_url + "entityOperations/query/", query, config).catch((e) => {
            console.log(e)
        }) as AxiosResponse

        expect(response.data).instanceOf(Array)

        expect(response.data[0].id).equals("urn:xdatatogo:TrafficRestriction:test")


        return new Promise<void>((resolve, reject) => {
            resolve()
        })
    })
});

