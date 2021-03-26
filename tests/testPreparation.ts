import { expect } from "chai";
import * as fs from 'fs'
import axios, { AxiosRequestConfig, AxiosResponse } from 'axios'
import * as uuid from 'uuid'
import { testConfig } from './testConfig'




export function makeSuite(name: string, testsFunc: Function) {

    describe(name, function () {

        before(async function () {
            await deleteAllEntities()
        });

        testsFunc();

        after(async function () {
            await deleteAllEntities()
        });
    });
}


export async function deleteAllEntities() {
    let response = await axios.delete(testConfig.base_url + "entities/", { auth: testConfig.auth })
}


export async function createEntity(entityId: string) {



    const entity = {
        "@context": [
            "https://uri.geonet-mrn.de/xdatatogo/xdatatogo-context.jsonld",
            "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.3.jsonld"
        ],

        "id": entityId,
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

    if (response) {
        console.log(response.status)
    }

}