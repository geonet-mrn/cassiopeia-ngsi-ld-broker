import { expect, assert } from "chai";
import axios, { AxiosRequestConfig, AxiosResponse } from 'axios'
import * as prep from "./testUtil"
import { testConfig } from './testConfig'



const entities = [
    {
        "id": "urn:ngsi-ld:Municipality:07332009",
        "type": "Municipality",
        "verwaltungsgemeinschaft": [
            {
                "type": "Property",
                "value": "Deidesheim"
            }
        ],
        "name": [
            {
                "type": "Property",
                "value": "Deidesheim"
            }
        ]
    },
    {
        "id": "urn:ngsi-ld:Municipality:07332017",
        "type": "Municipality",
        "verwaltungsgemeinschaft": [
            {
                "type": "Property",
                "value": "Deidesheim"
            }
        ],
        "name": [
            {
                "type": "Property",
                "value": "Forst an der Weinstraße"
            }
        ]
    },
    {
        "id": "urn:ngsi-ld:Municipality:07332035",
        "type": "Municipality",
        "verwaltungsgemeinschaft": [
            {
                "type": "Property",
                "value": "Deidesheim"
            }
        ],
        "name": [
            {
                "type": "Property",
                "value": "Meckenheim"
            }
        ]
    },

    {
        "id": "urn:ngsi-ld:Municipality:08226101",
        "type": "Municipality",
        "verwaltungsgemeinschaft": [
            {
                "type": "Property",
                "value": "VVG der Stadt Sinsheim"
            }
        ],
        "name": [
            {
                "type": "Property",
                "value": "Zuzenhausen"
            }
        ]
    }
]



describe('6.15.3.1 POST entityOperations/upsert', function () {

    before(async () => {
        await prep.deleteAllEntities()
    })


    after(async () => {
        await prep.deleteAllEntities()
    })





    it("should create and update the entities", async function () {

        let response: AxiosResponse | undefined

        // Step 1: Check whether there are really no entities in the broker:
        try {
            response = await axios.get(testConfig.base_url + 'entities/?type=Municipality')

        }
        catch (e) {

        }

        if (response != undefined) {
            expect(response.data.length).equals(0)
        }

        // Step 2: Batch create entities:
        let config = {
            headers: {
                "content-type": "application/ld+json"
            },
            auth: testConfig.auth
        }


        const createUrl = testConfig.base_url + "entityOperations/upsert"

        response = undefined

        try {
            response = await axios.post(createUrl, entities, config)

        }
        catch (e) {
            console.log(e)
        }


        if (response != undefined) {
            expect(response.status).equals(201)
        }


        // Step 3: Check whether created entities really exist:

        try {
            response = await axios.get(testConfig.base_url + 'entities/?type=Municipality')

        }
        catch (e) {

        }

        if (response != undefined) {
            expect(response.data.length).equals(entities.length)
        }

    })


});

