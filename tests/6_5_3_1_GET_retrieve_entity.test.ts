import { expect, assert } from "chai";
import axios, { AxiosResponse } from 'axios'
import * as uuid from 'uuid'
import * as prep from "./testPreparation"
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
    },

    {
        "id": "urn:ngsi-ld:Municipality:08222000",
        "type": "Municipality",
        "name": [{ "type": "Property", "value": "Mannheim" }]
    }
]



describe('GET entities/<entity_id>', function () {

    before(async () => {
        await prep.deleteAllEntities()

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


    it('should return the Entity with the ID specified in the URL', async function () {


        const config = {
            headers: {
                "content-type": "application/ld+json"
            },
            auth: testConfig.auth
        }





        //###################### BEGIN Create entities for test ######################
        const createUrl = testConfig.base_url + "entityOperations/upsert"

        let createEntitiesResponse = await axios.post(createUrl, entities, config).catch((e) => {
            //console.log(e)

        }) as AxiosResponse

        expect(createEntitiesResponse.status).equals(201)
        //###################### END Create entities for test ######################


        let url = testConfig.base_url + "entities/urn:ngsi-ld:Municipality:07332009"

        const response = await axios.get(url, config)



        expect(response.data.id).equal("urn:ngsi-ld:Municipality:07332009")


        return new Promise<void>((resolve, reject) => {
            resolve()
        })

    });
});

