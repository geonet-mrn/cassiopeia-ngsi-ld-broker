import { expect, assert } from "chai";
import axios, { AxiosResponse } from 'axios'
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
    },

    {
        "id": "urn:ngsi-ld:Municipality:08222000",
        "type": "Municipality",
        "name": [{ "type": "Property", "value": "Mannheim" }]
    }
]



describe('6.5.3.1 GET entities/<entity_id>', function () {

    beforeEach(async () => {
        await prep.deleteAllEntities()

    })


    afterEach(async () => {
        await prep.deleteAllEntities()

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


        let getUrl = testConfig.base_url + "entities/urn:ngsi-ld:Municipality:07332009"

        const response = await axios.get(getUrl, config)



        expect(response.data.id).equal("urn:ngsi-ld:Municipality:07332009")


    });
});
