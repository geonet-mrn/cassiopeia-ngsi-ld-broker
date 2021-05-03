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



describe('6.5.3.2 DELETE entities/<entity_id>', function () {

    beforeEach(async () => {
        await prep.deleteAllEntities()

    })


    afterEach(async () => {
        await prep.deleteAllEntities()

    })


    it('should delete the Entity with the ID', async function () {


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



        // Step 2: Check whether all entities are there:       
        let queryUrl = testConfig.base_url + "entities/"

        let response = await axios.get(queryUrl, config)

        expect(response.data.length).greaterThan(1)


        const numEntitiesBeforeDelete = response.data.length

        // Step 2: Check whether the entity we are going to delete exists:

        let url = testConfig.base_url + "entities/urn:ngsi-ld:Municipality:07332009"
        response = await axios.get(url, config)

        expect(response.data.id).equals("urn:ngsi-ld:Municipality:07332009")


        // Step 3: Delete the entity:
        url = testConfig.base_url + "entities/urn:ngsi-ld:Municipality:07332009"
        response = await axios.delete(url, config)

        expect(response.status).equals(204)


        // Step 2: Check whether all entities are there:       
        url = testConfig.base_url + "entities/"

        response = await axios.get(queryUrl, config)

        expect(response.data.length).greaterThan(1)

        const numEntitiesAfterDelete = response.data.length

        expect(numEntitiesBeforeDelete - numEntitiesAfterDelete).equals(1)


        // Step 3: Check whether exactly the specified entity was deleted:


        url = testConfig.base_url + "entities/urn:ngsi-ld:Municipality:07332009"

        let getResponse = undefined


        try {
            getResponse = await axios.get(url, config)
        }
        catch (e) {
       
            
            expect(e.response.data.status).equals(404)
        }


        expect(getResponse).equals(undefined)


    });
});

