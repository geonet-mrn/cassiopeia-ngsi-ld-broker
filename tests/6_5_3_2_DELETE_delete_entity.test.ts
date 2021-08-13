import { expect, assert } from "chai";
import axios, { AxiosResponse } from 'axios'
import * as prep from "./testUtil"
import { testConfig } from './testConfig'

const entityIdToDelete = "urn:ngsi-ld:Municipality:07332009"


const config = {
    headers: {
        "content-type": "application/ld+json"
    },
    auth: testConfig.auth
}


const entities = [
    {
        "id": entityIdToDelete,
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
    }
]



describe('6.5.3.2 DELETE entities/<entityId>', function () {

    before(async () => {
        await prep.deleteAllEntities()

    })


    after(async () => {
        await prep.deleteAllEntities()

    })


    it('should delete the Entity with the specified ID', async function () {



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

        expect(response.data.length).equals(2)



        const numEntitiesBeforeDelete = response.data.length

        // Step 2: Check whether the entity we are going to delete exists:

        let entityUrl = queryUrl + entityIdToDelete

       
        response = await axios.get(entityUrl, config)

        expect(response.data.id).equals(entityIdToDelete)




        response = await axios.delete(entityUrl, config)

        expect(response.status).equals(204)


        // Step 2: Check whether all entities are there:       


        response = await axios.get(queryUrl, config)

        expect(response.data.length).greaterThan(0)

        const numEntitiesAfterDelete = response.data.length

        expect(numEntitiesBeforeDelete - numEntitiesAfterDelete).equals(1)



        // Step 3: Check whether exactly the specified entity was deleted:
        let getResponse = await prep.axiosGet(entityUrl, config)

        expect(getResponse.status).equals(404)
    });
});

