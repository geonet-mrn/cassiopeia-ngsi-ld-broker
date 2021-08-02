import { expect, assert } from "chai";
import axios, { AxiosResponse } from 'axios'
import * as prep from "./testUtil"
import { testConfig } from './testConfig'


const originalEntity = {
    "id": "urn:ngsi-ld:TestEntity1",
    "type": "TestEntity",

    "testProp1": [
        {
            "type": "Property",
            "value": "before"
        }
    ],

    "testProp2": [
        {
            "type": "Property",
            "value": "another value before"
        }
    ],
}



const updateAttributesFragment = {
    "id": "urn:ngsi-ld:TestEntity1",
    "type": "TestEntity",

    "testProp1": [
        {
            "type": "Property",
            "value": "after"
        }
    ]
}


describe('6.6.3.2 PATCH entities/<entityId>/attrs/', function () {

    beforeEach(async () => {
        await prep.deleteAllEntities()

    })


    afterEach(async () => {
        await prep.deleteAllEntities()

    })





    it("should append the attributes provided in the uploaded NGSI-LD fragment to the entity specified by the URL path", async function () {

        const config = {
            headers: {
                "content-type": "application/ld+json"
            },
            auth: testConfig.auth
        }

        const entityUrl = testConfig.base_url + "entities/" + originalEntity.id



        //###################### BEGIN Step 1 ######################
        let createEntityResponse = await axios.post(testConfig.base_url + "entities/", originalEntity, config).catch((e) => {
            console.log(e)
        }) as AxiosResponse

        expect(createEntityResponse.status).equals(201)
        //###################### END Step 1 ######################



        //###################### BEGIN Step 2 ######################
        let updateAttributesResponse = await axios.patch(entityUrl + /attrs/, updateAttributesFragment, config).catch((e) => {
            console.log(e)
        }) as AxiosResponse

      
        
        expect(updateAttributesResponse.status).equals(204)
        //###################### END Step 2 ######################



        //###################### BEGIN Step 3 ######################

        const getModifiedEntityResponse = await axios.get(entityUrl)
    
        expect(getModifiedEntityResponse.status).equals(200)

        const modifiedEntity = getModifiedEntityResponse.data

        expect(modifiedEntity.testProp1[0].value).equals("after")
        //###################### END Step 3 ######################

    })
});