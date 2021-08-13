import { expect, assert } from "chai";
import axios, { AxiosResponse } from 'axios'
import * as prep from "./testUtil"
import { testConfig } from './testConfig'


const config = {
    headers: {
        "content-type": "application/ld+json"
    },
    auth: testConfig.auth
}

const originalEntity = {
    "id": "urn:ngsi-ld:TestEntity1",
    "type": "TestEntity",

    "testProp1": [
        {
            "type": "Property",
            "value": "before"
        },

        {
            "type": "Property",
            "value": "another value before",
            "datasetId": "urn:ngsi-ld:DatasetId:dataset1"
        }
    ],

    "testProp2": [
        {
            "type": "Property",
            "value": "and something else again"
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

    before(async () => {
        await prep.deleteAllEntities()

    })


    after(async () => {
        await prep.deleteAllEntities()
    })





    it("should update the specified existing entity with the attributes provided in the uploaded NGSI-LD fragment", async function () {

        // 5.6.2.4:
        // For each of the Attributes included in the Fragment, if the target Entity includes a matching one (considering
        // term expansion rules as mandated by clause 5.5.7), then replace it by the one included by the Fragment:

       

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

        let instanceFound = false
        let otherInstanceFound = false

        for(const instance of modifiedEntity.testProp1) {

            // The instance with default dataset ID (i.e. undefined) should be changed:
            if (instance.datasetId === undefined) {
                instanceFound = true
                expect(instance.value).equals("after")
            }

            // The other instance should not be changed:
            else if(instance.datasetId == "urn:ngsi-ld:DatasetId:dataset1") {
                expect(instance.value).equals("another value before")
                otherInstanceFound = true
            }
        }
        
        expect(instanceFound).equals(true)
        expect(otherInstanceFound).equals(true)

        //###################### END Step 3 ######################

    })
});