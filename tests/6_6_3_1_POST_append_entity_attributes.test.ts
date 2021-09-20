import { expect, assert } from "chai";
import axios, { AxiosResponse } from 'axios'
import * as prep from "./testUtil"
import { testConfig } from './testConfig'

const entityId = "urn:ngsi-ld:Municipality:07337059"

const originalEntity = {
    "id": entityId ,
    "type": "TestEntity",

    "name": [
        {
            "type": "Property",
            "value": "Oberotterbach"
        }
    ]
}

const config = {
    headers: {
        "content-type": "application/ld+json"
    },
    auth: testConfig.auth
}



describe('6.6.3.1 POST entities/<entityId>/attrs/', function () {

    before(async () => {
        await prep.deleteAllEntities()
    })


    after(async () => {
        await prep.deleteAllEntities()
    })


    it("should append the attributes provided in the uploaded NGSI-LD fragment to the entity specified by the URL path", async function () {

        const entityUrl = testConfig.base_url + "entities/" + originalEntity.id

        //###################### BEGIN Step 1 ######################
        let createEntityResponse = await axios.post(testConfig.base_url + "entities/", originalEntity, config).catch((e) => {
            console.log(e)
        }) as AxiosResponse

        expect(createEntityResponse.status).equals(201)
        //###################### END Step 1 ######################



        //###################### BEGIN Step 2 ######################

        const appendFragment1 = {
            "id": entityId,
            "type": "TestEntity",
        
            "appendedAttribute": [
                {
                    "type": "Property",
                    "value": "appendedValue"
                }
            ]
        }

        let appendAttributesResponse = await axios.post(entityUrl + /attrs/, appendFragment1, config).catch((e) => {
            console.log(e)
        }) as AxiosResponse

      
        // TODO: 1 Why 204? When should we expect 207?
        expect(appendAttributesResponse.status).equals(204)
        //###################### END Step 2 ######################



        //###################### BEGIN Step 3 ######################

        const getModifiedEntityResponse = await axios.get(entityUrl)
    
        expect(getModifiedEntityResponse.status).equals(200)

        const modifiedEntity = getModifiedEntityResponse.data

        expect(modifiedEntity['appendedAttribute']).instanceOf(Array)
        //###################### END Step 3 ######################

    })



    
    it("should NOT append an attribute instance with default datasetId if an instance with default dataset id already exists and noOverwrite is set to true", async function () {

        const appendFragment = {
            "id": entityId,
            "type": "TestEntity",

            "name": [{
                "type": "Property",
                "value": "appended"
            }]
        }

        // Patch entity through normal API:
        let err2: any = undefined

        let url = testConfig.base_url + "entities/" + entityId + "/attrs/?options=noOverwrite"
        
        let patchResponse = await axios.post(url, appendFragment, config).catch((e) => {
            err2 = e
        }) as AxiosResponse

        expect(patchResponse).to.not.be.undefined

        expect(patchResponse.status).equals(207)


        let getResponse = await axios.get(testConfig.base_url + "entities/" + entityId).catch((e) => {
            err2 = e
        }) as AxiosResponse

        expect(getResponse).to.not.be.undefined

        const entity = getResponse.data

        expect(entity.name[0].value).equals("Oberotterbach")
    })
    


    
    it("should overwirte an attribute instance with default datasetId if an instance with default dataset id is passed and noOverwrite is not set", async function () {

        const appendFragment = {
            "id": entityId,
            "type": "TestEntity",

            "name": [{
                "type": "Property",
                "value": "appendUpdateName"
            }]
        }

        // Patch entity through normal API:
        let err2: any = undefined

        let url = testConfig.base_url + "entities/" + entityId + "/attrs/"
        
        let patchResponse = await axios.post(url, appendFragment, config).catch((e) => {
            err2 = e
        }) as AxiosResponse

        expect(patchResponse).to.not.be.undefined

        expect(patchResponse.status).equals(204)


        let getResponse = await axios.get(testConfig.base_url + "entities/" + entityId).catch((e) => {
            err2 = e
        }) as AxiosResponse

        expect(getResponse).to.not.be.undefined

        const entity = getResponse.data

        expect(entity.name[0].value).equals("appendUpdateName")

    })
    
});