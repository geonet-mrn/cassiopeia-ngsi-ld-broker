import { expect, assert } from "chai";
import axios, { AxiosRequestConfig, AxiosResponse } from 'axios'
import * as prep from "./testUtil"
import { testConfig } from './testConfig'
import * as uuid from 'uuid'


let config = {
    headers: { "content-type": "application/ld+json" },
    auth: testConfig.auth
}

   
const entityId = "urn:ngsi-ld:TemporalTestEntity:test" + uuid.v4()



describe('6.18.3.1 POST temporal/entities/', function () {

    before(async () => {
        await prep.deleteAllEntities()



    })


    after(async () => {
        await prep.deleteAllEntities()
    })





    it("should create a temporal entity", async function () {

    
        const temporalEntity = {


            "id": entityId,
            "type": "TemporalTestEntity",

            "testProperty": [{
                "type": "Property",
                "value": 1
            },
            {
                "type": "Property",
                "value": 2
            }]
        }



        // Create entity:
        let err: any = undefined

        let createResponse = await axios.post(testConfig.base_url + "temporal/entities/", temporalEntity, config).catch((e) => {
            err = e
        }) as AxiosResponse

        expect(createResponse).to.not.be.undefined

        expect(createResponse.status).equals(201)


        // TODO: Check presence of location header in response

        err = undefined

        let getResponse = await axios.get(testConfig.base_url + "temporal/entities/" + entityId, config).catch((e) => {
            err = e
        }) as AxiosResponse


        if (err != undefined) {
            console.log(err)
        }

        expect(getResponse).to.not.be.undefined

        expect(getResponse.data.id).equals(entityId)


    })





    it("should update the previously created temporal entity (i.e. append new attributes)", async function () {


        const temporalEntityUpdate = {


            "id": entityId,
            "type": "TemporalTestEntity",

            "testProperty": [{
                "type": "Property",
                "value": 3
            },
            {
                "type": "Property",
                "value": 5,
                "datasetId" : "urn:ngsi-ld:DatasetId:Test"
            },
            {
                "type": "Property",
                "value": 7,
                "datasetId" : "urn:ngsi-ld:DatasetId:Test"
            }]
        }



        // Update entity (append attributes):

        let err: any = undefined

        let createResponse = await axios.post(testConfig.base_url + "temporal/entities/", temporalEntityUpdate, config).catch((e) => {
            err = e
        }) as AxiosResponse

        expect(createResponse).to.not.be.undefined

        expect(createResponse.status).equals(204)


        // TODO: Check presence of location header in response

        err = undefined

        let getResponse = await axios.get(testConfig.base_url + "temporal/entities/" + entityId, config).catch((e) => {
            err = e
        }) as AxiosResponse


        if (err != undefined) {
            console.log(err)
        }

        expect(getResponse).to.not.be.undefined

        expect(getResponse.data.id).equals(entityId)



        expect(getResponse.data.testProperty.length).equals(5)
    
        
        
       
    })




    // NOTE: This tests new behaviour that was implemented from 2021-08-11 on:

    it("should make the entity available through the 'normal' API (non-temporal) as well", async function () {


        const url = testConfig.base_url + "entities/" + entityId

        
        const response = await axios.get(url)

        expect(response).to.not.be.undefined

        const entity = response.data


        // The property 'testProperty' should have only one instance:

        console.log(entity.testProperty)
        expect(entity.testProperty.length).equals(2)
        
    })


});

