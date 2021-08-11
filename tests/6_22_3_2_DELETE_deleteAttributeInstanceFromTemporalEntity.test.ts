import { expect, assert } from "chai";
import axios, { AxiosRequestConfig, AxiosResponse } from 'axios'
import * as prep from "./testUtil"
import { testConfig } from './testConfig'




let config = {
    headers: { "content-type": "application/ld+json" },
    auth: testConfig.auth
}


const entityId = "urn:ngsi-ld:TemporalTestEntity:test"

const temporalEntity = {


    "id": entityId,
    "type": "TemporalTestEntity",

    "testProperty": [{
        "type": "Property",
        "value": 1
    }],


    "propertyToDelete": [
        {
            "type": "Property",
            "value": 1,
            "datasetId": "urn:ngsi-ld:notdefault1"
        },
        {
            "type": "Property",
            "value": 1,
            "datasetId": "urn:ngsi-ld:notdefault2"
        },
        {
            "type": "Property",
            "value": 2
        }]
}


describe('6.22.3.2 DELETE temporal/entities/<entityId>/attrs/<attrId>/<instanceId>', function () {



    before(async () => {
        await prep.deleteAllEntities()
    })


    after(async () => {
        await prep.deleteAllEntities()


    })



    it("should delete the attribute instance with the specified instance id from the specified temporal entity", async function () {


        //############# BEGIN Create entity ##############

        let err: any = undefined

        let createResponse = await axios.post(testConfig.base_url + "temporal/entities/", temporalEntity, config).catch((e) => {
            err = e
        }) as AxiosResponse

        expect(createResponse).to.not.be.undefined

        expect(createResponse.status).equals(201)
        //############# END Create entity ##############


        //############## BEGIN Fetch created entity to know the attribute instances ##############
        let fetchResponse = await axios.get(testConfig.base_url + "temporal/entities/" + entityId)

        expect(fetchResponse).to.not.be.undefined

        const fetchedEntity = fetchResponse.data
        //############## END Fetch created entity to know the attribute instances ##############



        //################ BEGIN Delete attribute instance with specific instance id ###############
        expect(fetchedEntity.propertyToDelete.length).equals(3)

        let instanceId = fetchedEntity.propertyToDelete[0].instanceId

        const deleteUrl = testConfig.base_url + "temporal/entities/" + entityId + "/attrs/propertyToDelete/" + instanceId
       
        err = undefined
        let deleteResponse: any = await axios.delete(deleteUrl, config).catch((e) => {
            err = e
        })

        if (err != undefined) {
            console.log(err)
        }

        expect(deleteResponse).to.not.be.undefined

        if (deleteResponse != undefined) {
            expect(deleteResponse.status).equals(204)
        }
        //################ END Delete attribute instance with specific instance id ###############


        //############## BEGIN Fetch entity again to check whether the specified attribute instance was really deleted ##############
        let fetchResponse2 = await axios.get(testConfig.base_url + "temporal/entities/" + entityId)

        expect(fetchResponse2).to.not.be.undefined

        const e2 = fetchResponse2.data

        
        expect(e2.propertyToDelete).to.not.be.undefined

        expect(e2.propertyToDelete.length).equals(2)

        for(const instance of e2.propertyToDelete) {
            expect(instance.instanceId).to.not.equal(instanceId)
        }
        //############## END Fetch entity again to check whether the specified attribute instance was really deleted ##############

    })

});

