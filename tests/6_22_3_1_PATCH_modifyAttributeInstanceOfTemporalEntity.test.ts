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

  

    "propertyToPatch": [
        {
            "type": "Property",
            "value": "before",
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



const patchFragment = {

    "id": entityId,
    "type": "TemporalTestEntity",


    "propertyToPatch": [
        {
            "type": "Property",
            "value": "after",

        }]
}


describe('6.23.3.2 PATCH temporal/entities/<entityId>/attrs/<attrId>/<instanceId>', function () {



    before(async () => {
        await prep.deleteAllEntities()
    })


    after(async () => {
        await prep.deleteAllEntities()


    })



    it("should patch the attribute instance with the specified instance id from the specified temporal entity", async function () {


        //############# BEGIN Create entity ##############

        let err: any = undefined

        let createResponse = await axios.post(testConfig.base_url + "temporal/entities/", temporalEntity, config).catch((e) => {
            err = e

            console.log(err.response.data)
        }) as AxiosResponse

        expect(createResponse).to.not.be.undefined

        expect(createResponse.status).equals(201)
        //############# END Create entity ##############


        //############## BEGIN Fetch created entity to know the attribute instances ##############
        let fetchResponse = await axios.get(testConfig.base_url + "temporal/entities/" + entityId)

        expect(fetchResponse).to.not.be.undefined

        const fetchedEntity = fetchResponse.data
        //############## END Fetch created entity to know the attribute instances ##############



        //################ BEGIN Patch attribute instance with specific instance id ###############
        expect(fetchedEntity.propertyToPatch.length).greaterThan(0)

         let instanceId = fetchedEntity.propertyToPatch[0].instanceId

        const patchUrl = testConfig.base_url + "temporal/entities/" + entityId + "/attrs/propertyToPatch/" + instanceId

     
        err = undefined
        let patchResponse: any = await axios.patch(patchUrl, patchFragment, config).catch((e) => {
            err = e
        })

        if (err != undefined) {
            console.log(err.response.data)
        }

        expect(patchResponse).to.not.be.undefined

        if (patchResponse != undefined) {
            expect(patchResponse.status).equals(204)
        }
        //################ END Delete attribute instance with specific instance id ###############


        //############## BEGIN Fetch entity again to check whether the specified attribute instance was really deleted ##############
        let fetchResponse2 = await axios.get(testConfig.base_url + "temporal/entities/" + entityId, config)

        expect(fetchResponse2).to.not.be.undefined

        const e2 = fetchResponse2.data

        
        expect(e2.propertyToPatch).to.not.be.undefined

        expect(e2.propertyToPatch.length).greaterThan(0)

        let patchSuccess = false

        for (const instance of e2.propertyToPatch) {
            if (instance.instanceId == instanceId) {
                console.log("INSTANCE ID MATCH")
               
                expect(instance.value).equals("after")
                patchSuccess = true
            }
        }

        expect(patchSuccess).equals(true)
        //############## END Fetch entity again to check whether the specified attribute instance was really deleted ##############

    })

});

