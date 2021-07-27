import { expect, assert } from "chai";
import axios, { AxiosRequestConfig, AxiosResponse } from 'axios'
import * as prep from "./testUtil"
import { testConfig } from './testConfig'



let config = {
    headers: { "content-type": "application/ld+json" },
    auth: testConfig.auth
}


const entityId = "urn:ngsi-ld:TemporalTestEntity:test"



describe('6.20.3.1 POST temporal/entities/<entityId>/attrs/', function () {

    before(async () => {
        await prep.deleteAllEntities()
    })


    after(async () => {
        await prep.deleteAllEntities()
    })





    it("should add the passed attributes to the specified temporal entity", async function () {

        //############# BEGIN Create entity ##############
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

        let err: any = undefined

        let createResponse = await axios.post(testConfig.base_url + "temporal/entities/", temporalEntity, config).catch((e) => {
            err = e
        }) as AxiosResponse

        expect(createResponse).to.not.be.undefined

        expect(createResponse.status).equals(201)
        //############# END Create entity ##############







        const temporalEntityFragment = {


            "id": entityId,
            "type": "TemporalTestEntity",

            "testProperty": [{
                "type": "Property",
                "value": 3
            }]
        }



        // Update entity (append attributes):

        err = undefined

        let appendResponse = await axios.post(testConfig.base_url + "temporal/entities/" + entityId + "/attrs/", temporalEntityFragment, config).catch((e) => {
            err = e
        }) as AxiosResponse

        if (err != undefined) {
            console.log(err)
        }
        expect(appendResponse).to.not.be.undefined

        expect(appendResponse.status).equals(204)


        // TODO: Check presence of location header in response

        err = undefined

        let getResponse = await axios.get(testConfig.base_url + "temporal/entities/" + entityId, config).catch((e) => {
            err = e
        }) as AxiosResponse


     

        expect(getResponse).to.not.be.undefined

        expect(getResponse.data.id).equals(entityId)

      
        const compareEntity = {


            "id": entityId,
            "type": "TemporalTestEntity",

            "testProperty": [{
                "type": "Property",
                "value": 1
            },
            {
                "type": "Property",
                "value": 2
            },
            {
                "type": "Property",
                "value": 3
            }]
        }

        expect(JSON.stringify(getResponse.data)).equals(JSON.stringify(compareEntity))


    })





});

