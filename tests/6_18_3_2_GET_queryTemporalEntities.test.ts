import { expect, assert } from "chai";
import axios, { AxiosRequestConfig, AxiosResponse } from 'axios'
import * as prep from "./testUtil"
import { testConfig } from './testConfig'



let config = {
    headers: { "content-type": "application/ld+json" },
    auth: testConfig.auth
}

   
const entityId = "urn:ngsi-ld:TemporalTestEntity:test"



describe('6.18.3.2 GET temporal/entities/', function () {

    before(async () => {
        await prep.deleteAllEntities()



    })


    after(async () => {
        await prep.deleteAllEntities()
    })





    it("should return the temporal entities that match the passed temporal entity", async function () {

    
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

        let getResponse = await axios.get(testConfig.base_url + "temporal/entities/", config).catch((e) => {
            err = e
        }) as AxiosResponse


        if (err != undefined) {
            console.log(err)
        }

        expect(getResponse).to.not.be.undefined

        expect(getResponse.data.length).equals(1)


    })





});

