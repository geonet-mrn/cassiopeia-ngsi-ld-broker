import { expect, assert } from "chai";
import axios, { AxiosRequestConfig, AxiosResponse } from 'axios'
import * as prep from "./testUtil"
import { testConfig } from './testConfig'



let config = {
    headers: { "content-type": "application/ld+json" },
    auth: testConfig.auth
}


const temporalEntityId = "urn:ngsi-ld:TestEntity:test_normal"

const temporalEntity = {


    "id": temporalEntityId,
    "type": "TestEntity",

    "testProperty": [{
        "type": "Property",
        "value": 1
    },
    {
        "type": "Property",
        "value": 2
    }]
}



describe('Interaction between "normal" and "temporal" API endpoints', function () {

    before(async () => {
        await prep.deleteAllEntities()



    })


    after(async () => {
        await prep.deleteAllEntities()
    })





    it("should create an entity through the temporal API", async function () {

        // Create entity through temporal API:
        let err: any = undefined

        let createResponse = await axios.post(testConfig.base_url + "temporal/entities/", temporalEntity, config).catch((e) => {
            err = e
        }) as AxiosResponse

        expect(createResponse).to.not.be.undefined

        expect(createResponse.status).equals(201)
    })




    it("should retrieve the entity that was created through the temporal API with the *temporal* API", async function () {


        // TODO: Check presence of location header in response

        let err = undefined



        let getResponse = await axios.get(testConfig.base_url + "temporal/entities/" + temporalEntityId, config).catch((e) => {
            err = e
        }) as AxiosResponse


        if (err != undefined) {
            console.log(err)
        }

        expect(getResponse).to.not.be.undefined

        const entity = getResponse.data

        expect(entity.id).equals(temporalEntityId)

        expect(entity.testProperty.length).equals(2)

    })




    it("should retrieve the entity that was created through the temporal API with the *normal* API too", async function () {


        // TODO: Check presence of location header in response

        let err = undefined



        let getResponse = await axios.get(testConfig.base_url + "entities/" + temporalEntityId, config).catch((e) => {
            err = e
        }) as AxiosResponse


        if (err != undefined) {
            console.log(err)
        }

        expect(getResponse).to.not.be.undefined

        const entity = getResponse.data


        expect(entity.id).equals(temporalEntityId)

        expect(entity.testProperty.length).equals(1)


    })
});

