import { expect, assert } from "chai";
import axios, { AxiosRequestConfig, AxiosResponse } from 'axios'
import * as prep from "./testUtil"
import { testConfig } from './testConfig'
import * as uuid from 'uuid'


let config = {
    headers: { "content-type": "application/ld+json" },
    auth: testConfig.auth
}


const temporalEntityId = "urn:ngsi-ld:TestEntity:" + uuid.v4()

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
        let err1: any = undefined

        let createResponse = await axios.post(testConfig.base_url + "temporal/entities/", temporalEntity, config).catch((e) => {
            err1 = e
        }) as AxiosResponse

        expect(createResponse).to.not.be.undefined

        expect(createResponse.status).equals(201)
    })




    it("should retrieve the entity that was created through the temporal API with the *temporal* API", async function () {


        // TODO: Check presence of location header in response

        let err2 = undefined



        let getResponse = await axios.get(testConfig.base_url + "temporal/entities/" + temporalEntityId, config).catch((e) => {
            err2 = e
        }) as AxiosResponse


        if (err2 != undefined) {
            console.log(err2)
        }

        expect(getResponse).to.not.be.undefined

        const entity2 = getResponse.data

        expect(entity2.id).equals(temporalEntityId)

        expect(entity2.testProperty.length).equals(2)

     
    })




    it("should retrieve the entity that was created through the temporal API with the *normal* API too", async function () {

        // TODO: Check presence of location header in response

        let err3 = undefined



        let getResponse2 = await axios.get(testConfig.base_url + "entities/" + temporalEntityId, config).catch((e) => {
            err3 = e
        }) as AxiosResponse


        if (err3 != undefined) {
            console.log(err3)
        }

        expect(getResponse2).to.not.be.undefined

        const entity3 = getResponse2.data

        console.log(entity3)

        expect(entity3.id).equals(temporalEntityId)

        expect(entity3.testProperty.length).equals(1)


    })
});

