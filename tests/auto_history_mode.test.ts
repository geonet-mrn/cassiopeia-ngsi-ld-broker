import { expect, assert } from "chai";
import axios, { AxiosRequestConfig, AxiosResponse } from 'axios'
import * as prep from "./testUtil"
import { testConfig } from './testConfig'


let config = {
    headers: { "content-type": "application/ld+json" },
    auth: testConfig.auth
}


const entityId = "urn:ngsi-ld:TestEntity:test"

const entity = {


    "id": entityId,
    "type": "TestEntity",

    "testProperty": [{
        "type": "Property",
        "value": "original"
    }]
}



describe('Auto-history mode', function () {

    before(async () => {
        await prep.deleteAllEntities()
    })


    after(async () => {
        await prep.deleteAllEntities()
    })





    it("should create an entity through the normal API", async function () {

        // Create entity through temporal API:
        let err1: any = undefined

        let createResponse = await axios.post(testConfig.base_url + "entities/", entity, config).catch((e) => {
            err1 = e
        }) as AxiosResponse

        expect(createResponse).to.not.be.undefined

        expect(createResponse.status).equals(201)
    })




    it("should update an attribute through the normal API", async function () {

        const patchFragment = {
            "id": entityId,
            "type": "TestEntity",


            "testProperty": [

                {
                    "type": "Property",
                    "value": "patched"

                }
            ]
        }
        let err1: any = undefined

        let patchResponse = await axios.patch(testConfig.base_url + "entities/" + entityId + "/attrs/testProperty", patchFragment, config).catch((e) => {
            err1 = e
        }) as AxiosResponse

        if (err1 != undefined) {
            console.log(err1.response)
        }


        expect(patchResponse).to.not.be.undefined

        expect(patchResponse.status).equals(204)

    })




    it("should retrieve the entity through the *temporal* API", async function () {


        // TODO: Check presence of location header in response

        let err2 = undefined



        let getResponse = await axios.get(testConfig.base_url + "temporal/entities/" + entityId, config).catch((e) => {
            err2 = e
        }) as AxiosResponse


        if (err2 != undefined) {
            console.log(err2)
        }

        expect(getResponse).to.not.be.undefined

        const entity2 = getResponse.data

        expect(entity2.id).equals(entityId)

        expect(entity2.testProperty.length).equals(2)

    })




/*
    it("should find the entity with a NGSI-LD query through the *normal* API", async function () {

        let err2 = undefined

        let getResponse = await axios.get(testConfig.base_url + 'entities/?q=testProperty=="patched"', config).catch((e) => {
            err2 = e
        }) as AxiosResponse


        if (err2 != undefined) {
            console.log(err2)
        }

        expect(getResponse).to.not.be.undefined

        expect(getResponse.data.length).equals(1)

        const entity2 = getResponse.data[0]

        expect(entity2.id).equals(entityId)

        expect(entity2.testProperty.length).equals(1)

        expect(entity2.testProperty[0].value).equals("patched")

    })
*/



    it("should *NOT* find the entity with a NGSI-LD query through the *normal* API if we query for a historical attribute value", async function () {

        let err2 = undefined

        // This should fail because we query for the original attribute value ("original") which was changed
        // to "patched" in the previous test
        let getResponse = await axios.get(testConfig.base_url + 'entities/?q=testProperty=="original"', config).catch((e) => {
            err2 = e
        }) as AxiosResponse


        if (err2 != undefined) {
            console.log(err2)
        }

        expect(getResponse).to.not.be.undefined

        expect(getResponse.data.length).equals(0)
    })



});

