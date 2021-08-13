import { expect, assert } from "chai";
import axios, { AxiosRequestConfig, AxiosResponse } from 'axios'
import * as prep from "./testUtil"
import { testConfig } from './testConfig'


let config = {
    headers: { "content-type": "application/ld+json" },
    auth: testConfig.auth
}


const entityId1 = "urn:ngsi-ld:TestEntity:test"


const entityId2 = "urn:ngsi-ld:TestEntity:test2"



describe('Auto-history mode', function () {

    before(async () => {
        await prep.deleteAllEntities()
    })


    after(async () => {
        await prep.deleteAllEntities()
    })





    it("should create an entity through the normal API", async function () {


        const entity1 = {

            "id": entityId1,
            "type": "TestEntity",

            "testProperty": [{
                "type": "Property",
                "value": "original"
            }],

            "testProperty2": [{
                "type": "Property",
                "value": 5
            }]
        }



        const entity2 = {

            "id": entityId2,
            "type": "TestEntity",

            "testProperty": [{
                "type": "Property",
                "value": "we_dont_want_to_know"
            }],

            "testProperty2": [{
                "type": "Property",
                "value": 3
            }]
        }

        // Create entity through temporal API:
        let err1: any = undefined

        let createResponse = await axios.post(testConfig.base_url + "entities/", entity1, config).catch((e) => {
            err1 = e
        }) as AxiosResponse

        expect(createResponse).to.not.be.undefined

        expect(createResponse.status).equals(201)

        // Create a second entity for "distraction":
        
        let createResponse2 = await axios.post(testConfig.base_url + "entities/", entity2, config).catch((e) => {
            err1 = e
        }) as AxiosResponse

        expect(createResponse2).to.not.be.undefined

        expect(createResponse2.status).equals(201)
    })




    it("should update an attribute through the normal API", async function () {

        const patchFragment = {
            "id": entityId1,
            "type": "TestEntity",


            "testProperty": [

                {
                    "type": "Property",
                    "value": "patched"

                }
            ]
        }
        let err1: any = undefined

        let patchResponse = await axios.patch(testConfig.base_url + "entities/" + entityId1 + "/attrs/testProperty", patchFragment, config).catch((e) => {
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



        let getResponse = await axios.get(testConfig.base_url + "temporal/entities/" + entityId1, config).catch((e) => {
            err2 = e
        }) as AxiosResponse


        if (err2 != undefined) {
            console.log(err2)
        }

        expect(getResponse).to.not.be.undefined

        const entity2 = getResponse.data

        expect(entity2.id).equals(entityId1)

        expect(entity2.testProperty.length).equals(2)

    })





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

        expect(entity2.id).equals(entityId1)

        expect(entity2.testProperty.length).equals(1)

        expect(entity2.testProperty[0].value).equals("patched")

    })




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




    it("should find the entity with a NGSI-LD query through the *temporal* API if we query for a historical attribute value", async function () {

        let err2 = undefined

        // This should fail because we query for the original attribute value ("original") which was changed
        // to "patched" in the previous test
        let getResponse = await axios.get(testConfig.base_url + 'temporal/entities/?q=testProperty=="original"', config).catch((e) => {
            err2 = e
        }) as AxiosResponse


        if (err2 != undefined) {
            console.log(err2)
        }

        expect(getResponse).to.not.be.undefined

        expect(getResponse.data.length).equals(1)

    })



});

