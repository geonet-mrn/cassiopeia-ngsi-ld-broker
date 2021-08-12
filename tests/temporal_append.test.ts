import { expect, assert } from "chai";
import axios, { AxiosRequestConfig, AxiosResponse } from 'axios'
import * as prep from "./testUtil"
import { testConfig } from './testConfig'
import * as uuid from 'uuid'


let config = {
    headers: { "content-type": "application/ld+json" },
    auth: testConfig.auth
}


const entityId = "urn:ngsi-ld:TestEntity:test"




describe('Patch temporal entity with normal API', function () {

    before(async () => {
        await prep.deleteAllEntities()
    })


    after(async () => {
        await prep.deleteAllEntities()
    })


    it("should create an entity through the temporal API", async function () {

        const entity = {
            "id": entityId,
            "type": "TestEntity",

            "testProperty": [{
                "type": "Property",
                "value": "initial1"
            },
            {
                "type": "Property",
                "value": "initial2"
            }]
        }

        // Create entity through temporal API:
        let err1: any = undefined

        let createResponse = await axios.post(testConfig.base_url + "temporal/entities/", entity, config).catch((e) => {
            err1 = e
        }) as AxiosResponse

        expect(createResponse).to.not.be.undefined

        expect(createResponse.status).equals(201)
    })



    
    it("should patch the entity through the normal API", async function () {

        const updateFragment1 = {
            "id": entityId,
            "type": "TestEntity",

            "testProperty": [{
                "type": "Property",
                "value": "firstUpdate"
            }]
        }

        // Patch entity through normal API:
        let err1: any = undefined

        let patchResponse = await axios.patch(testConfig.base_url + "entities/" + entityId + "/attrs/testProperty", updateFragment1, config).catch((e) => {
            err1 = e
        }) as AxiosResponse

        expect(patchResponse).to.not.be.undefined

        expect(patchResponse.status).equals(204)
    })
    



    it("should return the patched entity through the normal API", async function () {

        // Patch entity through normal API:
        let err3: any = undefined

        let getResponse = await axios.get(testConfig.base_url + "entities/" + entityId, config).catch((e) => {
            err3 = e
        }) as AxiosResponse

        expect(getResponse).to.not.be.undefined

        const entity = getResponse.data


        //console.log("normal:")
        //console.log(entity)
        expect(entity.testProperty.length).equals(1)

        expect(entity.testProperty[0].value).equals("firstUpdate")
    })
});

