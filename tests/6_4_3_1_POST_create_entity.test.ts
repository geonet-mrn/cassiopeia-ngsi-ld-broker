import { expect, assert } from "chai";
import axios, { AxiosRequestConfig, AxiosResponse } from 'axios'
import * as prep from "./testUtil"
import { testConfig } from './testConfig'





let config = {
    headers: { "content-type": "application/ld+json" },
    auth: testConfig.auth
}


const validEntity =
{
    "id": "urn:ngsi-ld:Test1",
    "type": "TestEntity",

    "name": [
        {
            "type": "Property",
            "value": "Test"
        }
    ]
}




const entityWithDuplicateDatasetIds =
{
    "id": "urn:ngsi-ld:Test1",
    "type": "TestEntity",

    "name": [
        {
            "type": "Property",
            "value": "Test1"
        },
        {
            "type": "Property",
            "value": "Test2"
        }
    ]
}


describe('6.4.3.1 POST /entities/', function () {

    beforeEach(async () => {
        await prep.deleteAllEntities()


    })


    afterEach(async () => {
        await prep.deleteAllEntities()

    })



    it("should create a new Entity", async function () {


        // Create entity:
        let response = await axios.post(testConfig.base_url + "entities/", validEntity, config)

        expect(response.status).equals(201)


        response = await axios.get(testConfig.base_url + "entities/urn:ngsi-ld:Test1", config)

        expect(response.status).equals(200)

        expect(response.data.name[0].value).equals("Test")

    })



    it("should return HTTP status code 409 if an entity with the same ID already exists", async function () {

        // Create entity:
        let response = await axios.post(testConfig.base_url + "entities/", validEntity, config)

        expect(response.status).equals(201)

        // Try to create the same entity (with same id) again:
        let response2 = await axios.post(testConfig.base_url + "entities/", validEntity, config).catch((err) => {
            expect(err.response.status).equals(409)
        })


        expect(response2).to.be.undefined
    })



    it("should not create a new entity if one or more of its attributes has multiple instances with the same datasetId (defined or undefined)", async function () {

        let err : any = undefined
        
        // Try to create invalid entity:
        let response = await axios.post(testConfig.base_url + "entities/", entityWithDuplicateDatasetIds, config).catch((e) => err = e)

        expect(err).to.not.be.undefined

        if (err != undefined) {
            expect(err.response.status).equals(400)
        }

        
    })


});