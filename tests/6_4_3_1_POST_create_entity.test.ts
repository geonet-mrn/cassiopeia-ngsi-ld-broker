import { expect, assert } from "chai";
import axios, { AxiosRequestConfig, AxiosResponse } from 'axios'
import * as prep from "./testUtil"
import { testConfig } from './testConfig'
import { axiosPost } from "./testUtil";



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

    before(async () => {
        await prep.deleteAllEntities()
    })


    after(async () => {
        await prep.deleteAllEntities()
    })



    it("should create a new Entity", async function () {
        
        let response = await axios.post(testConfig.base_url + "entities/", validEntity, config)

        expect(response.status).equals(201)

        response = await axios.get(testConfig.base_url + "entities/urn:ngsi-ld:Test1", config)

        expect(response.status).equals(200)
        expect(response.data.name[0].value).equals("Test")

    })



    it("should return HTTP status code 409 if an entity with the same ID already exists", async function () {
        
        let response = await axiosPost(testConfig.base_url + "entities/", validEntity, config)

        expect(response.status).equals(409)
    })



    it("should not create a new entity and return HTTP 400 if one or more of its attributes has multiple instances with the same datasetId (defined or undefined)", async function () {

        let response = await axiosPost(testConfig.base_url + "entities/", entityWithDuplicateDatasetIds, config)

        expect(response.status).equals(400)        
    })

});