import { expect, assert } from "chai";
import axios, { AxiosRequestConfig, AxiosResponse } from 'axios'
import * as prep from "./testUtil"
import { testConfig } from './testConfig'





let config = {
    headers: { "content-type": "application/ld+json" },
    auth: testConfig.auth
}


const entity =
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



describe('6.4.3.1 POST /entities/', function () {

    before(async () => {
        await prep.deleteAllEntities()


    })


    after(async () => {
        await prep.deleteAllEntities()

    })



    it("Should create a new Entity", async function () {


        // Create entity:
        let response = await axios.post(testConfig.base_url + "entities/", entity, config)

        expect(response.status).equals(201)


        response = await axios.get(testConfig.base_url + "entities/urn:ngsi-ld:Test1", config)

        expect(response.status).equals(200)

        expect(response.data.name[0].value).equals("Test")

    })



    it("Should return HTTP status code 409 if an entity with the same ID already exists", async function () {


        // Try to create the same entity (with same id) again:
        let response = await axios.post(testConfig.base_url + "entities/", entity, config).catch((err) => {
            expect(err.response.status).equals(409)
        })

     
        expect(response).to.be.undefined
    })


});