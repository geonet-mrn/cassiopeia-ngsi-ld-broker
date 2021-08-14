import { expect, assert } from "chai";
import * as util from "./testUtil"
import { testConfig } from './testConfig'
import { axiosPost } from "./testUtil";



let config = {
    headers: { "content-type": "application/ld+json" },
    auth: testConfig.auth
}


const entityId1 = "urn:ngsi-ld:TemporalTestEntity:test1"

const entityId2 = "urn:ngsi-ld:TemporalTestEntity:test2"


describe('6.18.3.2 GET temporal/entities/', function () {

    before(async () => {
        await util.deleteAllEntities()
    })


    after(async () => {
        await util.deleteAllEntities()
    })



    it("should create a temporal entity", async function () {

        const entity1 = {

            "id": entityId1,
            "type": "TemporalTestEntity",

            "name": [{
                "type": "Property",
                "value": "Entity 1"                
            }],

            "testProperty": [{
                "type": "Property",
                "value": 1,
                "observedAt": "2015-01-01T00:00:00Z"
            },
            {
                "type": "Property",
                "value": 2,
                "observedAt": "2014-01-01T00:00:00Z"
            },
            {
                "type": "Property",
                "value": 3,
                "observedAt": "2013-01-01T00:00:00Z"
            }]
        }


        const entity2 = {

            "id": entityId2,
            "type": "TemporalTestEntity",

            "name": [{
                "type": "Property",
                "value": "Entity 2"                
            }],

            "testProperty": [{
                "type": "Property",
                "value": 1,
                "observedAt": "3015-01-01T00:00:00Z"
            },
            {
                "type": "Property",
                "value": 2,
                "observedAt": "3014-01-01T00:00:00Z"
            },
            {
                "type": "Property",
                "value": 3,
                "observedAt": "3013-01-01T00:00:00Z"
            }]
        }


        // Create entity 1:
        let createResponse = await axiosPost(testConfig.base_url + "temporal/entities/", entity1, config)
        expect(createResponse.status).equals(201)

        // Create entity 2:
        let createResponse2 = await axiosPost(testConfig.base_url + "temporal/entities/", entity2, config)
        expect(createResponse2.status).equals(201)

        let getResponse = await util.axiosGet(testConfig.base_url + "temporal/entities/" + entityId1, config)
        let getResponse2 = await util.axiosGet(testConfig.base_url + "temporal/entities/" + entityId2, config)

        const entity = getResponse.data

        expect(entity.testProperty.length).equals(3)
    })


    it("should return an error 'BadRequestData' (HTTP 400) if no complete temporal query was passed", async function () {

        // Request with no temporal parameters at all:
        let getResponse = await util.axiosGet(testConfig.base_url + "temporal/entities/", config)
        expect(getResponse.status).equals(400)

        // Request with only "timeRel":
        getResponse = await util.axiosGet(testConfig.base_url + "temporal/entities/?timerel=before", config)
        expect(getResponse.status).equals(400)

        // Request with only "timeAt":
        getResponse = await util.axiosGet(testConfig.base_url + "temporal/entities/?timeAt=2000-01-01T00:00:00Z", config)
        expect(getResponse.status).equals(400)

    })



    it("should return one entity if a temporal query for attribute before 2100-01-01T00:00:00Z is passed", async function () {

        let getResponse = await util.axiosGet(testConfig.base_url + "temporal/entities/?timerel=before&timeAt=2100-01-01T00:00:00Z", config)
        expect(getResponse.status).equals(200)
        expect(getResponse.data.length).equals(1)
        expect(getResponse.data[0].id).equals(entityId1)
    })



    it("should return one entity if a temporal query for attribute after 2100-01-01T00:00:00Z is passed", async function () {

        let getResponse = await util.axiosGet(testConfig.base_url + "temporal/entities/?timerel=after&timeAt=2100-01-01T00:00:00Z", config)
        expect(getResponse.status).equals(200)

        expect(getResponse.data.length).equals(1)

        const entity = getResponse.data[0]
        expect(entity.id).equals(entityId2)
    })




    it("should return one entity if a temporal query for attributes between 2020 and 2014 is passed", async function () {

        let getResponse = await util.axiosGet(testConfig.base_url + "temporal/entities/?attrs=testProperty&timerel=between&timeAt=2014-01-01T00:00:00Z&endTimeAt=2020-01-01T00:00:00Z", config)
        expect(getResponse.status).equals(200)

        expect(getResponse.data.length).equals(1)

        const entity = getResponse.data[0]
        expect(entity.id).equals(entityId1)

        console.log(entity)
    })




    it("should return no entity if none of the requested attributes has a temporally matching attribute instance, even if another, not requested attribute has temporally matching attribute instances", async function () {

        // Note how we only request the attribute "name" here, which has no temporally matching attribute instances:
        let getResponse = await util.axiosGet(testConfig.base_url + "temporal/entities/?attrs=name&timerel=between&timeAt=2014-01-01T00:00:00Z&endTimeAt=2020-01-01T00:00:00Z", config)
        expect(getResponse.status).equals(200)

        expect(getResponse.data.length).equals(0)

    })



    it("should return two entities if a temporal query for attribute after 1100-01-01T00:00:00Z is passed", async function () {

        let getResponse = await util.axiosGet(testConfig.base_url + "temporal/entities/?timerel=after&timeAt=1100-01-01T00:00:00Z", config)
        expect(getResponse.status).equals(200)
        expect(getResponse.data.length).equals(2)

        expect(getResponse.data[0].testProperty.length).equals(3)
        expect(getResponse.data[1].testProperty.length).equals(3)
    })




    it("should return only the lastN attribute instances", async function () {

        let getResponse = await util.axiosGet(testConfig.base_url + "temporal/entities/?timerel=after&timeAt=1100-01-01T00:00:00Z&lastN=1", config)
        expect(getResponse.status).equals(200)
        expect(getResponse.data.length).equals(2)

        expect(getResponse.data[0].testProperty.length).equals(1)
        expect(getResponse.data[1].testProperty.length).equals(1)
    })


    it("should return an empty array if a temporal query for attributes after 3100-01-01T00:00:00Z is passed", async function () {

        let getResponse = await util.axiosGet(testConfig.base_url + "temporal/entities/?timerel=after&timeAt=3100-01-01T00:00:00Z", config)
        expect(getResponse.status).equals(200)
        expect(getResponse.data.length).equals(0)
    })



    it("should return an empty array if a temporal query for attributes before 1100-01-01T00:00:00Z is passed", async function () {

        let getResponse = await util.axiosGet(testConfig.base_url + "temporal/entities/?timerel=before&timeAt=1100-01-01T00:00:00Z", config)
        expect(getResponse.status).equals(200)
        expect(getResponse.data.length).equals(0)
    })



});

