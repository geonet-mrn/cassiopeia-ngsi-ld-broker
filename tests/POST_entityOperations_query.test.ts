import { expect, assert } from "chai";
import axios, { AxiosResponse } from 'axios'
import * as uuid from 'uuid'
import * as prep from "./testPreparation"
import {testConfig} from './testConfig'

const entityId = "urn:xdatatogo:TrafficRestriction:" + uuid.v4()


describe('POST entityOperations/query', function () {

    before(async () => {
        await prep.deleteAllEntities()

        await prep.createEntity(entityId)
        
        return new Promise<void>((resolve, reject) => {
            resolve()
        })
    })


    after(async () => {
        await prep.deleteAllEntities()
        
        return new Promise<void>((resolve, reject) => {
            resolve()
        })
    })





    it("should return the entities that match the property query", async function () {

        const query = {
            "@context": "https://uri.geonet-mrn.de/xdatatogo/xdatatogo-context.jsonld",
            "q": "maxSpeed==123",
            "type": "Query"

        }

        let config = {
            headers: {
                // ATTENTION: If content-type is "application/ld+json", 
                // no context "link" header must be sent! (Spec 6.3.5)
                //"link": '<https://uri.geonet-mrn.de/xdatatogo/xdatatogo-context.jsonld>; rel="http://www.w3.org/ns/json-ld%23context";type="application/ld+json"',
                "content-type": "application/ld+json"
            }
        }


        let response = await axios.post(testConfig.base_url + "entityOperations/query", query, config).catch((e) => {
            console.log(e)
        }) as AxiosResponse

        expect(response.data[0].id).equals(entityId)
        
        return new Promise<void>((resolve, reject) => {
            resolve()
        })
    })



    it("should return an empty array because no existing entities match the query", async function () {
        const query = {
            "@context": "https://uri.geonet-mrn.de/xdatatogo/xdatatogo-context.jsonld",

            geoQ: {
                "georel": "near;maxDistance==1000",
                "geometry": "Point",
                // NOTE: Actual coordinates of existing entity are [50,50]
                "coordinates": [0, 0],
                "geoproperty": "location"
            },
            "type": "Query"
        }

        let config = {
            headers: {
                // ATTENTION: If content-type is "application/ld+json", 
                // no context "link" header must be sent! (Spec 6.3.5)
                //"link": '<https://uri.geonet-mrn.de/xdatatogo/xdatatogo-context.jsonld>; rel="http://www.w3.org/ns/json-ld%23context";type="application/ld+json"',
                "content-type": "application/ld+json"
            },
            auth: testConfig.auth
        }


        let response = await axios.post(testConfig.base_url + "entityOperations/query/", query, config).catch((e) => {
            console.log(e)
        }) as AxiosResponse


        assert(response)

        expect(response.data).instanceOf(Array)
        expect(response.data.length).equals(0)

        return new Promise<void>((resolve, reject) => {
            resolve()
        })
    })


    it("should return and array with one existing entity that matches the geo query", async function () {

        const query = {
            "@context": "https://uri.geonet-mrn.de/xdatatogo/xdatatogo-context.jsonld",
            geoQ: {
                "georel": "near;maxDistance==0",
                "geometry": "Point",
                // NOTE: Actual coordinates of existing entity are [50,50]
                "coordinates": [50, 50],
                "geoproperty": "location"
            },
            "type": "Query"
        }

        let config = {
            headers: {
                // ATTENTION: If content-type is "application/ld+json", 
                // no context "link" header must be sent! (Spec 6.3.5)
                //"link": '<https://uri.geonet-mrn.de/xdatatogo/xdatatogo-context.jsonld>; rel="http://www.w3.org/ns/json-ld%23context";type="application/ld+json"',
                "content-type": "application/ld+json"
            },
            auth: testConfig.auth
        }


        let response = await axios.post(testConfig.base_url + "entityOperations/query/", query, config).catch((e) => {
            console.log(e)
        }) as AxiosResponse

        expect(response.data).instanceOf(Array)

        expect(response.data[0].id).equals(entityId)


        return new Promise<void>((resolve, reject) => {
            resolve()
        })
    })
});

