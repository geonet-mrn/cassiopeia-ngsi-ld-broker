import { expect, assert } from "chai";
import axios, { AxiosResponse } from 'axios'
import * as uuid from 'uuid'
import * as prep from "./testPreparation"
import {testConfig} from './testConfig'
import * as fs from 'fs'
import { doesNotMatch } from "node:assert";



describe('POST entityOperations/upsert', function () {

    before(async () => {
        await prep.deleteAllEntities()  
       
    }) 


    after(async () => {
        await prep.deleteAllEntities()
      
    })




    it("should create or update the passed entities", async function () {

        let config_POST = {
            headers: {
                // ATTENTION: If content-type is "application/ld+json", 
                // no context "link" header must be sent! (Spec 6.3.5)
                //"link": '<https://uri.geonet-mrn.de/xdatatogo/xdatatogo-context.jsonld>; rel="http://www.w3.org/ns/json-ld%23context";type="application/ld+json"',
                "content-type": "application/ld+json"
            },
            auth: testConfig.auth
        }

        let config_GET = {
            headers: {
                
                "link": '<https://uri.geonet-mrn.de/xdatatogo/xdatatogo-context.jsonld>; rel="http://www.w3.org/ns/json-ld%23context";type="application/ld+json"',
                "content-type": "application/ld+json"
            },
            auth: testConfig.auth
        }



        const entitiesToUpload = JSON.parse(fs.readFileSync("tests/trafficObstacles.ngsi-ld.json").toString())

        // Upsert entities:
        let response = await axios.post(testConfig.base_url + "entityOperations/upsert", entitiesToUpload, config_POST).catch((e) => {
            console.log(e)
        }) as AxiosResponse


        // Fetch created entities:
        response = await axios.get(testConfig.base_url + "entities/?type=TrafficRestriction", config_GET).catch((e) => {
            console.log(e)
        }) as AxiosResponse

        assert(response)

        expect(response.data).instanceOf(Array)
        
        expect(response.data.length).equals(5)

        return new Promise<void>((resolve, reject) => {
            resolve()
        })
    })
});

