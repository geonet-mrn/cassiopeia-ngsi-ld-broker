import { expect, assert } from "chai";
import axios, { AxiosResponse } from 'axios'
import * as uuid from 'uuid'
import * as prep from "./testPreparation"
import { testConfig } from './testConfig'
import * as fs from 'fs'


describe('POST entityOperations/upsert', function () {

    before(async () => {
        await prep.deleteAllEntities()

    })


    after(async () => {
        await prep.deleteAllEntities()
    })


    it("should create or update the passed entities", async function () {

        const config_POST = {
            headers: {
                // ATTENTION: If content-type is "application/ld+json", 
                // no context "link" header must be sent! (Spec 6.3.5)
                //"link": '<https://uri.geonet-mrn.de/xdatatogo/xdatatogo-context.jsonld>; rel="http://www.w3.org/ns/json-ld%23context";type="application/ld+json"',
                "content-type": "application/ld+json"
            },
            auth: testConfig.auth
        }

        const config_GET = {
            headers: {

                "link": '<https://uri.geonet-mrn.de/xdatatogo/xdatatogo-context.jsonld>; rel="http://www.w3.org/ns/json-ld%23context";type="application/ld+json"',
                "content-type": "application/ld+json"
            },
            auth: testConfig.auth
        }


        const url_upsert = testConfig.base_url + "entityOperations/upsert?options=update"

        const entitiesToUpload = JSON.parse(fs.readFileSync("tests/trafficObstacles.ngsi-ld.json").toString())


        //################################# BEGIN STEP 1 ##################################
        // Upsert entities for the FIRST time, i.e. ALL ENTITIES ARE NEWLY CREATED:
        const firstUpsertResponse = await axios.post(url_upsert, entitiesToUpload, config_POST).catch((e) => {
            console.log(e)
        }) as AxiosResponse


        // Since all entities are newly created, the number of entity IDs in the array that should be the response
        // should be equal to the number of created entities:
        expect(firstUpsertResponse.data.length).equal(entitiesToUpload.length)

        // The response status code should be 201:
        expect(firstUpsertResponse.status).equals(201)
        //################################# END STEP 1 ##################################



        //################################# BEGIN STEP 2 ##################################
        // Now, upsert entities for the SECOND time, i.e. ALL ENTITIES ALREADY EXIST AND ARE UPDATED:
        const secondUpsertResponse = await axios.post(url_upsert, entitiesToUpload, config_POST).catch((e) => {            
            console.log(e)
        }) as AxiosResponse

        // Since all entities were successfully updated, the reponse body should now be empty:
        expect(secondUpsertResponse.data).equal("")

        // The responst status code should now be 204:
        expect(secondUpsertResponse.status).equals(204)
        //################################# END STEP 2 ##################################

        

        //################################# BEGIN STEP 3 ##################################
        // Finally, perform another request to check whether the newly created entities really exist:
        const fetchResponse = await axios.get(testConfig.base_url + "entities/?type=TrafficRestriction", config_GET).catch((e) => {
            console.log(e)
        }) as AxiosResponse

        assert(fetchResponse)

        expect(fetchResponse.data).instanceOf(Array)
        expect(fetchResponse.data.length).equals(5)
        //################################# END STEP 3 ##################################

        // TODO: 2 Test error case!

        return new Promise<void>((resolve, reject) => {
            resolve()
        })
    })
});

