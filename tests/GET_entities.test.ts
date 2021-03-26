import { expect, assert } from "chai";
import axios, { AxiosResponse } from 'axios'
import * as uuid from 'uuid'
import * as prep from "./testPreparation"
import {testConfig} from './testConfig'

const entityId = "urn:xdatatogo:TrafficRestriction:" + uuid.v4()


describe('GET entities/<entity_id>', function () {

    before(async () => {
        await prep.deleteAllEntities()
               
        await prep.createEntity(entityId)

        return new Promise<void>((resolve, reject) => {
            resolve()
        })
    }) 


    it('should return the Entity with the ID specified in the URL', async function () {

     
        let url = testConfig.base_url + "entities/" + entityId

        let config = {
            headers: {
                "link": '<https://uri.geonet-mrn.de/xdatatogo/xdatatogo-context.jsonld>; rel="http://www.w3.org/ns/json-ld%23context";type="application/ld+json"'
            }
        }

        // Retrieve created entity again:
        let response = await axios.get(url, config)



        expect(response.data.id).equal(entityId)

        return new Promise<void>((resolve, reject) => {
            resolve()
        })
    });
});

