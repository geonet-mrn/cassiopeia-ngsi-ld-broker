import { expect, assert } from "chai";
import axios, { AxiosResponse } from 'axios'
import * as prep from "./testUtil"
import { testConfig } from './testConfig'
import { axiosGet } from "./testUtil";



let config = {
    headers: { "content-type": "application/ld+json" },
    auth: testConfig.auth
}

const entities = [
    {
        "id": "urn:ngsi-ld:Entity:entity1",
        "type": "TestEntity",
        "verwaltungsgemeinschaft": [
            {
                "type": "Property",
                "value": "Deidesheim"
            }
        ],
        "name": [
            {
                "type": "Property",
                "value": "Deidesheim"
            }
        ],
       

        "nestedProp1": {
            "type": "Property",
            "value": "level 1",

            "nestedProp2": {
                "type": "Property",
                "value": "level 2 in entity1",
            
                "nestedProp3": {
                    "type": "Property",
                    "value": {
                        "firstname": "John",
                        "lastname": "Doe",
                        "age": 23,
                        "address": {
                            "street" : "somewhere road"
                        }
                    }
                }
            }
        }
    },


    {
        "id": "urn:ngsi-ld:Entity:entity2",
        "type": "TestEntity",
        "name": [{ "type": "Property", "value": "Mannheim" }],
        
        "nestedProp1": {
            "type": "Property",
            "value": "level 1",

            "nestedProp2": {
                "type": "Property",
                "value": "level 2",
            
                "nestedProp3": {
                    "type": "Property",
                    "value": {
                        "firstname": "Erika",
                        "lastname": "Mustermann",
                        "age": 123
                    }
                }
            }
        }
    },
    {
        "id": "urn:ngsi-ld:Entity:entity3",
        "type": "TestEntity",
        "name": [{ "type": "Property", "value": "Somewhere" }],
      
    }
]



describe('6.4.3.2 GET /entities/', function () {

    before(async () => {
        await prep.deleteAllEntities()

        const createUrl = testConfig.base_url + "entityOperations/upsert"

        let createEntitiesResponse = await axios.post(createUrl, entities, config)

        expect(createEntitiesResponse.status).equals(201)
    })


    after(async () => {
        await prep.deleteAllEntities()
    })






    it("should return the number of matching entities in a response header if the query parameter 'count' is set to 'true', regardless of how many entites are actually returned due to possible 'limit' parameter (Spec 6.3.13)", async function () {

        let queryResponse = await axiosGet(testConfig.base_url + 'entities/?count=true', config)
        
        expect(queryResponse.headers["ngsild-results-count"]).to.not.be.undefined
    })


});