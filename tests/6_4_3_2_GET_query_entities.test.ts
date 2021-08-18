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

        "name": {
            "type": "Property",
            "value": "Entity 1, with a comma"
        },


        "owner": {
            "type": "Relationship",
            "object": "urn:ngsi-ld:Entity:FrauMueller"
        },




        "testProp": {
            "type": "Property",
            "value": "a==b"
        },


        "testNumber": {
            "type": "Property",
            "value": 10
        },

        "location": {
            "type": "GeoProperty",
            "value": "{\"type\": \"Point\",\"coordinates\": [8.18, 49.4]}"
        },

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
                            "street": "somewhere road"
                        }
                    }
                }
            }
        }
    },


    {
        "id": "urn:ngsi-ld:Entity:entity2",
        "type": "TestEntity",

        "name": {
            "type": "Property",
            "value": "Entity2"
        },


        "owner": {
            "type": "Relationship",
            "object": "urn:ngsi-ld:Entity:HerrMeier"
        },


        "testNumber": {
            "type": "Property",
            "value": 20
        },

        "location": {
            "type": "GeoProperty",
            "value": "{\"type\": \"Point\",\"coordinates\": [8.5, 49.5]}"
            

        },
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
        "name": [{ "type": "Property", "value": "Entity3" }],

        "testNumber": {
            "type": "Property",
            "value": 30
        },


        "location": {
            "type": "GeoProperty",
            "value": "{\"type\": \"Point\",\"coordinates\": [48.5,60.5]}"
            
        }
    },
    {
        "id": "urn:ngsi-ld:Entity:entity4",
        "type": "TestEntity",

        "something": {
            "type": "Property",
            "value": "nobody knows"
        }
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




    /*
    it("should return the expected entities for the passed NGSI-LD queries targeting nested properties", async function () {

        let queryResponse = undefined
        
        queryResponse = await axiosGet(testConfig.base_url + 'entities/?q=nestedProp1', config)
        expect(queryResponse.status).equals(200)
        expect(queryResponse.data.length).equals(2)

        
        queryResponse = await axiosGet(testConfig.base_url + 'entities/?q=nestedProp1.nestedProp2', config)
        expect(queryResponse.status).equals(200)
        expect(queryResponse.data.length).equals(2)

        queryResponse = await axiosGet(testConfig.base_url + 'entities/?q=nestedProp1.nestedProp2=="level 2 in entity1"', config)
        expect(queryResponse.status).equals(200)
        expect(queryResponse.data.length).equals(1)

        queryResponse = await axiosGet(testConfig.base_url + 'entities/?q=nestedProp1.nestedProp2.nestedProp3', config)
        expect(queryResponse.status).equals(200)
        expect(queryResponse.data.length).equals(2)
        
    })
    */

    it("should return the number of matching entities in a response header if the query parameter 'count' is set to 'true', regardless of how many entites are actually returned due to possible 'limit' parameter (Spec 6.3.13)", async function () {

        let queryResponse = await axiosGet(testConfig.base_url + 'entities/?count=true', config)

        expect(queryResponse.headers["ngsild-results-count"]).to.not.be.undefined
    })


    it("should return the expected entities for a query against a value list (see spec 4.9)", async function () {

        let queryResponse = await axiosGet(testConfig.base_url + 'entities/?q=name=="Entity2","Entity 1, with a comma"', config)
        expect(queryResponse.data.length).equals(2)

        queryResponse = await axiosGet(testConfig.base_url + 'entities/?q=testNumber==10,20', config)
        expect(queryResponse.data.length).equals(2)
    })


    it("should return the expected entities for a query against a range (see spec 4.9)", async function () {

        let queryResponse = await axiosGet(testConfig.base_url + 'entities/?q=testNumber==5..15', config)
        expect(queryResponse.data.length).equals(1)

        queryResponse = await axiosGet(testConfig.base_url + 'entities/?q=testNumber==0..3', config)
        expect(queryResponse.data.length).equals(0)
    })





    it("should return the expected entities for a query against property value that contains the character sequence '=='", async function () {

        let queryResponse = await axiosGet(testConfig.base_url + 'entities/?q=testProp=="a==b"', config)
        expect(queryResponse.data.length).equals(1)

        queryResponse = await axiosGet(testConfig.base_url + 'entities/?q=testProp=="(("', config)
        expect(queryResponse.status).equals(200)
        expect(queryResponse.data.length).equals(0)

    })


    /*
    it("should return the expected entities for the passed NGSI-LD queries that contain a trailing path", async function () {

        let queryResponse = await axiosGet(testConfig.base_url + 'entities/?q=nestedProp1.nestedProp2.nestedProp3[firstname]=="John"', config)
        expect(queryResponse.status).equals(200)
        expect(queryResponse.data.length).equals(1)

        queryResponse = await axiosGet(testConfig.base_url + 'entities/?q=nestedProp1.nestedProp2.nestedProp3[address.street]=="somewhere road"', config)
        expect(queryResponse.status).equals(200)
        expect(queryResponse.data.length).equals(1)


        queryResponse = await axiosGet(testConfig.base_url + 'entities/?q=nestedProp1.nestedProp2.nestedProp3[age]>20', config)
        expect(queryResponse.status).equals(200)
        expect(queryResponse.data.length).equals(2)


        queryResponse = await axiosGet(testConfig.base_url + 'entities/?q=nestedProp1.nestedProp2.nestedProp3[age]==123', config)
        expect(queryResponse.status).equals(200)
        expect(queryResponse.data.length).equals(1)



        queryResponse = await axiosGet(testConfig.base_url + 'entities/?q=nestedProp1.nestedProp2.nestedProp3[age]<10', config)
        expect(queryResponse.status).equals(200)
        expect(queryResponse.data.length).equals(0)


        queryResponse = await axiosGet(testConfig.base_url + 'entities/?q=nestedProp1.nestedProp2.nestedProp3[address.street]=="elsewhere road"', config)
        expect(queryResponse.status).equals(200)
        expect(queryResponse.data.length).equals(0)

    })
    */



    it("should return the requested entities as a GeoJSON FeatureCollection if the accept header 'application/geo+json' is set (spec 6.3.15)", async function () {

        let queryResponse = await axiosGet(testConfig.base_url + 'entities/?geometryProperty=name', config)
        expect(queryResponse.status).equals(200)
        expect(queryResponse.data.type).equals("FeatureCollection")
    })




    it("should return only entities that contain at least one of the attributes in the attributes list, if an attributes list is provided", async function () {

        let queryResponse = await axiosGet(testConfig.base_url + 'entities/?attrs=thisdoesnotexist', config)
        expect(queryResponse.status).equals(200)
        expect(queryResponse.data.length).equals(0)

        queryResponse = await axiosGet(testConfig.base_url + 'entities/?attrs=name', config)
        expect(queryResponse.status).equals(200)
        expect(queryResponse.data.length).equals(3)

    })



    it("should return the requested entities as a GeoJSON FeatureCollection if the accept header 'application/geo+json' is set (spec 6.3.15)", async function () {

        let queryResponse = await axiosGet(testConfig.base_url + 'entities/?geometryProperty=name', config)
        expect(queryResponse.status).equals(200)
        expect(queryResponse.data.type).equals("FeatureCollection")
    })



    it("NOT OFFICIALLY PART OF NGSI-LD: Should return the requested entities as GeoJSON if the GET parameter 'geometryProperty' is set", async function () {

        let queryResponse = await axiosGet(testConfig.base_url + 'entities/?geometryProperty=location', config)
        expect(queryResponse.status).equals(200)
        expect(queryResponse.data.type).equals("FeatureCollection")
    })






    it("should return the expected entities for a query against a relationship", async function () {

        let queryResponse = undefined

        // URI without quotes (correct according to spec 4.9):
        queryResponse = await axiosGet(testConfig.base_url + 'entities/?q=owner==urn:ngsi-ld:Entity:FrauMueller', config)
        expect(queryResponse.data.length).equals(1)
        expect(queryResponse.data[0].id).equals("urn:ngsi-ld:Entity:entity1")


        // URI with quotes (correct according to spec 4.9):
        queryResponse = await axiosGet(testConfig.base_url + 'entities/?q=owner=="urn:ngsi-ld:Entity:HerrMeier"', config)
        expect(queryResponse.data.length).equals(1)


        // URI with quotes (correct according to spec 4.9):
        queryResponse = await axiosGet(testConfig.base_url + 'entities/?q=owner', config)
        expect(queryResponse.data.length).equals(2)

    })


});