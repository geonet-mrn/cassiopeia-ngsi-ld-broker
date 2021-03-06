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
        "id": "urn:ngsi-ld:Municipality:07332009",
        "type": "Municipality",
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
        "location": {
            "type": "GeoProperty",
            "value": {
                 "type": "Point",
                 "coordinates": [8.18, 49.4]
            }
        },
    },
  

    {
        "id": "urn:ngsi-ld:Municipality:08222000",
        "type": "Municipality",
        "name": [{ "type": "Property", "value": "Mannheim" }],
        "location": {
            "type": "GeoProperty",
            "value": {
                 "type": "Point",
                 "coordinates": [8.5, 49.5]
            }
        },
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


    it("should return the one entity (Mannheim) that matches the 'maxDistance' geo-query", async function () {

        let queryResponse = await axiosGet(testConfig.base_url + 'entities/?georel=near;maxDistance==1000&geometry=Point&coordinates=[8.5,49.5]', config)

        expect(queryResponse.status).equals(200)
                
        expect(queryResponse.data.length).equals(1)

        const entity = queryResponse.data[0]

        expect(entity.name[0].value).equals("Mannheim")

    })


    it("should return the one entity (Mannheim) that matches the 'within' geo-query", async function () {

        let queryResponse = await axiosGet(testConfig.base_url + 'entities/?georel=within&geometry=Polygon&coordinates=[[[8.4,49.4],[8.6,49.4],[8.6,49.6],[8.4,49.6],[8.4,49.4]]]', config)

        expect(queryResponse.status).equals(200)
                
        expect(queryResponse.data.length).equals(1)
    
        const entity = queryResponse.data[0]

        expect(entity.name[0].value).equals("Mannheim")

    })



    it("should return no entity because none matches the 'within' geo-query", async function () {

        let queryResponse = await axiosGet(testConfig.base_url + 'entities/?georel=within&geometry=Polygon&coordinates=[[[28.4,49.4],[28.6,49.4],[28.6,49.6],[28.4,49.6],[8.4,49.4]]]', config)

        expect(queryResponse.status).equals(200)
                
        expect(queryResponse.data.length).equals(0)
    })

    it("should return two entities because both stored entities match the 'within' geo-query", async function () {

        let queryResponse = await axiosGet(testConfig.base_url + 'entities/?georel=within&geometry=Polygon&coordinates=[[[8.0,48.0],[10.0,48.0],[10.0,50.0],[8.0,50.0],[8.0,48.0]]]', config)

        expect(queryResponse.status).equals(200)
                
        expect(queryResponse.data.length).equals(2)
    })



    it("should return two entities because both stored entities match the 'disjoint' geo-query", async function () {

        let queryResponse = await axiosGet(testConfig.base_url + 'entities/?georel=disjoint&geometry=Polygon&coordinates=[[[28.4,49.4],[28.6,49.4],[28.6,49.6],[28.4,49.6],[28.4,49.4]]]', config)

        expect(queryResponse.status).equals(200)
                
        expect(queryResponse.data.length).equals(2)
    })
});