import { expect, assert } from "chai";
import axios, { AxiosResponse } from 'axios'
import * as prep from "./testUtil"
import { testConfig } from './testConfig'



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
        ]
    },
    {
        "id": "urn:ngsi-ld:Municipality:07332017",
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
                "value": "Forst an der Weinstraße"
            }
        ]
    },
    {
        "id": "urn:ngsi-ld:Municipality:07332035",
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
                "value": "Meckenheim"
            }
        ]
    },

    {
        "id": "urn:ngsi-ld:Municipality:08226101",
        "type": "Municipality",
        "verwaltungsgemeinschaft": [
            {
                "type": "Property",
                "value": "VVG der Stadt Sinsheim"
            }
        ],
        "name": [
            {
                "type": "Property",
                "value": "Zuzenhausen"
            }
        ]
    },

    {
        "id": "urn:ngsi-ld:Municipality:08222000",
        "type": "Municipality",
        "name": [{ "type": "Property", "value": "Mannheim" }]
    }
]



describe('6.4.3.2 GET /entities/', function () {

    before(async () => {
        await prep.deleteAllEntities()


        //###################### BEGIN Create entities for test ######################
        const createUrl = testConfig.base_url + "entityOperations/upsert"


      


        let createEntitiesResponse = await axios.post(createUrl, entities, config)

        expect(createEntitiesResponse.status).equals(201)
        //###################### END Create entities for test ######################
    })


    after(async () => {
        await prep.deleteAllEntities()

    })



    it("Should return all expected entities", async function () {

        
        const queryResponse = await axios.get(testConfig.base_url + 'entities/?q=name=="Meckenheim"')
           
        console.log(queryResponse.data[0])
        expect(queryResponse.data.length).equals(1)
        expect(queryResponse.data[0].name[0].value == "Meckenheim")


        const queryResponse2 = await axios.get(testConfig.base_url + 'entities/?q=verwaltungsgemeinschaft')
        expect(queryResponse2.data.length).equals(4)


        const queryResponse3 = await axios.get(testConfig.base_url + 'entities/?q=verwaltungsgemeinschaft=="Deidesheim"')
        expect(queryResponse3.data.length).equals(3)


        const queryResponse4 = await axios.get(testConfig.base_url + 'entities/?q=name=="Mannheim"')
        expect(queryResponse4.data.length).equals(1)

        const queryResponse5 = await axios.get(testConfig.base_url + 'entities/?q=name=="Mannheim";verwaltungsgemeinschaft=="Deidesheim"')
        expect(queryResponse5.data.length).equals(0)
    })


    it("should return the requested entities as a GeoJSON FeatureCollection if the accept header 'application/geo+json' is set (spec 6.3.15)", async function () {



        let queryResponse = undefined

        try {
            queryResponse = await axios.get(testConfig.base_url + 'entities/?geometryProperty=name', config)          
        }
        catch (e) {
            console.log(e)
        }

        if (queryResponse == undefined) {
            return
        }

        
        expect(queryResponse.data.type).equals("FeatureCollection")

    })



    it("NOT OFFICIALLY PART OF NGSI-LD: Should return the requested entities as GeoJSON if the GET parameter 'geometryProperty' is set", async function () {


        let queryResponse = undefined

        try {
            queryResponse = await axios.get(testConfig.base_url + 'entities/?geometryProperty=location', config)            
        }
        catch (e) {
            console.log(e)
        }

        if (queryResponse == undefined) {
            return
        }

        expect(queryResponse.data.type).equals("FeatureCollection")

    })
});