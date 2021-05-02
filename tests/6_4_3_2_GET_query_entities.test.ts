import { expect, assert } from "chai";
import axios, { AxiosResponse } from 'axios'
import * as prep from "./testUtil"
import { testConfig } from './testConfig'


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



describe('6.4.3.2 GET Query Entities', function () {

    beforeEach(async () => {
        await prep.deleteAllEntities()

    })


    afterEach(async () => {
        await prep.deleteAllEntities()

    })



    it("Should return all expected entities", async function () {

        const config = {
            headers: {
                "content-type": "application/ld+json"
            },
            auth: testConfig.auth
        }

    

        //###################### BEGIN Create entities for test ######################
        const createUrl = testConfig.base_url + "entityOperations/upsert"

        let createEntitiesResponse = await axios.post(createUrl, entities, config).catch((e) => {
            //console.log(e)

        }) as AxiosResponse

        expect(createEntitiesResponse.status).equals(201)
        //###################### END Create entities for test ######################


        
        const queryResponse = await axios.get(testConfig.base_url + 'entities/?q=name=="Meckenheim"')
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
});