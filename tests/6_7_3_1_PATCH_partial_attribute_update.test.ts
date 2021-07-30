import { expect, assert } from "chai";
import axios, { AxiosResponse } from 'axios'
import * as prep from "././testUtil"
import { testConfig } from './testConfig'

const config = {
    headers: {
        "content-type": "application/ld+json"
    },
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

        "multiAttribute": [
            {
                "type": "Property",
                "value": "default"
            },
            {
                "type": "Property",
                "value": "value1",
                "datasetId": "dataset1"
            }
        ]
    }
]



const patchFragment = {
    "id": "urn:ngsi-ld:Municipality:07332009",
    "type": "Municipality",


    "multiAttribute": [

        {
            "type": "Property",
            "value": "patched default"

        }
    ]
}




describe('6.7.3.1 PATCH entities/<entityId>/attrs/<attrId>', function () {


    beforeEach(async () => {
        await prep.deleteAllEntities()


        //###################### BEGIN Create entities for test ######################
        const createUrl = testConfig.base_url + "entityOperations/upsert"

        let createEntitiesResponse = await axios.post(createUrl, entities, config).catch((e) => {
            //console.log(e)

        }) as AxiosResponse


        expect(createEntitiesResponse.status).equals(201)
        //###################### END Create entities for test ######################

    })


    afterEach(async () => {
   //     await prep.deleteAllEntities()

    })


    it('should patch the specified attribute instance', async function () {





        // Step 1: Patch default instance attribute "multiAttribute":
        let patchUrl = testConfig.base_url + "entities/urn:ngsi-ld:Municipality:07332009/attrs/multiAttribute"
        const patchResponse = await axios.patch(patchUrl, patchFragment, config).catch((e) => {
            console.log(e.response.data)
            return
        })


        if (!patchResponse) {
            return new Promise((resolve, reject) => reject())
        }

        expect(patchResponse.status == 204)


        let getUrl = testConfig.base_url + "entities/urn:ngsi-ld:Municipality:07332009"


        // Step 3: Check whether attribute "name" was really patched:
        const getResponse2 = await axios.get(getUrl, config)

        let attr = getResponse2.data.multiAttribute


        if (!(attr instanceof Array)) {
            attr = [attr]
        }

      
        expect(attr.length).greaterThan(0)

        for (const instance of attr) {
            if (instance.datasetId == undefined) {
                expect(instance.value).equal("patched default")
            }
        }

    });
});

