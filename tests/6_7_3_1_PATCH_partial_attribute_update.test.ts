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

const entityId = "urn:ngsi-ld:TestEntity:TestEntity1"

const entities = [
    {
        "id": entityId,
        "type": "TestEntity",


        "name": [
            {
                "type": "Property",
                "value": "This is a test entity"
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



const validPatchFragment = {
    "id": "urn:ngsi-ld:TestEntity:TestEntity1",
    "type": "TestEntity",


    "multiAttribute": [

        {
            "type": "Property",
            "value": "patched default"

        }
    ]
}





const patchFragmentWithInvalidAttributeId = {
    "id": "urn:ngsi-ld:TestEntity:TestEntity2",
    "type": "TestEntity",


    "nonExistingAttribute": [

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
        await prep.deleteAllEntities()

    })


    it('should patch the specified attribute instance', async function () {

        // Step 1: Patch default instance attribute "multiAttribute":
        let patchUrl = testConfig.base_url + "entities/" + entityId + "/attrs/multiAttribute"

        const patchResponse = await axios.patch(patchUrl, validPatchFragment, config)



        expect(patchResponse.status == 204)


        let getUrl = testConfig.base_url + "entities/" + entityId


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



    it('should return HTTP error 404 because the specified entity does not exist', async function () {

        // Step 1: Try to patch with invalid entity ID:
        let patchUrl = testConfig.base_url + "entities/urn:ngsi-ld:NonExistingEntity/attrs/multiAttribute"

        let err = undefined
        const patchResponse = await axios.patch(patchUrl, validPatchFragment, config).catch((e) => err = e)



        expect(err).to.not.be.undefined

        //@ts-ignore
        expect(err.response.status).equals(404)
    });



    it('should return HTTP error 400 because the attribute specified in the URL does not exist in the payload fragment', async function () {

        // Step 1: Try to patch with invalid entity ID:
        let patchUrl = testConfig.base_url + "entities/" + entityId + "/attrs/nonExistingAttribute"

        let err = undefined
        const patchResponse = await axios.patch(patchUrl, validPatchFragment, config).catch((e) => err = e)



        expect(err).to.not.be.undefined

        //@ts-ignore
        expect(err.response.status).equals(400)
    });



    it('should return HTTP error 404 because the attribute specified in the URL and in the payload does not exist in the specified existing entity', async function () {

        // Step 1: Try to patch with invalid entity ID:
        let patchUrl = testConfig.base_url + "entities/" + entityId + "/attrs/nonExistingAttribute"

        let err = undefined
        const patchResponse = await axios.patch(patchUrl, patchFragmentWithInvalidAttributeId, config).catch((e) => err = e)


        expect(err).to.not.be.undefined

        //@ts-ignore
        expect(err.response.status).equals(404)
    });

});

