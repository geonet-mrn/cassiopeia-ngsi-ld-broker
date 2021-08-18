import { expect, assert } from "chai";
import axios, { AxiosResponse } from 'axios'
import * as prep from "./testUtil"
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
                "datasetId": "urn:ngsi-ld:dataset1"
            }
        ]
    }
]



describe('6.7.3.2 DELETE entities/<entityId>/attrs/<attrId>', function () {


    before(async () => {

        await prep.deleteAllEntities()

        //###################### BEGIN Create entities for test ######################
        const createUrl = testConfig.base_url + "entityOperations/upsert"

        let response = await prep.axiosPost(createUrl, entities, config)

        
        expect(response.status).equals(201)
        //###################### END Create entities for test ######################
    })


    after(async () => {
        await prep.deleteAllEntities()
    })


    it('should delete the specified attribute (that has only one instance) from the specified entity', async () => {


        // Step 1: Check whether entity exists:
        let getUrl = testConfig.base_url + "entities/urn:ngsi-ld:Municipality:07332009"

        const getResponse1 = await axios.get(getUrl, config)

        expect(getResponse1.data.id).equal("urn:ngsi-ld:Municipality:07332009")
        expect(getResponse1.data.name).to.not.equal(undefined)


        // Step 2: Delete specific instance attribute "name":
        let deleteUrl = testConfig.base_url + "entities/urn:ngsi-ld:Municipality:07332009/attrs/name"
        const deleteResponse = await axios.delete(deleteUrl, config)


        expect(deleteResponse.status == 204)


        // Step 3: Check whether attribute "name" was really deleted:
        const getResponse2 = await axios.get(getUrl, config)

        expect(getResponse2.data.id).equal("urn:ngsi-ld:Municipality:07332009")
        expect(getResponse2.data.name).to.equal(undefined)
    });


    it('should delete only the specified instance from the specified attribute of the specified entity', async () => {

        // Step 1: Check whether entity exists:
        let getUrl = testConfig.base_url + "entities/urn:ngsi-ld:Municipality:07332009"

        const getResponse1 = await axios.get(getUrl, config)

        expect(getResponse1.data.id).equal("urn:ngsi-ld:Municipality:07332009")
        expect(getResponse1.data.multiAttribute).to.not.equal(undefined)


        // Step 2: Delete instance "dataset1" from "multiAttribute":
        let deleteUrl = testConfig.base_url + "entities/urn:ngsi-ld:Municipality:07332009/attrs/multiAttribute?datasetId=urn:ngsi-ld:dataset1"
        const deleteResponse = await axios.delete(deleteUrl, config)

        expect(deleteResponse.status == 204)



        // Step 3: Check whether instance "dataset1" was really deleted:
        const getResponse2 = await axios.get(getUrl, config)

        expect(getResponse2.data.id).equal("urn:ngsi-ld:Municipality:07332009")

        expect(getResponse2.data.multiAttribute).to.not.equal(undefined)

        let attr = getResponse2.data.multiAttribute


        if (!(attr instanceof Array)) {
            attr = [attr]
        }

        expect(attr.length).greaterThan(0)

        for (const instance of attr) {
            expect(instance.datasetId).not.equal("dataset1")
        }
    });
});

