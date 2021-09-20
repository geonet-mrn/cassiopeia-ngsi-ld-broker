import { expect, assert } from "chai";
import axios, { AxiosRequestConfig, AxiosResponse } from 'axios'
import * as prep from "./testUtil"
import { testConfig } from './testConfig'
import { axiosDelete } from "./testUtil";



let config = {
    headers: { "content-type": "application/ld+json" },
    auth: testConfig.auth
}


const entityId = "urn:ngsi-ld:TemporalTestEntity:test"

const temporalEntity = {


    "id": entityId,
    "type": "TemporalTestEntity",

    "testProperty": [{
        "type": "Property",
        "value": 1
    }],


    "propertyToDelete": [
        {
            "type": "Property",
            "value": 1,
            "datasetId": "urn:ngsi-ld:notdefault1"
        },
        {
            "type": "Property",
            "value": 2,
            "datasetId": "urn:ngsi-ld:notdefault2"
        },
        {
            "type": "Property",
            "value": 3
        }]
}



describe('6.21.3.1 DELETE temporal/entities/<entityId>/attrs/<attrId>', function () {

    before(async () => {
        await prep.deleteAllEntities()

        //############# BEGIN Create entity ##############

        let err: any = undefined

        let createResponse = await axios.post(testConfig.base_url + "temporal/entities/", temporalEntity, config).catch((e) => {
            err = e
        }) as AxiosResponse

        expect(createResponse).to.not.be.undefined

        expect(createResponse.status).equals(201)
        //############# END Create entity ##############
    })


    after(async () => {
        await prep.deleteAllEntities()
    })



    it("should only delete the default instances (i.e. without datasetId) of the specified attribute from the specified temporal entity if no datasetId is provided and deleteAll is not 'true'", async function () {

        // Delete property "propertyToDelete":
        let deleteUrl = testConfig.base_url + "temporal/entities/" + entityId + "/attrs/propertyToDelete"

        let deleteResponse = await axiosDelete(deleteUrl, config)

        expect(deleteResponse.status).equals(204)




        let url = testConfig.base_url + "temporal/entities/" + entityId

        let getResponse = await prep.axiosGet(url, config)

        const entity = getResponse.data

        console.log(entity)

        expect(getResponse.data.id).equals(entityId)
        expect(getResponse.data.propertyToDelete.length).equals(2)      
    })




    /*
    it("should delete the instances with the specified datasetId of the specified attribute from the specified temporal entity", async function () {


        // Delete property "propertyToDelete" with 'deleteAll=true':

        let err = undefined
        let deleteUrl = testConfig.base_url + "temporal/entities/" + entityId + "/attrs/propertyToDelete?datasetId=urn:ngsi-ld:notdefault1"

        let deleteResponse = await axios.delete(deleteUrl, config).catch((e) => {
            err = e
        }) as AxiosResponse

        if (err != undefined) {
            console.log(err)
        }
        expect(deleteResponse).to.not.be.undefined

        expect(deleteResponse.status).equals(204)


        // TODO: Check presence of location header in response

        err = undefined

        let url = testConfig.base_url + "temporal/entities/" + entityId

        let getResponse = await axios.get(url, config).catch((e) => {
            err = e
        }) as AxiosResponse




        expect(getResponse).to.not.be.undefined

        expect(getResponse.data.id).equals(entityId)

        expect(getResponse.data.propertyToDelete.length).equals(1)

        const compareEntity = {


            "id": entityId,
            "type": "TemporalTestEntity",
            "testProperty": [{
                "type": "Property",
                "value": 1
            }],

            "propertyToDelete": [
                {
                    "type": "Property",
                    "value": 1,
                    "datasetId": "urn:ngsi-ld:notdefault2"
                }]


        }

        // NOTE: This does no longer work because of the instanceIds

        //expect(JSON.stringify(getResponse.data)).equals(JSON.stringify(compareEntity))


    })




    it("should delete all instances of the specified attribute from the specified temporal entity if the GET parameter 'deleteAll=true' is provided", async function () {


        // Delete property "propertyToDelete" with 'deleteAll=true':

        let err = undefined
        let deleteUrl = testConfig.base_url + "temporal/entities/" + entityId + "/attrs/propertyToDelete?deleteAll=true"

        let deleteResponse = await axios.delete(deleteUrl, config).catch((e) => {
            err = e
        }) as AxiosResponse

        if (err != undefined) {
            console.log(err)
        }
        expect(deleteResponse).to.not.be.undefined

        expect(deleteResponse.status).equals(204)


        // TODO: Check presence of location header in response

        err = undefined

        let url = testConfig.base_url + "temporal/entities/" + entityId

        let getResponse = await axios.get(url, config).catch((e) => {
            err = e
        }) as AxiosResponse




        expect(getResponse).to.not.be.undefined

        expect(getResponse.data.id).equals(entityId)

        expect(getResponse.data.propertyToDelete).to.be.undefined

        const compareEntity = {


            "id": entityId,
            "type": "TemporalTestEntity",
            "testProperty": [{
                "type": "Property",
                "value": 1
            }]

            // NOTE: The property "deleteProperty" no longer exists in the compare entity
        }

        // NOTE: This does no longer work because of the instanceIds


        //expect(JSON.stringify(getResponse.data)).equals(JSON.stringify(compareEntity))


    })
*/

});

