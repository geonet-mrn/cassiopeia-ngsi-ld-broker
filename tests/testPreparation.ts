import { expect } from "chai";
import * as fs from 'fs'
import axios, { AxiosRequestConfig, AxiosResponse } from 'axios'
import * as uuid from 'uuid'
import { testConfig } from './testConfig'




export async function deleteAllEntities() {
    let response = await axios.delete(testConfig.base_url + "entities/", { auth: testConfig.auth })
}

