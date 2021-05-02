import axios from 'axios'
import { testConfig } from './testConfig'


export async function sleep(ms : number) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

export async function deleteAllEntities() {
    let response = await axios.delete(testConfig.base_url + "entities/", { auth: testConfig.auth })
}

