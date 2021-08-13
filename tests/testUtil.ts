import axios, { AxiosRequestConfig, AxiosResponse } from 'axios'
import { error } from 'console';
import { testConfig } from './testConfig'


export async function sleep(ms: number) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

export async function deleteAllEntities() {
    let response = await axios.delete(testConfig.base_url + "entities/", { auth: testConfig.auth })
}

export async function axiosGet(url: string, config: AxiosRequestConfig): Promise<AxiosResponse> {

    let errorResponse = undefined


    let response = await axios.get(url, config).catch((err) => {
        errorResponse = err.response
    }) as AxiosResponse

    if (errorResponse != undefined) {
        response = errorResponse
    }

    console.log(response.status)

    return response
}


export async function axiosPost(url: string, payload: any, config: AxiosRequestConfig): Promise<AxiosResponse> {

    let errorResponse = undefined


    let response = await axios.post(url, payload,config).catch((err) => {
        errorResponse = err.response
    }) as AxiosResponse

    if (errorResponse != undefined) {
        response = errorResponse
    }


    console.log(response.status)

    return response
}