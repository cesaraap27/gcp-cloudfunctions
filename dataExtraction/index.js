const dotenv = require('dotenv').config();
const axios = require('axios');
const speech = require('@google-cloud/speech');
const language = require('@google-cloud/language');
const stripHtml = require('string-strip-html');
const {BigQuery} = require('@google-cloud/bigquery');
const automl = require('@google-cloud/automl').v1;
const _ = require('lodash');
const logHandler = require('./logHandler');
const {serializeError} = require('serialize-error');

//Variables de entorno
const tokenEmail = process.env.TOKEN_EMAIL;
const tokenPass = process.env.TOKEN_PASS;
const recommenderApi = process.env.RECOMMENDER_API ? process.env.RECOMMENDER_API : 'recommender-api-feature-prtailor-748-show-item-eqjdp7sjlq-ew.a.run.app';
const autoMLThreshold = process.env.AUTOML_THRESHOLD ? process.env.AUTOML_THRESHOLD : 0.2;
const modelId = process.env.MODEL_ID;
const projectId = process.env.PROJECT_ID;
const location = process.env.LOCATION ? process.env.LOCATION : 'eu';
const bqDataSetName = process.env.BQ_DATASET ? process.env.BQ_DATASET : 'model_recommendation';
const bqTableName = process.env.BQ_TABLE ? process.env.BQ_TABLE : 'metadataItems';

const blacklist = [
    '2',
    '3',
    'a_vivir_que_son_dos_dias',
    'acento_robinson',
    'cadenaser',
    'carrusel_deportivo',
    'de_buenas_a_primeras',
    'el_cine_en_la_ser',
    'el_larguero',
    'hablar_por_hablar',
    'hora_14',
    'hora_14_fin_de_semana',
    'hora_25',
    'hoy_por_hoy',
    'la_script',
    'la_ventana',
    'play_basket',
    'play_futbol',
    'prisaradio',
    'radio_albacete',
    'radio_algeciras',
    'radio_alicante',
    'radio_aranda',
    'radio_asturias',
    'radio_azul',
    'radio_bierzo',
    'radio_bilbao',
    'radio_cadiz',
    'radio_cartagena',
    'radio_castellon',
    'radio_castilla',
    'radio_club_tenerife',
    'radio_coca_ser_ronda',
    'radio_cordoba',
    'radio_coruna',
    'radio_denia',
    'radio_elche',
    'radio_elda',
    'radio_extremadura',
    'radio_granada',
    'radio_huelva',
    'radio_ibiza',
    'radio_jaen',
    'radio_jerez',
    'radio_leon',
    'radio_linares',
    'radio_lorca',
    'radio_madrid',
    'radio_mallorca',
    'radio_medina',
    'radio_murcia',
    'radio_ourense',
    'radio_palencia',
    'radio_pamplona',
    'radio_penafiel',
    'radio_pontevedra',
    'radio_rioja',
    'radio_salamanca',
    'radio_san_sebastian',
    'radio_santander',
    'radio_segovia',
    'radio_sevilla',
    'radio_ubeda',
    'radio_valencia',
    'radio_valladolid',
    'radio_vigo',
    'radio_zamora',
    'radio_zaragoza',
    'ser_almeria',
    'ser_andujar',
    'ser_avila',
    'ser_ciudad_real',
    'ser_consumidor',
    'ser_cuenca',
    'ser_gijon',
    'ser_guadalajara',
    'ser_historia',
    'ser_lanzarote',
    'ser_las_palmas',
    'ser_madrid_norte',
    'ser_madrid_oeste',
    'ser_madrid_sur',
    'ser_malaga',
    'ser_runner',
    'ser_soria',
    'ser_talavera',
    'ser_toledo',
    'ser_vitoria',
    'sofa_sonoro',
    'radio_denia',
    'radio_elche',
    'radio_elda',
    'radio_extremadura',
    'radio_granada',
    'radio_huelva',
    'radio_ibiza',
    'radio_jaen',
    'radio_jerez',
    'radio_leon',
    'radio_linares',
    'radio_lorca',
    'radio_madrid',
    'radio_mallorca',
    'radio_medina',
    'radio_murcia',
    'radio_ourense',
    'radio_palencia',
    'radio_pamplona',
    'radio_penafiel',
    'radio_pontevedra',
    'radio_rioja',
    'radio_salamanca',
    'radio_san_sebastian',
    'radio_santander',
    'radio_segovia',
    'radio_sevilla',
    'radio_ubeda',
    'radio_valencia',
    'radio_valladolid',
    'radio_vigo',
    'radio_zamora',
    'radio_zaragoza',
    'ser_almeria',
    'ser_andujar',
    'ser_avila',
    'ser_ciudad_real',
    'ser_consumidor',
    'ser_cuenca',
    'ser_gijon',
    'ser_guadalajara',
    'ser_historia',
    'ser_lanzarote',
    'ser_las_palmas',
    'ser_madrid_norte',
    'ser_madrid_oeste',
    'ser_madrid_sur',
    'ser_malaga',
    'ser_runner',
    'ser_soria',
    'ser_talavera',
    'ser_toledo',
    'ser_vitoria',
    'sofa_sonoro',
    'sucedio_una_noche',
];


//Inicializamos clientes
const clientSpeechClient = new speech.SpeechClient();

async function dataExtraction(event, context) {
    try {
        logHandler.info('Start the execution');
        console.log(dotenv);
        const myEventMsg = {
            type: 'item',
            companyId: 2,
            itemGuid: '7a17fa94-5595-4305-afaf-d199af247743',
            externalId: 'bd951ae3-24c1-4688-b540-8bf6e334632f',
            rawURL: 'https://traffic.omny.fm/d/clips/2446592a-b80e-4d28-a4fd-ae4c0140ac11/25b68ecd-f6ad-45fe-925b-aead0083eaab/80c83da9-a8e3-4771-8449-af1900a02eeb/audio.mp3?utm_source=Podcast&in_playlist=506d831b-b2ed-4627-960d-aead0083ead5',
        };

        const rawURL = myEventMsg.rawURL;
        const itemGuid = myEventMsg.itemGuid;
        const externalId = myEventMsg.externalId;
        const companyId = myEventMsg.companyId;
        let token = await getToken();

        logHandler.info('Process successfully completely', itemGuid);
    } catch (error) {
        logHandler.error('Error data extraction input:', myEventMsg);
        logHandler.error('Error data extraction error: ', serializeError(error));
        await changeStatusItems(itemGuid, companyId, 'error', token, true, {
            name: error.name,
            inComponent: 'dataExtraction',
        }).catch(() => {
            logHandler.error('Error changing status to error');
        });
    }
}

async function updateShowMeanDuration(showGuid, companyId, token) {
    let url = `https://${recommenderApi}/api/v1/company/${companyId}/shows/meanDuration/${showGuid}`;
    let config = {
        method: 'put',
        url,
        headers: {
            Authorization: `Bearer ${token}`,
        },
    };
    await axios(config)
        .then(() => {
            console.log(`Updated show duration of ${showGuid} successfully`);
        })
        .catch((err) => {
            console.log(err);
            console.log('Error in updating show duration query');
        });
}

async function getItemInfo(externalId, companyId, token) {
    //TODO: Crear endpoint para buscar por itemGuid (identificador interno)
    //let url = `https://${recommenderApi}/api/v1/company/${companyId}/items/${itemGuid}`;
    let url = `https://${recommenderApi}/api/v1/company/${companyId}/items/${externalId}`;
    console.log('getItem url: ', url);
    const config = {
        headers: {Authorization: `Bearer ${token}`},
    };
    try {
        let item = await axios.get(url, config);
        console.log("Get Item Info: ", item.status);
        return item.data;
    } catch (error) {
        console.log(error);
    }


}

async function getToken() {
    let body = {
        email: tokenEmail,
        password: tokenPass,
    };

    let url = `https://${recommenderApi}/api/v1/auth/login`;
    let response = await axios.post(url, body, {});
    return response.data.token;
}

/**
 * Call GCP: API language to retrieve transcription text by operationId.
 * @param {*} operationId
 * @param {*} rawURL
 * @returns
 */
async function getTranscription(operationId, rawURL) {
    const response = await clientSpeechClient.checkLongRunningRecognizeProgress(operationId);
    let done = response.done;
    console.log('done: ', done + ' url: ', rawURL);
    let transcripcion = '';
    if (done) {
        for (const result of response.result.results) {
            transcripcion = transcripcion + ' ' + result.alternatives[0].transcript;
        }
        return transcripcion;
    }

    return false;
}

/**
 * Call GCP: API Language for entity extraction.
 * @param {*} string
 * @returns
 */
async function entityExtraction(string) {
    // Creates a client
    const client = new language.LanguageServiceClient();
    let respEntities = [];
    let chunckString = string.match(/.{1,55000}/g);
    console.log('Number of chunks:' + chunckString.length);
    // Prepares a document, representing the provided text
    for (const chunk of chunckString) {
        const document = {
            content: chunk,
            type: 'PLAIN_TEXT',
        };

        // Detects entities in the document
        const [result] = await client
            .analyzeEntities({document})
            .catch((error) => logHandler.error('Error in analyzeEntities', serializeError(error)));
        const entities = result.entities;
        //console.log("entities:", result.entities)

        entities.forEach((entity) => {
            respEntities.push({name: entity.name, salience: entity.salience});
        });
    }

    return respEntities;
}

async function sentimentAnalysis(text) {
    let firstChunck = text.match(/.{1,55000}/g)[0];
    // Creates a client
    const client = new language.LanguageServiceClient();

    const document = {
        content: firstChunck,
        type: 'PLAIN_TEXT',
    };

    // Detects the sentiment of the document
    const [result] = await client.analyzeSentiment({document});

    const sentiment = result.documentSentiment;

    return sentiment.score;
}

async function insertBQ(datasetName, tableName, row) {
    const bigquery = new BigQuery();
    const timestamp = BigQuery.timestamp(new Date());
    const dataset = bigquery.dataset(datasetName);
    const table = dataset.table(tableName);
    row.ingestionDate = timestamp;

    await (table.insert(row)
            .then((data) => {
                const apiResponse = data[0];
                logHandler.info('Insert rows successfully', apiResponse);
            })
            .catch((err) => {
                if (err) {
                    logHandler.error(`Error insert into BQ: ${row.itemGuid}`, err);
                }
            })
    );
}

async function changeStatusItems(itemGuid, companyId, status, token, error, body) {
    const message = body ? JSON.stringify(body) : '';
    const config = {
        headers: {Authorization: `Bearer ${token}`},
    };
    let url = `https://${recommenderApi}/api/v1/company/${companyId}/items/${itemGuid}/changeStatus/${status}`;
    console.log('changeStatus url: ', url);
    if (!error) {
        let change = await axios.post(url, {}, config);
        return change.data;
    } else {
        let change = await axios.post(url, {errorMsg: message}, config);
        return change.data;
    }
}

async function setItemSentiment(itemGuid, externalId, externalShowId, companyId, sentiment, token) {

    const config = {
        headers: {Authorization: `Bearer ${token}`},
    };
    let body = {
        items: [
            {
                itemGuid: itemGuid,
                externalId: externalId,
                sentiment: sentiment,
                externalShowId: externalShowId,
            },
        ],
    };
    // TODO: Implementar un endpoint de actualizacion de items mediante el identificador interno "itemGuid".
    //let url = `https://${recommenderApi}/api/v1/company/${companyId}/items`;
    let url = `https://${recommenderApi}/api/v1/company/${companyId}/items`;
    console.log('set sentiment url: ', url);
    try {
        let response = await axios.post(url, body, config);
        console.log('response: ', response.status);
    } catch (error) {
        console.error('error in setting the tags', error);
        throw new Error(error);
    }
    return;
}

async function setItemTags(externalId, companyId, prisaTagDesc, prisaTagTrans, token) {
    let allTags = _.uniq(_.concat(prisaTagTrans, prisaTagDesc));
    const config = {
        headers: {Authorization: `Bearer ${token}`},
    };
    let body = {
        tags: allTags,
    };
    // TODO: Implementar cambiar el endpoint de actualizacion de items mediante el identificador interno "itemGuid".
    let url = `https://${recommenderApi}/api/v1/company/${companyId}/tags/${externalId}`;
    console.log('set tag url: ', url);
    try {
        await axios.post(url, body, config);
    } catch (error) {
        console.error('error in setting the tags', error);
        throw new Error(error);
    }

    return 'ok';
}

/**
 * Call Custome MODEL generated by Podbuddy.
 * @param {*} content
 * @returns
 */
async function predict(content) {
    const clientOptions = {apiEndpoint: 'eu-automl.googleapis.com'};
    // Construct request
    // Instantiates a client
    const client = new automl.PredictionServiceClient(clientOptions);
    let chunckString = content.match(/.{1,55000}/g);
    let respPrisaTags = [];
    for (const chunck of chunckString) {
        const request = {
            name: client.modelPath(projectId, location, modelId),
            payload: {
                textSnippet: {
                    content: chunck,
                    mimeType: 'text/plain', // Types: 'test/plain', 'text/html'
                },
            },
        };

        const [response] = await client.predict(request);

        let predictionList = response.payload;
        predictionList.forEach((prediction) => {
            if (prediction.classification.score >= autoMLThreshold)
                respPrisaTags.push({
                    name: prediction.displayName,
                    salience: prediction.classification.score,
                });
        });
    }
    let prisaTags = await filterBlackList(respPrisaTags, blacklist);
    let respPrisaTagsFiltered = _.uniqBy(prisaTags, 'name');
    return respPrisaTagsFiltered;
}

async function filterBlackList(tagList, blacklist) {
    let listFiltered = _.filter(tagList, function (o) {
        return !_.includes(blacklist, o.name);
    });
    return listFiltered;
}

module.exports = {
    dataExtraction,
};

//dataExtraction();