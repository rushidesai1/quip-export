jest.mock('node-fetch');
const fetch = require('node-fetch');
const {Response} = jest.requireActual('node-fetch');

jest.mock('../common/LoggerAdapter');
const LoggerAdapter = require('../common/LoggerAdapter');

const waitingTime = Math.ceil((new Date().getTime() / 1000) + 2 ); //2 sec
const QuipService = require('../QuipService');

const blob = [1, 2, 3, 4, 5, 6];

function createResponse200() {
    return new Response(JSON.stringify({data:123456}));
}

function createResponse200Blob() {
    const response = new Response(blob, {
        headers: {
            'content-type': 'image/jpeg',
            'content-length': blob.length,
            'content-disposition': 'attachment'
        }
    });
    response.blob = jest.fn(() => blob);
    return response;
}

//general error response
function createResponse500() {
    return new Response("", {status: 500});
}

function createResponse503() {
    return new Response(JSON.stringify({waiting:waitingTime}), {
        status: 503,
        headers: {'x-ratelimit-reset': waitingTime}
    });
}

function createResponse429() {
    return new Response('', {status: 429});
}

const quipService = new QuipService('###TOKEN###', 'http://quip.com');

beforeEach(() => {
    quipService.stats.query_count = 0;
    quipService.querries503 = new Map();
});

test('constructor tests', async () => {
    expect(quipService.accessToken).toBe('###TOKEN###');
    expect(quipService.apiURL).toBe('http://quip.com');
    expect(quipService.logger).toBeInstanceOf(LoggerAdapter);
});

test('_apiCall response with 503 code', async () => {
    fetch.mockReturnValue(Promise.resolve(createResponse200())).mockReturnValueOnce(Promise.resolve(createResponse503()));
    const res = await quipService._apiCall('/someURL');
    expect(res.data).toBe(123456);
    expect(quipService.stats.query_count).toBe(2);
});

test('_apiCall response with 503 code more than 10 times', async () => {
    quipService.waitingMs = 50;
    fetch.mockReturnValue(Promise.resolve(createResponse503()));
    const res = await quipService._apiCall('/someURL');
    expect(res).toBe(undefined);
    expect(quipService.logger.error).toHaveBeenCalledWith('Couldn\'t fetch /someURL, tryed to get it 10 times');
});

test('_apiCall response with 500 code', async () => {
    fetch.mockReturnValue(Promise.resolve(createResponse500()));
    await quipService._apiCall('/someURL');
    expect(quipService.logger.debug).toHaveBeenCalledWith('Couldn\'t fetch /someURL, received 500');
});

test('_apiCall: fetch with exeption', async () => {
    const error = new Error('Some server error');
    fetch.mockImplementation(() => {
        throw error;
    });
    await quipService._apiCall('/someURL');
    expect(quipService.logger.error).toHaveBeenCalledWith('Couldn\'t fetch /someURL, ', error);
});

test('_apiCall for JSON', async () => {
    fetch.mockReturnValue(Promise.resolve(createResponse200()));
    const res = await quipService._apiCallJson('/someURL');
    expect(res.data).toBe(123456);
});

test('_apiCall for Blob', async () => {
    fetch.mockReturnValue(Promise.resolve(createResponse200Blob()));
    const res = await quipService._apiCallBlob('/someURL');
    expect(res).toBe(blob);
});

test('_apiCallJson', async () => {
    quipService._apiCall = jest.fn();
    await quipService._apiCallJson('/someURL');
    expect(quipService._apiCall).toHaveBeenCalledWith(expect.anything(), expect.anything(), false);
});

test('_apiCallBlob', async () => {
    quipService._apiCall = jest.fn();
    await quipService._apiCallBlob('/someURL');
    expect(quipService._apiCall).toHaveBeenCalledWith(expect.anything(), expect.anything(), true);
});

test('_get429Wait default retries', async () => {
    const wait1 = quipService._get429Wait('/url');
    const wait2 = quipService._get429Wait('/url');
    const wait3 = quipService._get429Wait('/url');
    const wait4 = quipService._get429Wait('/url');
    expect(wait1).toBe(quipService.waitingMs);
    expect(wait2).toBe(quipService.waitingMs);
    expect(wait3).toBe(quipService.waitingMs);
    expect(wait4).toBeNull();
});

test('_get429Wait configurable retries', async () => {
    const qs = new QuipService('T', 'http://quip.com', 2);
    const w1 = qs._get429Wait('/url');
    const w2 = qs._get429Wait('/url');
    const w3 = qs._get429Wait('/url');
    expect(w1).toBe(qs.waitingMs);
    expect(w2).toBe(qs.waitingMs);
    expect(w3).toBeNull();
});