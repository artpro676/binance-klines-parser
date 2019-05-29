'use strict';

const _ = require('lodash');
const api = require('binance');
const _cliProgress = require('cli-progress');
const MongoClient = require('mongodb').MongoClient;

const BINANCE_KEY = 'vmPUZE6mv9SD5VNHk4HlWFsOr6aKE2zvsw0MuIgwCIPy6utIco14y7Ju91duEh8A';
const BINANCE_SECRET = 'NhqPtmdSJYdKjVHjA7PZj4Mge3R5YNiP1e3UZjInClVN65XAbvqqM6A7H5fATj0j';
const INTERVAL = 90;
const MONGO_URI = 'mongodb://localhost:27017';
const MONGO_DB_NAME = 'binance-history';

const binanceRest = new api.BinanceRest({
    key: BINANCE_KEY,
    secret: BINANCE_SECRET,
    // timeout: 15000, // Optional, defaults to 15000, is the request time out in milliseconds
    // recvWindow: 5000, // Optional, defaults to 5000, increase if you're getting timestamp errors
    disableBeautification: false,
    handleDrift: true
});

let dbConnection;

const connect = () => {
    return new Promise((res, rej) => {
        MongoClient.connect(MONGO_URI, function (err, client) {
            if (err) {
                return rej(err);
            }

            console.log("Connected successfully to server");

            dbConnection = client.db(MONGO_DB_NAME);

            res(dbConnection);
        });
    });
};

const units = {
    '1M': 30 * 24 * 60 * 60,
    '1w': 7 * 24 * 60 * 60,
    '3d': 3 * 24 * 60 * 60,
    '1d': 24 * 60 * 60,
    '12h': 12 * 60 * 60,
    '8h': 8 * 60 * 60,
    '6h': 6 * 60 * 60,
    '4h': 4 * 60 * 60,
    '2h': 2 * 60 * 60,
    '1h': 60 * 60,
    '30m': 30 * 60,
    '15m': 15 * 60,
    '5m': 5 * 60,
    '3m': 3 * 60,
    '1m': 60,
};


const loadSymbols = async () => {
    const {symbols} = await binanceRest.exchangeInfo();
    return _.chain(symbols)
        .map('symbol')
        .value();
};

const runParse = async (symbol, interval) => {

    let endTime = Date.now();
    // let startTime = endTime - (units[interval] * 1000);

    return new Promise((res) => {
        const m = setInterval(async () => {

            try {

            const klines = await binanceRest.klines({
                symbol,
                interval,
//            startTime,
                endTime: endTime - 1,
                limit: 1000
            });

            if (!endTime || klines.lenght === 0) {
                clearInterval(m);
                return res();
            }

            try {    
                await insertDocuments({symbol, interval}, klines);
            } catch (err) {
                console.error(err);
            }

            endTime = _.chain(klines).orderBy(['openTime'], ['desc']).last().get('openTime').value();
        } catch(err) {
            console.error(err);
        }
        }, INTERVAL);
    });
};

const insertDocuments = function({symbol, interval}, data) {
    
    return new Promise((res, rej) => {
        if(_.size(data) === 0) return res();

        // Get the documents collection
        const collection = dbConnection.collection(`${symbol}_${interval}`);
        // Insert some documents
        collection.insertMany(data, function (err, result) {
            if(err) return rej(err);
            console.log(`Inserted ${_.size(result)} items. LAST ITEM `, _.last(data));
            res(result);
        });
    });
};

(async function () {

    const symbols = await loadSymbols();

    await connect();

    // create a new progress bar instance and use shades_classic theme
    // const bar1 = new _cliProgress.Bar({}, _cliProgress.Presets.shades_classic);

    // start the progress bar with a total value of 200 and start value of 0
    // bar1.start(_.size(symbols), 0);

    for (let i in symbols) {
        for (let unit in units) {
            await runParse(symbols[i], unit);
            console.log(`${i}/${symbols.leading}`, symbols[i], unit);
        }
        //  bar1.update(i);
    }
//    bar1.stop();
})();