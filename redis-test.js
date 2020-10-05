
const redis = require('redis');
const redisSearch = require('redis-redisearch');
const async = require('async');
const { v4 } = require('uuid');

const indexName = "business-search";
const searchItems = new Array(10000).fill(0);

redisSearch(redis);

const client = redis.createClient({
    host: 'redis-16640.c212.ap-south-1-1.ec2.cloud.redislabs.com',
    port: 16640,
    auth_pass: 'U6Bq0UlpVTlu2tqb5dTZgQNtOVsFKzNt'
});

const categories = ['groceries', 'food', 'organic', 'resturant', 'milk', 'backery', 'departmental', 'fruit', 'wine', 'fried chicken'];
const fulfillmentType = ['pick-up', 'delivery', 'both'];
const status = ['active', 'suspend', 'closed'];
const liveStatus = ['open', 'close'];

const getRandomIndex = (max) => Math.round(Math.random() * max);

const createSearchIndex = () => {
    client.ft_create(
        indexName, 'ON', 'hash', 'SCHEMA',
        'name', 'text',
        'categories', 'text',
        'fullType', 'text',
        'openTime', 'text',
        'closeTime', 'text',
        'status', 'text',
        'liveStatus', 'text',
        'location', 'geo',
        'radius', 'numeric',
        (err) => {
            if (err) { throw err; }
            console.log('done');
            client.quit();
        }
    )
}

const addSearchIndex = () => {
    async.eachLimit(searchItems, 500, (item, cb) => {
        client.ft_add(
            indexName,  //key name
            v4(),   //doocument ID
            1,  //score
            'FIELDS',
            'name', 'Funny Store',
            'categories', categories[getRandomIndex(9)],
            'fullType', fulfillmentType[getRandomIndex(2)],
            'openTime', '09:30',
            'closeTime', '18:00',
            'status', status[getRandomIndex(2)],
            'liveStatus', liveStatus[getRandomIndex(1)],
            'location', [-133 + Math.random(), 53 + Math.random()].join(','),
            'radius', getRandomIndex(5),
            cb
        );
    },
        (err, response) => {
            if (err) { throw err };
            console.log('Done');
            client.quit();
        })
}

const searchBusinesses = (searchItems = {}, options, callback) => {

    let offset = 0; // default values
    let limit = 50; // default value

    // console.log('searchItems :: ', searchItems)

    const searchQuery = Object.keys(searchItems).reduce((query, searchKey) => {
        return `${query} @${searchKey}:${searchItems[searchKey]}`
    }, '');

    // console.log('searchQuery :: ', searchQuery)

    let searchParams = [
        indexName,    // name of the index
        searchQuery,  // query string,
    ];

    // if limit add the parameters
    if (options.offset || options.limit) {
        offset = options.offset || 0;
        limit = options.limit || 10
        searchParams.push("LIMIT");
        searchParams.push(offset);
        searchParams.push(limit);
    }
    // if sortby add the parameters  
    if (options.sortBy) {
        searchParams.push("SORTBY");
        searchParams.push(options.sortBy);
        searchParams.push((options.ascending) ? "ASC" : "DESC");
    }

    client.ft_search(
        searchParams,
        (err, searchResult) => {
            const totalNumberOfDocs = searchResult[0];
            const totalReturnedDocs = searchResult.length;
            // searchResult.map(result => {
            //     console.log('result is ', result);
            // });

            let businessListResult = [];
            searchResult.forEach((result, index) => {
                if (index === 0) return;
                if (index % 2 === 1) {
                    businessListResult.push({ id: result });
                }

                const business = {};
                for (i = 0; i < result.length; i += 2) {
                    business[result[i]] = result[i + 1];
                }
                // console.log('businessListResult :: ', businessListResult)
                businessListResult[businessListResult.length - 1].data = business;
            });
            client.quit();
            callback(err, businessListResult)
        }
    );
}

const searchBusinessesWithDistance = (searchItems = {}, options, callback) => {

    let offset = 0; // default values
    let limit = 50; // default value

    const { location: { lon, lat }, radius = 2, categories } = searchItems;

    /* raw query : 
    FT.AGGREGATE 
    business-search //index
    "@location:[-132.75072230590592 53.97944847031779 5 km]" //query
    LOAD 3 @location @categories @name 
    APPLY geodistance(@location,-132.75072230590592,53.97944847031779) as distance 
    sortby 2 @distance ASC 
    limit 0 50
 */

    let searchQuery = '';

    if (searchItems.categories) searchQuery = `@categories:${categories}`;
    if (lon && lat) searchQuery = `${searchQuery} @location:[${lon} ${lat} ${radius} km]`;

    let pipeline = [
        indexName,
        `${searchQuery}`,
        "LOAD", "8", "@name", "@categories", "@fullType", "@openTime", "@closeTime", "@status", "@liveStatus", "@location",
        "APPLY", `geodistance(@location,${lon},${lat})`, "as", "distance",
        "SORTBY", "2", "@distance", "ASC"
    ]

    // if sortby add the parameters  
    if (options.sortBy) {
        pipeline.push("SORTBY 2 @distance");
        pipeline.push((options.ascending) ? "ASC" : "DESC");
    }

    // if limit add the parameters
    if (options.offset || options.limit) {
        offset = options.offset || 0;
        limit = options.limit || 10
        pipeline.push("LIMIT");
        pipeline.push(`${offset}`);
        pipeline.push(`${limit}`);
    }

    client.ft_aggregate(
        pipeline,
        (err, searchResult) => {
            if (err) throw err;

            let totalNumberOfDocs = searchResult[0];
            const totalReturnedDocs = searchResult.length;
            let businessListResult = [];
            searchResult.forEach((result, index) => {
                if (index === 0) return;

                const business = {};
                for (i = 0; i < result.length; i += 2) {
                    business[result[i]] = result[i + 1];
                }
                businessListResult.push(business);
            });
            client.quit();
            callback(err, 'success')
        }
    );



}

//normal business-search
searchBusinesses({
    categories: 'organic',
    location: '[-132.75072230590592 53.97944847031779 20 km]'
},
    { offset: 0, limit: 100 },
    (err, result) => console.log(result)
);

// business-search with the distance
// searchBusinessesWithDistance({
//     categories: 'organic',
//     location: {
//         lon: -132.75072230590592,
//         lat: 53.97944847031779
//     },
//     radius: 5
// },
//     { offset: 0, limit: 10 },
//     (err, message) => console.log(message)
// );

// create new search index
// createSearchIndex();

// add items to the index
// addSearchIndex()





module.exports = {
    createSearchIndex,
    addSearchIndex,
    searchBusinesses
}

// while (counter < 10000) {
//     const key = `${v4()}jd4678904`
//     client.set(`${key}`, '9d7647b6-4653-4dc4-824b-e1d86e56899ajd4678904::9d7647b6-4653-4dc4-824b-e1d86e56899ajd4678904::9d7647b6-4653-4dc4-824b-e1d86e56899ajd4678904::9d7647b6-4653-4dc4-824b-e1d86e56899ajd4678904::9d7647b6-4653-4dc4-824b-e1d86e56899ajd4678904::9d7647b6-4653-4dc4-824b-e1d86e56899ajd4678904::9d7647b6-4653-4dc4-824b-e1d86e56899ajd4678904::9d7647b6-4653-4dc4-824b-e1d86e56899ajd4678904::9d7647b6-4653-4dc4-824b-e1d86e56899ajd4678904::9d7647b6-4653-4dc4-824b-e1d86e56899ajd4678904');
//     console.log('key is :: ', key, 'ccounter ::', counter)
//     counter++;
// }

// process.exit(0);