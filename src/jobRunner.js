#!/usr/bin/env node
const logger = require('./logger');
const handleJob = require('./handleJob');

(async () => {
    const job = JSON.parse(process.argv[2]);
    const results = await handleJob(job);
    const res_string = JSON.stringify(results, null, 4);

    logger.info('Finished with results:');
    logger.info(res_string);

    // generate result file which grader will send to the API
    var fs = require("fs");
    fs.writeFile(process.argv[3], res_string, (err) => {
        if (err) {
            console.error(err);
        }
    });
})().catch((e) => {
    logger.error(e);
});
