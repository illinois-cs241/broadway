#!/usr/bin/env node
const logger = require('./logger');
const handleJob = require('./handleJob');

(async () => {
    const job = JSON.parse(process.argv[2]);
    const results = await handleJob(job);
    logger.info('Finished with results:');
    const res_string = JSON.stringify(results, null, 2);
    logger.info(res_string);
    console.error(res_string); // workaround to communicate results to caller process
})().catch((e) => {
    logger.error(e);
});
