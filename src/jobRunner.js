#!/usr/bin/env node
const logger = require('./logger');
const handleJob = require('./handleJob');

(async () => {
    const job = JSON.parse(process.argv[2]);
    const results = await handleJob(job);
    logger.info('Finished with results:');
    const res_string = JSON.stringify(results, null, 4);
    logger.info(res_string);
    fs.writeFile("temp_result.json", res_string, (err) => {
        if (err) {
            console.error(err);
        }
    });
})().catch((e) => {
    logger.error(e);
});
