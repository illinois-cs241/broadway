const byline = require('byline');
const Docker = require('dockerode');
const tmp = require('tmp-promise');

const logger = require('./logger');
const util = require('./util');

const unique = a => a.filter((item, pos) => a.indexOf(item) == pos);

module.exports = async function (job) {
    const docker = new Docker();

    logger.info(job);

    // Check connectivity!
    await docker.ping();

    // First, pull all images in parallel
    const images = unique(job.stages.map(s => s.image));
    await Promise.all(images.map(image => pullImage(docker, image)));

    // Process environment
    const env = (job.env || []).filter(e => e.split('=').length === 2).reduce((acc, e) => {
        acc[e.split('=')[0]] = e;
        return acc;
    }, {});

    // Create a temp directory for this job
    const tmpDir = await tmp.dir({unsafeCleanup: true});
    logger.info(`Created tmp directory ${tmpDir.path}`);

    // We will add the roster if provided
    if (job.students !== undefined) {
        const fs = require('fs');
        const content = JSON.stringify(job.students);

        fs.writeFile(`${tmpDir.path}/roster.json`, content, 'utf8', function (err) {
            if (err) {
                return console.log(err);
            }

            console.log("The roster was saved in the temp directory");
        });
    }

    // We'll store information about each stage here
    const jobResult = {success: true, results: []};

    // Now, run stages in sequence
    for (let i = 0; i < job.stages.length; i++) {
        const stage = job.stages[i];
        logger.info(`Beginning pipeline stage ${i}`);

        // Merge global environment and job-specific environment
        const stageEnv = (stage.env || []).map((e) => {
            const components = e.split('=');
            if (components.length === 1) {
                return env[components[0]] || null;
            } else if (components.length === 2) {
                return e;
            } else {
                return null;
            }
        }).filter(e => e !== null);

        const options = {
            image: stage.image,
            entryPoint: stage.entry_point,
            timeout: stage.timeout,
            environment: stageEnv,
            tmpDir: tmpDir.path,
            enableNetworking: stage.enable_networking,
            hostName: stage.host_name,
            capabilities: stage.capabilities
        };
        const stageResult = await runContainer(docker, options);
        jobResult.results.push(stageResult);
        if (!stageResult.succeeded) {
            jobResult.success = false;
            logger.error(`Pipeline stage ${i} failed; aborting job.`);
            break;
        }
    }

    await tmpDir.cleanup();

    return jobResult;
};

async function pullImage(docker, image) {
    logger.info(`Pulling latest version of "${image}" image`);
    const repository = util.parseRepositoryTag(image);
    const params = {
        fromImage: repository.repository,
        tag: repository.tag || 'latest'
    };

    try {
        const stream = await docker.createImage(params);
        return new Promise((resolve, reject) => {
            docker.modem.followProgress(stream, (err) => {
                if (err) return reject(err);
                resolve();
            }, (output) => {
                logger.info(output);
            });
        });
    } catch (e) {
        logger.warn(`Error pulling "${image}" image; attempting to fall back to cached version`);
        logger.warn(e);
    }
}

async function runContainer(docker, options) {
    const {
        image,
        entryPoint,
        timeout,
        environment,
        tmpDir,
        enableNetworking,
        hostName,
        capabilities
    } = options;

    let results = {
        timedOut: false,
    };
    let jobTimeout = timeout || 30;
    let startTime = new Date();

    try {
        const container = await docker.createContainer({
            Hostname: hostName,
            Image: image,
            AttachStdout: true,
            AttachStderr: true,
            Tty: true,
            NetworkDisabled: !enableNetworking,
            HostConfig: {
                Binds: [
                    `${tmpDir}:/job`
                ],
                Memory: 1 << 30, // 1 GiB
                MemorySwap: 1 << 30, // same as Memory, so no access to swap
                KernelMemory: 1 << 29, // 512 MiB
                DiskQuota: 1 << 30, // 1 GiB
                IpcMode: 'private',
                CpuPeriod: 100000, // microseconds
                CpuQuota: 90000, // portion of the CpuPeriod for this container
                PidsLimit: 1024,
                CapAdd: capabilities
            },
            Env: environment,
            Cmd: entryPoint
        });
        const stream = await container.attach({
            stream: true,
            stdout: true,
            stderr: true,
        });
        const out = byline(stream);
        out.on('data', (line) => {
            logger.info(`container> ${line.toString('utf8')}`);
        });
        await container.start();
        let timeoutId;
        const timerPromise = new Promise((resolve) => {
            timeoutId = setTimeout(() => {
                results.timedOut = true;
                container.kill().then(resolve);
            }, jobTimeout * 1000);
        });
        const waitPromise = container.wait().then(() => {
            clearTimeout(timeoutId);
        });
        logger.info('Waiting for container to complete');
        await Promise.race([timerPromise, waitPromise]);
        const data = await container.inspect();
        if (results.timedOut) {
            results.message = 'Container timed out!';
            results.succeeded = false;
            logger.error(results.message);
        } else {
            results.message = `Container exited with exit code ${data.State.ExitCode}`;
            results.succeeded = data.State.ExitCode == 0;
            results.exitCode = data.State.ExitCode;
            (results.succeeded ? logger.info : logger.error)(results.message);
        }
        await container.remove();
    } catch (e) {
        logger.error(e);
        results.succeeded = false;
        results.message = e.toString();
    }

    let endTime = new Date();
    results.duration = Math.round((endTime - startTime) / 1000);
    return results;
}
