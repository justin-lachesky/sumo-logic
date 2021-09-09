const async = require('async');
const gaxios = require('gaxios');
const fs = require('fs');
const https = require('https');
const config = require('./config/config');
const gaxiosErrorToPojo = require('./utils/errorToPojo');

const { formatISO, subDays, subWeeks, subMonths, subYears } = require('date-fns');
const entityTemplateReplacementRegex = /{{entity}}/g;

const _configFieldIsValid = (field) => typeof field === 'string' && field.length > 0;

let Logger;

function startup(logger) {
  Logger = logger;
}

const getStartDate = (options) => {
  let currentDate = new Date();
  let range;

  switch (options.timeRange.value) {
    case 'day':
      range = subDays(currentDate, 1);
    case 'week':
      range = subWeeks(currentDate, 1);
    case 'month':
      range = subMonths(currentDate, 1);
    case 'year':
      range = subYears(currentDate, 1);
    default:
      null;
  }
  return formatISO(range);
};

const requestDefaults = (options) => {
  const {
    request: { ca, cert, key, passphrase, rejectUnauthorized, proxy }
  } = config;

  const httpsAgent = new https.Agent({
    ...(_configFieldIsValid(ca) && { ca: fs.readFileSync(ca) }),
    ...(_configFieldIsValid(cert) && { cert: fs.readFileSync(cert) }),
    ...(_configFieldIsValid(key) && { key: fs.readFileSync(key) }),
    ...(_configFieldIsValid(passphrase) && { passphrase }),
    ...(typeof rejectUnauthorized === 'boolean' && { rejectUnauthorized })
  });

  if (_configFieldIsValid(proxy)) {
    process.env.HTTP_PROXY = proxy;
    process.env.HTTPS_PROXY = proxy;
  }

  gaxios.instance.defaults = {
    agent: httpsAgent,
    headers: {
      Authorization:
        'Basic ' +
        Buffer.from(options.accessId + ':' + options.accessKey).toString('base64'),
      'Content-type': 'application/json'
    },
    retry: false
  };
};

const doLookup = async (entities, options, cb) => {
  let lookupResults;

  requestDefaults(options);

  try {
    lookupResults = await async.parallelLimit(
      entities.map((entity) => async () => {
        const lookupResult = await getJobMessages(entity, options);
        return _isMiss(lookupResult)
          ? {
              entity,
              data: null
            }
          : lookupResult;
      }),
      10
    );
  } catch (err) {
    const handledError = gaxiosErrorToPojo(err);
    Logger.error({ err: handledError }, 'Lookup Error');
    return cb(handledError);
  }

  Logger.trace({ lookupResults }, 'lookupResults');
  return cb(null, lookupResults);
};

const createJob = async (entity, options) => {
  const query = options.query.replace(entityTemplateReplacementRegex, entity.value);
  const endDate = formatISO(new Date());

  const job = await gaxios.request({
    method: 'POST',
    url: `https://api.us2.sumologic.com/api/v1/search/jobs`,
    data: {
      query,
      from: getStartDate(options),
      to: endDate,
      timeZone: options.timeZone,
      byReceiptTime: true
    }
  });
  return job;
};

const getCreatedJobId = async (entity, options) => {
  let result;

  try {
    const job = await createJob(entity, options);

    if (Object.keys(job).length > 0) {
      const getJobResults = async () => {
        result = await gaxios.request({
          method: 'GET',
          url: `https://api.us2.sumologic.com/api/v1/search/jobs/${job.data.id}`
        });

        if (result.data.state === 'DONE GATHERING RESULTS') {
          return {
            jobId: job.data.id
          };
        } else {
          await sleep(1000);
          return getJobResults();
        }
      };

      return getJobResults();
    }
  } catch (err) {
    throw err;
  }
};

const getJobMessages = async (entity, options) => {
  let results;

  const createdJobId = await getCreatedJobId(entity, options).catch((err) => {
    if (err) {
      Logger.error({ ERR: err });
      throw err;
    }
  });

  if (createdJobId) {
    results = await gaxios.request({
      method: 'GET',
      url: `https://api.us2.sumologic.com/api/v1/search/jobs/${createdJobId.jobId}/messages?offset=0&limit=10`
    });
  }

  return {
    entity,
    data:
      Object.keys(results.data).length > 0
        ? { summary: getSummary(results.data), details: results.data }
        : null
  };
};

function getSummary(data) {
  let tags = [];
  let cache = [];

  if (Object.keys(data).length > 0) {
    const totalMessages = data.messages.length;
    tags.push(`Messages: ${totalMessages}`);
  }

  if (Object.keys(data).length > 0) {
    data.messages.map((message) => {
      if (!cache.includes(message.map._source)) {
        tags.push(`_Source: ${message.map._source}`);
        cache.push(message.map._source);
      }
    });
  }
  return tags;
}

function validateOption(errors, options, optionName, errMessage) {
  if (
    typeof options[optionName].value !== 'string' ||
    (typeof options[optionName].value === 'string' &&
      options[optionName].value.length === 0)
  ) {
    errors.push({
      key: optionName,
      message: errMessage
    });
  }
}

function validateOptions(options, callback) {
  let errors = [];

  validateOption(errors, options, 'accessId', 'You must provide a valid access id.');
  validateOption(errors, options, 'accessKey', 'You must provide a valid access key.');
  validateOption(errors, options, 'timeZone', 'You must provide a valid time zone.');
  validateOption(errors, options, 'timeRange', 'You must provide a valid time range.');
  validateOption(errors, options, 'query', 'You must provide a valid query.');

  callback(null, errors);
}

const _isMiss = (lookupResult) =>
  !lookupResult ||
  lookupResult.data.details.messages.length <= 0 ||
  lookupResult.data.details.fields.length <= 0;

const sleep = async (time) =>
  new Promise((res, rej) => {
    setTimeout(() => res(), time);
  });

module.exports = {
  doLookup,
  startup,
  createJob,
  getCreatedJobId,
  validateOptions
};
