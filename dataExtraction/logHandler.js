const { serializeError } = require('serialize-error');

const logSeverity = {
  default: 'DEFAULT',
  debug: 'DEBUG',
  info: 'INFO',
  notice: 'NOTICE',
  warning: 'WARNING',
  error: 'ERROR',
  critical: 'CRITICAL',
  alert: 'ALERT',
  emergency: 'EMERGENCY',
};

const createLog = (severity) => {
  return (message, info) => {
    let data = null
    if (typeof info === 'object'){
        data = serializeError(info);
    } else if (info) {
        message = `${message} ${info}`;
    }
    console.log(JSON.stringify(Object.assign({ severity, message }, data)));
  };
};

const logFactory = {
  default: createLog(logSeverity.default),
  debug: createLog(logSeverity.debug),
  info: createLog(logSeverity.info),
  notice: createLog(logSeverity.notice),
  warning: createLog(logSeverity.warning),
  error: createLog(logSeverity.error),
  critical: createLog(logSeverity.critical),
  alert: createLog(logSeverity.alert),
  emergency: createLog(logSeverity.emergency),
};

module.exports = logFactory;
