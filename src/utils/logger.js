const { createLogger, format, transports } = require('winston');
require('winston-daily-rotate-file');

const LOG_LEVEL = process.env.LOG_LEVEL || 'info';
const IS_DEBUG = LOG_LEVEL.toLowerCase() === 'debug';

const fileRotateTransport = new transports.DailyRotateFile({
  dirname: './logs',
  filename: '%DATE%.log',
  datePattern: 'YYYY-MM-DD',
  maxFiles: '14d',
  level: LOG_LEVEL,
});

const logger = createLogger({
  level: LOG_LEVEL,
  format: format.combine(
    format.timestamp({ format: 'YYYY-MM-DD HH:mm:ss' }),
    format.printf(({ timestamp, level, message, ...meta }) =>
      `${timestamp} [${level.toUpperCase()}] ${message} ${Object.keys(meta).length ? JSON.stringify(meta) : ''}`
    )
  ),
  transports: [
    new transports.Console({ level: LOG_LEVEL }),
    fileRotateTransport
  ]
});

module.exports = logger;