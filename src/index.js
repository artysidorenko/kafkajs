const {
  createLogger,
  LEVELS: { INFO },
} = require('./loggers')

const LoggerConsole = require('./loggers/console')
const Cluster = require('./cluster')
const createProducer = require('./producer')
const createConsumer = require('./consumer')
const createAdmin = require('./admin')
const ISOLATION_LEVEL = require('./protocol/isolationLevel')

const PRIVATE = {
  CREATE_CLUSTER: Symbol('private:Kafka:createCluster'),
  LOGGER: Symbol('private:Kafka:logger'),
  OFFSETS: Symbol('private:Kafka:offsets'),
}

module.exports = class Client {
  constructor({
    brokers,
    ssl,
    sasl,
    clientId,
    connectionTimeout,
    authenticationTimeout,
    retry,
    logLevel = INFO,
    logCreator = LoggerConsole,
    allowExperimentalV011 = true,
  }) {
    this[PRIVATE.OFFSETS] = new Map()
    this[PRIVATE.LOGGER] = createLogger({ level: logLevel, logCreator })
    this[PRIVATE.CREATE_CLUSTER] = ({
      metadataMaxAge = 300000,
      allowAutoTopicCreation = true,
      maxInFlightRequests = null,
      isolationLevel,
    }) =>
      new Cluster({
        logger: this[PRIVATE.LOGGER],
        brokers,
        ssl,
        sasl,
        clientId,
        connectionTimeout,
        authenticationTimeout,
        metadataMaxAge,
        retry,
        allowAutoTopicCreation,
        allowExperimentalV011,
        maxInFlightRequests,
        isolationLevel,
        offsets: this[PRIVATE.OFFSETS],
      })
  }

  /**
   * @public
   */
  producer({
    createPartitioner,
    retry,
    metadataMaxAge,
    allowAutoTopicCreation,
    idempotent,
    transactionalId,
    transactionTimeout,
    maxInFlightRequests,
  } = {}) {
    const cluster = this[PRIVATE.CREATE_CLUSTER]({
      metadataMaxAge,
      allowAutoTopicCreation,
      maxInFlightRequests,
    })

    return createProducer({
      retry: { ...cluster.retry, ...retry },
      logger: this[PRIVATE.LOGGER],
      cluster,
      createPartitioner,
      idempotent,
      transactionalId,
      transactionTimeout,
    })
  }

  /**
   * @public
   */
  consumer({
    groupId,
    partitionAssigners,
    metadataMaxAge,
    sessionTimeout,
    heartbeatInterval,
    maxBytesPerPartition,
    minBytes,
    maxBytes,
    maxWaitTimeInMs,
    retry,
    allowAutoTopicCreation,
    maxInFlightRequests,
    readUncommitted = false,
  } = {}) {
    const isolationLevel = readUncommitted
      ? ISOLATION_LEVEL.READ_UNCOMMITTED
      : ISOLATION_LEVEL.READ_COMMITTED

    const cluster = this[PRIVATE.CREATE_CLUSTER]({
      metadataMaxAge,
      allowAutoTopicCreation,
      maxInFlightRequests,
      isolationLevel,
    })

    return createConsumer({
      retry: { ...cluster.retry, retry },
      logger: this[PRIVATE.LOGGER],
      cluster,
      groupId,
      partitionAssigners,
      sessionTimeout,
      heartbeatInterval,
      maxBytesPerPartition,
      minBytes,
      maxBytes,
      maxWaitTimeInMs,
      isolationLevel,
    })
  }

  /**
   * @public
   */
  admin({ retry } = {}) {
    const cluster = this[PRIVATE.CREATE_CLUSTER]({ allowAutoTopicCreation: false })
    return createAdmin({
      retry: { ...cluster.retry, retry },
      logger: this[PRIVATE.LOGGER],
      cluster,
    })
  }

  /**
   * @public
   */
  logger() {
    return this[PRIVATE.LOGGER]
  }
}
