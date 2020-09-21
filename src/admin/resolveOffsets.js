const { assign } = Object
const initializeConsumerOffsets = require('../consumer/offsetManager/initializeConsumerOffsets')
const isInvalidOffset = require('../consumer/offsetManager/isInvalidOffset')
const { EARLIEST_OFFSET } = require('../constants')

const indexPartitions = (obj, { partition, offset }) => {
  return assign(obj, { [partition]: offset })
}

module.exports = async ({ cluster, groupId, consumerOffsets }) => {
  const unresolvedPartitions = consumerOffsets.map(({ topic, partitions }) => ({
    topic,
    partitions: partitions
      .filter(({ offset }) => isInvalidOffset(offset))
      .map(({ partition }) => ({ partition })),
    fromBeginning: partitions.some(({ offset }) => offset === EARLIEST_OFFSET),
  }))

  const hasUnresolvedPartitions = () =>
    unresolvedPartitions.filter(t => t.partitions.length > 0).length > 0

  if (hasUnresolvedPartitions()) {
    console.log('ARTY')
    const topicOffsets = await cluster.fetchTopicsOffset(unresolvedPartitions)
    console.log(JSON.stringify(topicOffsets, null, 4))
    consumerOffsets = initializeConsumerOffsets(consumerOffsets, topicOffsets)
  }

  consumerOffsets.forEach(({ topic, partitions }) => {
    cluster.committedOffsets({ groupId })[topic] = partitions.reduce(indexPartitions, {
      ...cluster.committedOffsets({ groupId })[topic],
    })
  })

  return consumerOffsets
}
