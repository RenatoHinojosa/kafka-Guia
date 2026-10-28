// Traduce { topic, particiones, retencionDias } al formato de admin.createTopics
function definicionTopico({ topic, particiones, retencionDias }) {
  return {
    topic,
    numPartitions: particiones,
    replicationFactor: 1,          // un solo broker en local
    configEntries: [
      { name: 'cleanup.policy', value: 'delete' },
      { name: 'retention.ms',
        value: String(retencionDias * 86400000) },
    ],
  };
}

module.exports = { definicionTopico };
