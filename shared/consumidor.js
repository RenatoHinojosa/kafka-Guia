// Infraestructura del consumidor: parseo, validación, reintentos y DLT.
// Cada servicio (report, audit) solo aporta su función procesar(evento):
// la lógica de negocio queda separada de la mensajería.
const { Partitioners } = require('kafkajs');
const { crearKafka } = require('./kafka');
const { validar } = require('./dto');
const { PEDIDOS, DLT } = require('./topics');

async function iniciarConsumidor({ clientId, groupId, procesar }) {
  const kafka = crearKafka(clientId);
  const consumer = kafka.consumer({ groupId });
  const dlt = kafka.producer({ createPartitioner: Partitioners.DefaultPartitioner });

  // Publica el mensaje original + el motivo (y correlationId) en pedidos.eventos.dlt
  async function enviarADlt(message, partition, motivo) {
    const texto = Array.isArray(motivo) ? motivo.join(', ') : motivo;
    await dlt.send({
      topic: DLT,
      messages: [{
        key: message.key,
        value: message.value,
        headers: {
          ...message.headers,
          motivo: texto,
          topicOrigen: PEDIDOS,
          particionOrigen: String(partition),
          offsetOrigen: message.offset,
        },
      }],
    });
    console.error(`[${groupId}] -> DLT  p${partition} off${message.offset}  motivo: ${texto}`);
  }

  // Muestra qué particiones le tocaron a este consumidor (útil para ver el rebalanceo)
  consumer.on(consumer.events.GROUP_JOIN, ({ payload }) => {
    const asignadas = payload.memberAssignment[PEDIDOS] || [];
    console.log(`[${groupId}] particiones asignadas: [${asignadas.join(', ')}]`);
  });

  await consumer.connect(); await dlt.connect();
  await consumer.subscribe({ topic: PEDIDOS,
                             fromBeginning: true });

  await consumer.run({ eachMessage: async ({ message,
                                    partition }) => {
    let evento;
    try { evento = JSON.parse(message.value.toString()); }
    catch (e) { return enviarADlt(message, partition, 'JSON invalido'); }

    const errores = validar(evento);
    if (errores.length) return enviarADlt(message, partition, errores);

    console.log(`[${groupId}] p${partition} off${message.offset}  ${evento.pedidoId}  ${evento.tipo}`);

    for (let i = 1; i <= 3; i++) {          // reintentos
      try { await procesar(evento); return; }  // negocio
        catch (e) {
        if (e.reintentable === false) return enviarADlt(message, partition, e.message);  // directo a la DLT
        console.warn(`[${groupId}] intento ${i} ${evento.eventId} ${e.message}`);
        await new Promise(r => setTimeout(r, 1000 * 2 ** (i - 1)));
      }
    }
    return enviarADlt(message, partition, 'agotados los reintentos');
  }});
  // Al terminar eachMessage sin error, KafkaJS confirma (commit) el offset.

  // Ctrl+C: salir del grupo de forma ordenada (provoca rebalanceo inmediato)
  for (const sig of ['SIGINT', 'SIGTERM']) {
    process.on(sig, async () => {
      await consumer.disconnect(); await dlt.disconnect();
      process.exit(0);
    });
  }
}

module.exports = { iniciarConsumidor };
