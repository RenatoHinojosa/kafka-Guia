// Puente SQS -> Kafka. En local simulamos los eventos de pedidos con este script.
//   npm run producer             -> 10 eventos con 3 pedidoId distintos
//   npm run producer -- 20       -> 20 eventos
//   npm run producer -- 12 1500  -> 12 eventos, uno cada 1,5 s (para verlos en vivo)
//   npm run producer -- --malo   -> JSON roto + evento sin pedidoId (terminan en la DLT)
//   npm run producer -- --falla  -> evento válido que report no puede procesar (reintentos + DLT)
const crypto = require('crypto');
const { Partitioners } = require('kafkajs');
const { crearKafka } = require('../shared/kafka');
const { crearEvento, validar } = require('../shared/dto');
const { PEDIDOS } = require('../shared/topics');

const producer = crearKafka('bridge').producer({
  idempotent: true,        // sin duplicados por red
  maxInFlightRequests: 1,
  createPartitioner: Partitioners.DefaultPartitioner,
});

async function publicar(datos, correlationId) {
  const evento = crearEvento(datos);
  const errores = validar(evento);
  if (errores.length) throw new Error(errores.join());
  const [r] = await producer.send({
    topic: PEDIDOS,
    acks: -1,                // all
    messages: [{
      key: evento.pedidoId,  // mismo pedido, misma particion
      value: JSON.stringify(evento),
      headers: { correlationId },
    }],
  });
  console.log(`-> p${r.partition} off${r.baseOffset}  ${evento.pedidoId}  ${evento.tipo}`);
}

// Envía tal cual, sin validar: solo para provocar errores en los consumidores
async function publicarCrudo(key, value) {
  await producer.send({
    topic: PEDIDOS,
    acks: -1,
    messages: [{ key, value, headers: { correlationId: crypto.randomUUID() } }],
  });
  console.log(`-> (crudo) ${value}`);
}

const FLUJO = ['PEDIDO_CREADO', 'PEDIDO_ACEPTADO', 'PEDIDO_DESPACHADO', 'PEDIDO_ENTREGADO'];
const PEDIDOS_DEMO = [
  { pedidoId: 'P-1001', clienteId: 'C-1', total: 15990 },
  { pedidoId: 'P-1002', clienteId: 'C-2', total: 8500 },
  { pedidoId: 'P-1003', clienteId: 'C-3', total: 23400 },
];

async function main() {
  const arg = process.argv[2];
  await producer.connect();

  if (arg === '--malo') {
    await publicarCrudo('P-ROTO', '{ esto no es JSON');
    await publicarCrudo('SIN-ID', JSON.stringify(crearEvento({ tipo: 'PEDIDO_CREADO', clienteId: 'C-9', total: 1000 })));
  } else if (arg === '--falla') {
    await publicar({ tipo: 'PEDIDO_CREADO', pedidoId: 'P-9999', clienteId: 'C-FALLA', total: 1000 }, crypto.randomUUID());
  } else {
    // Cada pedido avanza por su flujo: P-1001 CREADO, P-1002 CREADO, P-1003 CREADO, P-1001 ACEPTADO, ...
    const n = Number(arg) || 10;
    const pausa = Number(process.argv[3]) || 0;   // ms entre eventos
    for (let i = 0; i < n; i++) {
      const pedido = PEDIDOS_DEMO[i % PEDIDOS_DEMO.length];
      const tipo = FLUJO[Math.floor(i / PEDIDOS_DEMO.length) % FLUJO.length];
      await publicar({ ...pedido, tipo }, crypto.randomUUID());
      if (pausa) await new Promise(r => setTimeout(r, pausa));
    }
  }

  await producer.disconnect();
}

main().catch(e => { console.error(e.message); process.exit(1); });
