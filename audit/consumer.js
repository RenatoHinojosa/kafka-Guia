// Servicio audit (groupId = audit)
// Recibe los MISMOS eventos que report, con su propio offset.
// Guarda cada evento en una "tabla" (en memoria) para armar el timeline de cada pedido.
const { iniciarConsumidor } = require('../shared/consumidor');

const tabla = [];              // filas de auditoría (solo lectura / append)
const vistos = new Set();      // eventId ya guardados -> idempotencia

async function procesar(evento) {
  if (vistos.has(evento.eventId)) return;   // duplicado: se ignora
  vistos.add(evento.eventId);
  tabla.push(evento);

  const timeline = tabla
    .filter(e => e.pedidoId === evento.pedidoId)
    .map(e => `${e.fecha.slice(11, 19)} ${e.tipo}`);
  console.log(`   timeline ${evento.pedidoId}: ${timeline.join(' -> ')}`);
}

iniciarConsumidor({ clientId: 'audit-service', groupId: 'audit', procesar })
  .catch(e => { console.error(e); process.exit(1); });
