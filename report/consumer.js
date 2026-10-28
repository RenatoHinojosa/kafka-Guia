// Servicio report (groupId = report)
// Calcula ventas por hora, lead time (creado -> entregado) y pedidos activos por estado.
const { iniciarConsumidor } = require('../shared/consumidor');

const ventasPorHora = {};   // '2026-10-27T16' -> total vendido
const creadoEn = {};        // pedidoId -> fecha PEDIDO_CREADO
const estadoActual = {};    // pedidoId -> último tipo

async function procesar(evento) {
  // Falla simulada para mostrar reintentos + DLT (npm run producer -- --falla)
  if (evento.clienteId === 'C-FALLA') throw new Error('error simulado en report');

  estadoActual[evento.pedidoId] = evento.tipo;

  if (evento.tipo === 'PEDIDO_CREADO') {
    const hora = evento.fecha.slice(0, 13);
    ventasPorHora[hora] = (ventasPorHora[hora] || 0) + evento.total;
    creadoEn[evento.pedidoId] = evento.fecha;
  }

  if (evento.tipo === 'PEDIDO_ENTREGADO' && creadoEn[evento.pedidoId]) {
    const ms = new Date(evento.fecha) - new Date(creadoEn[evento.pedidoId]);
    console.log(`   lead time ${evento.pedidoId}: ${(ms / 1000).toFixed(1)} s`);
  }

  const activos = {};
  for (const tipo of Object.values(estadoActual)) activos[tipo] = (activos[tipo] || 0) + 1;
  console.log('   ventas por hora:', ventasPorHora, ' | pedidos por estado:', activos);
}

iniciarConsumidor({ clientId: 'report-service', groupId: 'report', procesar })
  .catch(e => { console.error(e); process.exit(1); });
