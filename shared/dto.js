const crypto = require('crypto');

const TIPOS = ['PEDIDO_CREADO', 'PEDIDO_ACEPTADO',
  'PEDIDO_DESPACHADO', 'PEDIDO_ENTREGADO',
  'PEDIDO_CANCELADO'];

function crearEvento(d) {
  return {
    eventId: d.eventId || crypto.randomUUID(),
    tipo: d.tipo,
    pedidoId: d.pedidoId,
    clienteId: d.clienteId,
    total: d.total,
    fecha: d.fecha || new Date().toISOString(),
  };
}

function validar(e) {
  const err = [];
  if (!e.eventId) err.push('eventId requerido');
  if (!TIPOS.includes(e.tipo)) err.push('tipo invalido');
  if (!e.pedidoId) err.push('pedidoId requerido');
  if (typeof e.total !== 'number') err.push('total');
  return err;   // [] = valido
}

module.exports = { TIPOS, crearEvento, validar };
