const crypto = require('crypto');

const TIPOS = [
  'PEDIDO_CREADO',
  'PEDIDO_ACEPTADO',
  'PEDIDO_EN_PREPARACION',   // nuevo en v2
  'PEDIDO_DESPACHADO',
  'PEDIDO_ENTREGADO',
  'PEDIDO_CANCELADO',
];

const ROLES = ['CLIENTE', 'OPERADOR', 'ADMIN'];

// Contrato v2: agrega version, items (solo en PEDIDO_CREADO), actor y origen.
function crearEvento(d) {
  return {
    version: 2,
    eventId: d.eventId || crypto.randomUUID(),
    tipo: d.tipo,
    pedidoId: d.pedidoId,
    clienteId: d.clienteId,
    total: d.total,
    items: d.items,                 // undefined se omite en JSON.stringify
    actor: d.actor,                 // { usuario, rol }
    origen: d.origen || 'web',
    fecha: d.fecha || new Date().toISOString(),
  };
}

// En PEDIDO_CREADO: items no vacio, cantidad > 0 y total = suma(cantidad x precio)
function validarItems(e) {
  if (!Array.isArray(e.items) || e.items.length === 0) {
    return ['items no puede venir vacio'];
  }
  const err = [];
  for (const it of e.items) {
    if (!it || !it.productoId) err.push('item sin productoId');
    else if (!(it.cantidad > 0)) err.push(`cantidad invalida en ${it.productoId}`);
    else if (typeof it.precio !== 'number' || it.precio < 0) err.push(`precio invalido en ${it.productoId}`);
  }
  if (err.length) return err;

  const suma = e.items.reduce((acc, it) => acc + it.cantidad * it.precio, 0);
  if (typeof e.total === 'number' && e.total !== suma) {
    err.push(`total no cuadra: total=${e.total}, suma de items=${suma}`);
  }
  return err;
}

function validar(e) {
  const err = [];
  if (e.version !== 2) err.push('version debe ser 2');
  if (!e.eventId) err.push('eventId requerido');
  if (!TIPOS.includes(e.tipo)) err.push('tipo invalido');
  if (!e.pedidoId) err.push('pedidoId requerido');
  if (typeof e.total !== 'number') err.push('total debe ser numero');

  if (!e.actor || !e.actor.usuario) err.push('actor.usuario requerido');
  else if (!ROLES.includes(e.actor.rol)) err.push('actor.rol invalido');
  if (!e.origen) err.push('origen requerido');

  if (e.tipo === 'PEDIDO_CREADO') err.push(...validarItems(e));
  return err;   // [] = valido
}

module.exports = { TIPOS, ROLES, crearEvento, validar };