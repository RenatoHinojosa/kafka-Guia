// Productor simulado (mock) de mspedidos360-orders. En local simulamos los eventos de pedidos con este script.
//   npm run producer                -> 10 eventos con 3 pedidoId distintos
//   npm run producer -- 15          -> 15 eventos (3 pedidos recorren los 5 estados completos)
//   npm run producer -- 12 1500     -> 12 eventos, uno cada 1,5 s (para verlos en vivo)
//   npm run producer -- --malo      -> JSON roto + evento sin pedidoId (terminan en la DLT)
//   npm run producer -- --falla     -> evento válido que report no puede procesar (reintentos + DLT)
//   npm run producer -- --total-malo -> PEDIDO_CREADO cuyo total no cuadra con los items (DLT)
//   npm run producer -- --salto     -> P-2001 pasa de CREADO a DESPACHADO (transicion invalida)
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

// Catalogo de prueba (parte del mock: en el proyecto final los productos vienen de catalog)
const CATALOGO = {
  'PROD-1': { nombre: 'Pizza familiar', precio: 8990 },
  'PROD-2': { nombre: 'Bebida 1,5 L',   precio: 1990 },
  'PROD-3': { nombre: 'Postre',         precio: 3490 },
};

const item = (productoId, cantidad) => ({ productoId, cantidad, precio: CATALOGO[productoId].precio });
const totalDe = (items) => items.reduce((acc, it) => acc + it.cantidad * it.precio, 0);

// Publica un evento. Por defecto valida antes de enviar; con validarAntes=false
// se envia igual, para que el error lo detecte el consumidor y vaya a la DLT.
async function publicar(datos, correlationId, { validarAntes = true } = {}) {
  const evento = crearEvento(datos);
  if (validarAntes) {
    const errores = validar(evento);
    if (errores.length) throw new Error(errores.join());
  }
  const [r] = await producer.send({
    topic: PEDIDOS,
    acks: -1,                // all
    messages: [{
      key: evento.pedidoId,   // EXPERIMENTO: revertir a evento.pedidoId al terminar  //eventId mismo pedido, misma particion
      value: JSON.stringify(evento),
      headers: { correlationId },
    }],
  });
  console.log(`-> p${r.partition} off${r.baseOffset}  ${evento.pedidoId}  ${evento.tipo}  (${evento.actor.usuario})`);
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

// El CLIENTE crea el pedido; el OPERADOR lo acepta, prepara, despacha y entrega.
const FLUJO = [
  { tipo: 'PEDIDO_CREADO',         rol: 'CLIENTE'  },
  { tipo: 'PEDIDO_ACEPTADO',       rol: 'OPERADOR' },
  { tipo: 'PEDIDO_EN_PREPARACION', rol: 'OPERADOR' },
  { tipo: 'PEDIDO_DESPACHADO',     rol: 'OPERADOR' },
  { tipo: 'PEDIDO_ENTREGADO',      rol: 'OPERADOR' },
];

const PEDIDOS_DEMO = [
  { pedidoId: 'P-1001', clienteId: 'C-1', items: [item('PROD-1', 2), item('PROD-2', 1)] },
  { pedidoId: 'P-1002', clienteId: 'C-2', items: [item('PROD-1', 1), item('PROD-3', 1)] },
  { pedidoId: 'P-1003', clienteId: 'C-3', items: [item('PROD-2', 2), item('PROD-3', 2)] },
];

const actorDe = (rol, clienteId) =>
  rol === 'CLIENTE'
    ? { usuario: `cli-${clienteId}`, rol }
    : { usuario: 'op-ana', rol };

// Arma los datos del evento para un pedido en un paso del flujo.
// total viaja en todos los eventos (los cancelaciones lo necesitan para restar ventas);
// items solo en PEDIDO_CREADO.
function datosDe(pedido, paso) {
  return {
    pedidoId: pedido.pedidoId,
    clienteId: pedido.clienteId,
    tipo: paso.tipo,
    total: totalDe(pedido.items),
    items: paso.tipo === 'PEDIDO_CREADO' ? pedido.items : undefined,
    actor: actorDe(paso.rol, pedido.clienteId),
  };
}

async function main() {
  const arg = process.argv[2];
  await producer.connect();

  if (arg === '--malo') {
    await publicarCrudo('P-ROTO', '{ esto no es JSON');
    await publicarCrudo('SIN-ID', JSON.stringify(crearEvento({
      tipo: 'PEDIDO_CREADO', clienteId: 'C-9', total: 1000,
      actor: { usuario: 'cli-C-9', rol: 'CLIENTE' },
    })));
  } else if (arg === '--falla') {
    const items = [item('PROD-2', 1)];
    await publicar({
      tipo: 'PEDIDO_CREADO', pedidoId: 'P-9999', clienteId: 'C-FALLA',
      total: totalDe(items), items, actor: actorDe('CLIENTE', 'C-FALLA'),
    }, crypto.randomUUID());
  } else if (arg === '--total-malo') {
    // El total no cuadra con los items: se envia sin validar para que lo rechacen los consumidores
    const items = [item('PROD-1', 1), item('PROD-2', 1)];
    await publicar({
      tipo: 'PEDIDO_CREADO', pedidoId: 'P-3001', clienteId: 'C-4',
      total: totalDe(items) + 1000, items, actor: actorDe('CLIENTE', 'C-4'),
    }, crypto.randomUUID(), { validarAntes: false });
    
    } else if (arg === '--salto') {
    // P-2001 pasa de CREADO directo a DESPACHADO (se salta ACEPTADO y EN_PREPARACION)
    const items = [item('PROD-1', 1)];
    const base = { pedidoId: 'P-2001', clienteId: 'C-5', total: totalDe(items) };
    await publicar({ ...base, tipo: 'PEDIDO_CREADO', items, actor: actorDe('CLIENTE', 'C-5') }, crypto.randomUUID());
    await publicar({ ...base, tipo: 'PEDIDO_DESPACHADO', actor: actorDe('OPERADOR') }, crypto.randomUUID());
  } else {
    // Cada pedido avanza por su flujo: P-1001 CREADO, P-1002 CREADO, P-1003 CREADO, P-1001 ACEPTADO, ...
    const n = Number(arg) || 10;
    const pausa = Number(process.argv[3]) || 0;   // ms entre eventos
    for (let i = 0; i < n; i++) {
      const pedido = PEDIDOS_DEMO[i % PEDIDOS_DEMO.length];
      const paso = FLUJO[Math.floor(i / PEDIDOS_DEMO.length) % FLUJO.length];
      await publicar(datosDe(pedido, paso), crypto.randomUUID());
      if (pausa) await new Promise(r => setTimeout(r, pausa));
    }
  }

  await producer.disconnect();
}

main().catch(e => { console.error(e.message); process.exit(1); });