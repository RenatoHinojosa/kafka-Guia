const TRANSICIONES = {
  NINGUNO:               ['PEDIDO_CREADO'],
  PEDIDO_CREADO:         ['PEDIDO_ACEPTADO', 'PEDIDO_CANCELADO'],
  PEDIDO_ACEPTADO:       ['PEDIDO_EN_PREPARACION', 'PEDIDO_CANCELADO'],
  PEDIDO_EN_PREPARACION: ['PEDIDO_DESPACHADO', 'PEDIDO_CANCELADO'],
  PEDIDO_DESPACHADO:     ['PEDIDO_ENTREGADO', 'PEDIDO_CANCELADO'],
  PEDIDO_ENTREGADO:      [],   // estado final
  PEDIDO_CANCELADO:      [],   // estado final
};

// anterior = último tipo visto del pedido (undefined/null si es el primero)
function transicionValida(anterior, nuevo) {
  const permitidos = TRANSICIONES[anterior || 'NINGUNO'] || [];
  return permitidos.includes(nuevo);
}

module.exports = { TRANSICIONES, transicionValida };