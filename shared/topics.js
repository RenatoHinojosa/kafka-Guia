// Nombres y configuración de los tópicos (cada número tiene una razón)
const PEDIDOS = 'pedidos.eventos';
const DLT = 'pedidos.eventos.dlt';

const TOPICOS = [
  // 3 carriles para escalar; la key (pedidoId) mantiene el orden de cada pedido
  { topic: PEDIDOS, particiones: 3, retencionDias: 7 },
  // Poco volumen; se conserva más tiempo para investigar los fallos
  { topic: DLT, particiones: 1, retencionDias: 30 },
];

module.exports = { PEDIDOS, DLT, TOPICOS };
