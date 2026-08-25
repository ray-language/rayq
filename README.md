# rayq

Broker de colas de trabajos **persistente** con entrega *at-least-once*, escrito en [raylang](https://github.com/ray-language/raylang): productores y consumidores hablan `packages/rpc` (estreno del paquete), cada cola persiste en un log append-only (WAL) que sobrevive a un `kill -9`, los mensajes sin ack se re-entregan por *visibility timeout*, los `nack` reintentan con backoff exponencial y, agotados los intentos, caen a la dead-letter queue `<cola>.dlq`.

```text
$ rayq serve --dir ./rayq-data &
$ rayq push jobs '{"task":"resize","img":"a.png"}'
01a03563-a59d-7962-a0f7-…

$ rayq pull jobs
01a03563-a59d-7962-a0f7-…	1	{"task":"resize","img":"a.png"}
$ rayq ack jobs 01a03563-a59d-7962-a0f7-…

# Un consumidor de verdad: un comando por mensaje (cuerpo por stdin, exit 0 = ack)
$ rayq worker jobs -- ./process-image.sh

$ rayq stats jobs
{ "acked": 1, "dead": 0, "delayed": 0, "inflight": 0, "pushed": 1, "ready": 0 }
```

## Semántica

- **At-least-once**: un `pull` arranca el reloj de visibilidad (default 30 s);
  sin `ack` a tiempo, el mensaje vuelve a `ready` con `attempt + 1`. Un
  consumidor debe tolerar duplicados.
- **`nack`**: reintento con backoff exponencial (1 s, 2 s, … tope 60 s).
  Agotados `max_attempts` (default 5), el mensaje pasa a `<cola>.dlq` (ack en
  la cola origen + push en la DLQ, ambas en sus WAL).
- **FIFO** por cola (ids `uuid_v7`, ordenables por tiempo — el orden sobrevive
  al replay).
- Cuerpos: texto (JSON típicamente). Binario: codifícalo tú (base64).

## Persistencia (el territorio que esta app vino a pisar)

Cada cola es `<dir>/<cola>.log`: un registro JSON por línea
(`push`/`ack`/`nack`), **write-ahead** (primero el disco, luego la memoria).
La recuperación es el replay: una línea rota (crash a mitad de append) se
descarta y se reporta — probado con `kill -9` a mitad de trabajo: los mensajes
in-flight vuelven a `ready`, los ackeados no reaparecen. La **compactación**
(al arrancar y cada 1000 acks) reescribe el log solo con lo pendiente y lo
renombra encima (`fs.rename` atómico), reabriendo el handle (el viejo apunta
al inode antiguo).

**Durabilidad y exclusión (resueltas en raylang M115)**: cada append pasa por
`fs.sync` (durable ante corte de luz, no solo ante crash del proceso), y el
broker toma un `flock` consultivo sobre `<dir>/LOCK` al arrancar — un segundo
broker sobre el mismo dir falla con un error claro en vez de corromper.

## Uso

```text
rayq serve [--dir DIR] [--bind H] [--port N] [--visibility MS] [--max-attempts N] [--drain MS]
rayq push QUEUE BODY | pull QUEUE [--wait MS] [--ack] | ack/nack QUEUE ID
rayq stats [QUEUE]
rayq worker QUEUE -- CMD [ARGS...]
```

`pull --ack` confirma al recibir (at-most-once: si tu proceso muere después,
el mensaje se perdió — para trabajo real usa `worker` o ack manual). El puerto
default es 7450; RPC = frames con prefijo de longitud + JSON (`packages/rpc`).

## Rendimiento (sanity check)

Cliente único secuencial, misma máquina, broker nativo: **~6.8k push/s** y
~3.6k pull+ack/s (2 RPC por mensaje → ~7.3k RPC/s); broker en VM: ~4.2k / ~2.5k.
Cada push es un append a disco con `fs.sync` (durable de verdad).

## Estado actual

| Capacidad | Estado |
|-----------|--------|
| push/pull/ack/nack/stats por RPC + CLI | ✅ |
| WAL por cola + replay con tolerancia a línea rota (kill -9 probado) | ✅ |
| Visibility timeout + redelivery con contador de intentos | ✅ |
| Backoff exponencial en nack + dead-letter queue | ✅ |
| Compactación (arranque + cada 1000 acks) con rename atómico | ✅ |
| `worker`: un comando del SO por mensaje (stdin, exit 0 = ack) | ✅ |
| Apagado graceful (SIGTERM/SIGINT vía rpc.serve_graceful) | ✅ |
| Binario nativo (broker y cliente; E2E verificado) | ✅ |
| Tests (WAL, máquina de estados con tiempo manual, E2E con reinicio) | ✅ 10 |
| fsync / durabilidad ante corte de luz | ✅ (raylang M115.1: `fs.sync` por append) |
| File locks (dos brokers, mismo dir) | ✅ (raylang M115.2: flock sobre `LOCK`) |
| Long-poll servidor (`pull --wait` sondea del lado cliente) | 📋 v2 |

## Hallazgos de dogfood (necesidades confirmadas del lenguaje)

Anotados en `raylang/IDEAS.md` §66:

1. **[RESUELTO — raylang M115.1]** `std/fs` no tiene `fsync`/flush: `fs.sync`
   existe y el WAL lo llama por append.
2. **[RESUELTO — raylang M115.2]** No hay file locks: `fs.try_lock` existe y
   el broker candea `<dir>/LOCK` (con `broker.stop` para soltarlo ordenado).
3. `fs.rename` SÍ existe y funciona como reemplazo atómico (la compactación
   entera se apoya en él); `truncate` no hizo falta gracias a ese patrón.
4. **Positivo — `packages/rpc` funcionó a la primera** en su estreno:
   serve_graceful, correlación de ids, errores como valores de punta a punta,
   una fibra por conexión. Y su nota "Solo VM" también está desactualizada:
   el broker nativo sirve RPC sin problema (igual que el webserver en raygate).
5. Patrón bonito: ids `uuid_v7` + Map de claves ordenadas = el replay
   reconstruye la cola en orden FIFO gratis.

## Desarrollo

```sh
ray test                      # 10 tests
ray run src/main.ray serve --dir /tmp/rayq-dev
ray build --native src/main.ray -o rayq --release
ray run debug/bench.ray       # throughput contra un broker en 18971
```

Estructura: `src/main.ray` (CLI) · `wal.ray` (log append-only + compact) ·
`queue.ray` (máquina de estados, tiempo explícito) · `broker.ray` (actor dueño
de colas y WALs) · `server.ray` (RPC) · `client.ray` (verbos + worker).
