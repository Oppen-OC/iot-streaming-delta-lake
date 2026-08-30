"""Genera telemetria de vehiculos y la escribe como ficheros JSON.

Simula una flota de coches electricos repartida por 5 ciudades espanolas sobre
datos reales del Vehicle Energy Dataset (velocidad y bateria medidas por OBD-II a
1 Hz), y mete anomalias a proposito para que el pipeline tenga algo que limpiar.

Se lanza en una terminal aparte mientras el notebook lee lo que va escribiendo:

    python generador.py --duracion 60
"""

import argparse
import json
import math
import random
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd

AQUI = Path(__file__).parent
SEMILLA = AQUI / "datos" / "semilla_ved.parquet"
SALIDA = AQUI / "lake" / "landing"

# Los datos originales se grabaron aqui.
ANN_ARBOR = (42.2808, -83.7430)

CIUDADES = {
    "Madrid":    (40.4168, -3.7038),
    "Barcelona": (41.3851,  2.1734),
    "Valencia":  (39.4699, -0.3763),
    "Sevilla":   (37.3891, -5.9845),
    "Bilbao":    (43.2630, -2.9350),
}

# A escala 1:1 un solo evento del final de un viaje de 96 minutos empujaria el
# watermark casi una hora y el resto se tiraria por atrasado. A 60x, el viaje mas
# largo ocupa 1,6 minutos y cabe dentro de los 10 minutos de watermark.
ACELERACION = 60

# Cada registro recibe como mucho un fallo, para poder comparar uno a uno lo que se
# rompio contra lo que el pipeline detecto.
ANOMALIAS = {
    "VELOCIDAD_ABSURDA":      0.020,   # 300-900 km/h
    "COORDENADA_IMPOSIBLE":   0.015,   # latitud 999
    "BATERIA_FUERA_DE_RANGO": 0.015,   # bateria negativa o mayor de 100
    "CAMPO_VACIO":            0.015,   # falta el vehiculo, la ciudad o la fecha
    "FUERA_DE_CIUDAD":        0.010,   # coordenada valida pero de otra ciudad
    "CIUDAD_DESCONOCIDA":     0.010,   # una ciudad que no esta en la lista
    "DUPLICADO":              0.015,   # el mismo evento repetido
    "EVENTO_TARDIO":          0.015,   # llega con 15-40 minutos de retraso
}


def preparar(inicio):
    """Carga la semilla y la convierte en telemetria de una flota multi-ciudad."""
    df = pd.read_parquet(SEMILLA)

    # Solo 27 coches de VED miden la bateria, asi que saldrian 5 por ciudad. Se usa
    # el viaje como unidad de flota: entre viajes el coche se recarga, o sea que un
    # viaje ya es independiente del anterior.
    df["flota_id"] = df.groupby(["veh_id", "trip"]).ngroup()
    df["city"] = [list(CIUDADES)[i % len(CIUDADES)] for i in df["flota_id"]]

    # Las marcas de tiempo son de 2017. Se recolocan en el presente (y aceleradas)
    # para que Spark las trate como eventos que acaban de ocurrir.
    t0 = df.groupby("flota_id")["ts_ms"].transform("min")
    df["event_ts"] = inicio + pd.to_timedelta((df["ts_ms"] - t0) / ACELERACION, unit="ms")

    # Traslacion de Ann Arbor a cada ciudad. Un grado de longitud mide distinto
    # segun la latitud (82 km en Ann Arbor, 88 en Sevilla), asi que la diferencia
    # en longitud se reescala; si no, las distancias saldrian mal.
    lat0, lon0 = ANN_ARBOR
    lat_ciudad = np.array([CIUDADES[c][0] for c in df["city"]])
    lon_ciudad = np.array([CIUDADES[c][1] for c in df["city"]])
    escala = math.cos(math.radians(lat0)) / np.cos(np.radians(lat_ciudad))
    df["lat"] = df["lat"] - lat0 + lat_ciudad
    df["lon"] = (df["lon"] - lon0) * escala + lon_ciudad

    # Barajadas para que cada fichero toque varias ciudades y las particiones se
    # llenen en paralelo. Cada registro lleva su propia marca de tiempo.
    return df.sample(frac=1, random_state=0).reset_index(drop=True)


def registro(fila):
    return {
        "event_id":    str(uuid.uuid4()),
        "vehicle_id":  f"VEH-{fila.flota_id:05d}",
        "city":        fila.city,
        "event_ts":    fila.event_ts,
        "lat":         round(float(fila.lat), 6),
        "lon":         round(float(fila.lon), 6),
        "speed_kmh":   float(fila.speed_kmh),
        "battery_pct": float(fila.battery_pct),
        "trip_id":     f"{fila.veh_id}-{fila.trip}",
    }


def romper(reg, rnd):
    """Estropea el registro, o no. Devuelve el nombre del fallo aplicado o None."""
    tirada, acumulado = rnd.random(), 0.0
    for fallo, probabilidad in ANOMALIAS.items():
        acumulado += probabilidad
        if tirada >= acumulado:
            continue

        if fallo == "VELOCIDAD_ABSURDA":
            reg["speed_kmh"] = round(rnd.uniform(300, 900), 1)
        elif fallo == "COORDENADA_IMPOSIBLE":
            reg["lat"] = 999.0
        elif fallo == "BATERIA_FUERA_DE_RANGO":
            reg["battery_pct"] = rnd.choice([-5.0, 150.0])
        elif fallo == "CAMPO_VACIO":
            reg[rnd.choice(["vehicle_id", "city", "event_ts"])] = None
        elif fallo == "FUERA_DE_CIUDAD":
            # Coordenada perfectamente valida, pero de otra ciudad: el coche dice
            # estar en Madrid y aparece en Bilbao.
            otra = rnd.choice([c for c in CIUDADES if c != reg["city"]])
            reg["lat"], reg["lon"] = CIUDADES[otra]
        elif fallo == "CIUDAD_DESCONOCIDA":
            reg["city"] = rnd.choice(["Lisboa", "Oporto", "Toulouse"])
        elif fallo == "EVENTO_TARDIO":
            reg["event_ts"] = reg["event_ts"] - timedelta(minutes=rnd.randint(15, 40))
        # DUPLICADO no toca el registro: lo repite quien llama.
        return fallo
    return None


def escribir(registros, n):
    """Guarda un lote como JSON, una linea por evento."""
    lineas = []
    for reg in registros:
        reg = dict(reg)
        if reg["event_ts"] is not None:
            reg["event_ts"] = reg["event_ts"].isoformat().replace("+00:00", "Z")
        lineas.append(json.dumps(reg))

    # Se renombra al final: Spark lista el directorio en cada vuelta y un fichero a
    # medio escribir lo leeria truncado.
    tmp = SALIDA / f"lote-{n:04d}.escribiendo"
    tmp.write_text("\n".join(lineas), encoding="utf-8")
    tmp.rename(SALIDA / f"lote-{n:04d}.json")


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--duracion", type=float, default=60, help="segundos generando")
    p.add_argument("--intervalo", type=float, default=2, help="segundos entre ficheros")
    p.add_argument("--tamano", type=int, default=500, help="eventos por fichero")
    p.add_argument("--semilla-azar", type=int, default=42)
    args = p.parse_args()

    SALIDA.mkdir(parents=True, exist_ok=True)
    rnd = random.Random(args.semilla_azar)

    inicio = pd.Timestamp(datetime.now(timezone.utc)).floor("s")
    print(f"cargando {SEMILLA.name}...")
    df = preparar(inicio)
    print(f"{len(df):,} eventos disponibles de {df.flota_id.nunique():,} vehiculos")

    manifiesto = (SALIDA / "_manifiesto.jsonl").open("w", encoding="utf-8")
    fin = time.time() + args.duracion
    n, pos, total = 0, 0, 0

    while time.time() < fin and pos < len(df):
        lote, cuenta = [], {}
        for fila in df.iloc[pos:pos + args.tamano].itertuples():
            reg = registro(fila)
            fallo = romper(reg, rnd)
            if fallo:
                cuenta[fallo] = cuenta.get(fallo, 0) + 1
            lote.append(reg)
            if fallo == "DUPLICADO":
                lote.append(dict(reg))
        pos += args.tamano

        escribir(lote, n)
        # Contra esto comprueba despues el notebook que la limpieza hizo su trabajo.
        manifiesto.write(json.dumps({"fichero": f"lote-{n:04d}.json",
                                     "filas": len(lote), "anomalias": cuenta}) + "\n")
        manifiesto.flush()

        n, total = n + 1, total + len(lote)
        print(f"  lote {n:>3}  {len(lote):>4} eventos  (total {total:,})")
        time.sleep(args.intervalo)

    manifiesto.close()
    print(f"\n{n} ficheros y {total:,} eventos en {SALIDA}")


if __name__ == "__main__":
    main()
