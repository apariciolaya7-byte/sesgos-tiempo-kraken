import os
import tempfile
import json
import time
import pandas as pd
import sys
import pathlib
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
import kraken_data as kd


def test_save_and_load_positions(tmp_path):
    # usar archivo temporal
    tmp_file = tmp_path / "open_positions_test.json"
    kd.POSITIONS_FILE = str(tmp_file)

    # preparar posiciones
    kd.OPEN_POSITIONS.clear()
    pos = kd.Position(
        symbol='BTC/USD',
        direction='LONG (COMPRA)',
        entry_price=100.0,
        amount_base=0.001,
        stop_loss=90.0,
        take_profit=120.0,
        status='OPEN',
        open_time=pd.Timestamp('2025-12-16T12:00:00Z')
    )
    kd.OPEN_POSITIONS.append(pos)

    kd.save_open_positions()
    assert tmp_file.exists()

    # limpiar y recargar
    kd.OPEN_POSITIONS.clear()
    kd.load_open_positions()

    assert len(kd.OPEN_POSITIONS) == 1
    loaded = kd.OPEN_POSITIONS[0]
    assert loaded['symbol'] == 'BTC/USD'
    assert 'open_time' in loaded
    # open_time debe ser string iso
    assert isinstance(loaded['open_time'], str)
 