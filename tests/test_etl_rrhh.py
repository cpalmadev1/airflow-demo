import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'dags'))

from etl_rrhh import extraer, validar, cargar

def test_extraer_retorna_tres_registros():
    resultado = extraer()
    assert len(resultado) == 3

def test_extraer_contiene_campo_nombre():
    resultado = extraer()
    assert 'nombre' in resultado[0]

def test_extraer_tiene_registro_invalido():
    resultado = extraer()
    invalidos = [r for r in resultado if r['sueldo'] is None]
    assert len(invalidos) == 1