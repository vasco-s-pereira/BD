#!/usr/bin/python3
import os
from logging.config import dictConfig

from flask import Flask, jsonify, request
import psycopg
from psycopg.rows import namedtuple_row
from datetime import datetime

dictConfig(
    {
        "version": 1,
        "formatters": {
            "default": {
                "format": "[%(asctime)s] %(levelname)s in %(module)s:%(lineno)s - %(funcName)20s(): %(message)s",
            }
        },
        "handlers": {
            "wsgi": {
                "class": "logging.StreamHandler",
                "stream": "ext://flask.logging.wsgi_errors_stream",
                "formatter": "default",
            }
        },
        "root": {"level": "INFO", "handlers": ["wsgi"]},
    }
)

DATABASE_URL = os.environ.get("DATABASE_URL", "postgres://saude:saude@postgres/saude")

app = Flask(__name__)
app.config.from_prefixed_env()
log = app.logger


@app.route("/", methods=("GET",))
def list_clinicas():
    with psycopg.connect(conninfo=DATABASE_URL) as conn:
        with conn.cursor(row_factory=namedtuple_row) as cur:
            clinicas = cur.execute(
                """
                SELECT nome, morada
                FROM clinica
                ORDER BY nome DESC;
                """,
                {},
            ).fetchall()
          
    return jsonify(clinicas)

@app.route("/c/<clinica>/", methods=("GET",))
def list_especialidades(clinica):
    with psycopg.connect(conninfo=DATABASE_URL) as conn:
        with conn.cursor(row_factory=namedtuple_row) as cur:
            with conn.cursor(row_factory=namedtuple_row) as cur:
                especialidades = cur.execute(
                    """
                    SELECT DISTINCT m.especialidade
                    FROM medico m
                    JOIN trabalha t ON t.nif = m.nif
                    JOIN clinica c ON c.nome = t.nome
                    WHERE c.nome = %s;
                    """,
                    (clinica,)
                ).fetchall()
    
    return jsonify(especialidades)

@app.route("/c/<clinica>/<especialidade>/", methods=("GET",))
def list_medicos(clinica, especialidade):
    with psycopg.connect(conninfo=DATABASE_URL) as conn:
        with conn.cursor(row_factory=namedtuple_row) as cur:
            
            medicos = cur.execute(
                """
                SELECT m.nif, m.nome
                FROM medico m
                JOIN trabalha t ON t.nif = m.nif
                JOIN clinica c ON c.nome = t.nome
                WHERE c.nome = %s AND m.especialidade = %s;
                """,
                (clinica, especialidade)
            ).fetchall()

            result = []

            for medico in medicos:
                horarios = cur.execute(
                        """
                        SELECT a.data, a.hora
                        FROM consulta a
                        WHERE a.nif = %s AND a.data > CURRENT_DATE
                        ORDER BY a.data, a.hora
                        LIMIT 3;
                        """,
                        (medico.nif,)
                    )
                
                result.append({
                    'nome': medico.nome,
                    'horarios_disponiveis': [{'data': horario.data.isoformat(), 'hora': horario.hora.isoformat()} for horario in horarios]
                })

    return jsonify(result)

@app.route("/a/<clinica>/registar/", methods=("POST",))
def register(clinica):
    
    paciente = str(request.args.get('paciente'))
    medico = str(request.args.get('medico'))
    data_consulta = str(request.args.get('data'))
    hora_consulta = str(request.args.get('hora'))

    if not all([paciente, medico, data_consulta, hora_consulta]):
        return jsonify({"erro": "Parametros em falta."}), 400
    
    today = datetime.now()
    hours = str(str(today.hour) + ':' + str(today.minute))
    today = str(today)

    if data_consulta < today or (data_consulta == today and hora_consulta <= hours):
        return jsonify({"erro": "A consulta tem que se realizar no futuro."}), 400
    

    with psycopg.connect(conninfo=DATABASE_URL) as conn:
            with conn.cursor(row_factory=namedtuple_row) as cur:
                cur.execute(
                    """
                    SELECT
                    COALESCE(MAX(CAST(id AS INTEGER)), 0) AS max_id
                    FROM consulta;
                    """,
                    {},)
                
                result = cur.fetchone()
                max_id = result.max_id

                
                id = str(max_id + 1)

                cur.execute(
                    """
                    INSERT INTO consulta (id, ssn, nif, nome, data, hora, codigo_sns)
                    VALUES (%s, %s, %s, %s, %s, %s, NULL);
                    """,
                    (id, paciente, medico, clinica, data_consulta, hora_consulta)
                )

                conn.commit()

    return jsonify({"Consulta inserida com sucesso."}), 204

@app.route("/a/<clinica>/cancelar/", methods=("POST",))
def cancelar_consulta(clinica):
    
    paciente = str(request.args.get('paciente'))
    medico = str(request.args.get('medico'))
    data_consulta = str(request.args.get('data'))
    hora_consulta = str(request.args.get('hora'))

    if not all([paciente, medico, data_consulta, hora_consulta]):
        return jsonify({"erro": "Parametros em falta."}), 400
    
    today = datetime.now()
    hours = str(str(today.hour) + ':' + str(today.minute))
    today = str(today)

    if data_consulta < today or (data_consulta == today and hora_consulta <= hours):
        return jsonify({"erro": "A consulta tem que se realizar no futuro."}), 400
    
    with psycopg.connect(conninfo=DATABASE_URL) as conn:
            with conn.cursor(row_factory=namedtuple_row) as cur:
                cur.execute(
                    """
                    DELETE FROM consulta
                    WHERE ssn = %s
                    AND nif = %s
                    AND nome = %s
                    AND data = %s
                    AND hora = %s
                    """,
                    (paciente, medico, clinica, data_consulta, hora_consulta)
                    )
                
                conn.commit()
    
    return jsonify({"Consulta cancelada com sucesso."}), 204

if __name__ == "__main__":
    app.run()
