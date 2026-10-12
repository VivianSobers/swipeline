FROM apache/superset:6.1.0
USER root
RUN uv pip install --python /app/.venv/bin/python pydruid==0.6.9 psycopg2-binary==2.9.10
COPY deploy/superset/superset_config.py /app/pythonpath/superset_config.py
COPY deploy/superset/test_config.py /app/test_config.py
COPY deploy/superset/test_bundle.py /app/test_bundle.py
COPY deploy/superset/init.sh /app/init.sh
RUN chmod +x /app/init.sh \
    && mkdir -p /secrets \
    && chown superset:superset /secrets
COPY dashboards/payments-operations.zip /app/dashboards/payments-operations.zip
USER superset
