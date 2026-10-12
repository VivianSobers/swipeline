"""Validate the entire dashboard archive against the pinned importer schemas offline."""
import sys
import os
import tempfile
from pathlib import Path
from zipfile import ZipFile

with tempfile.TemporaryDirectory() as work:
    secret = Path(work) / 'secret'
    secret.write_text('test-only-bundle-validation-secret-for-local-smoke')
    os.environ['SUPERSET_SECRET_KEY_PATH'] = str(secret)
    os.environ['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///' + str(Path(work) / 'metadata.db')
    from superset import create_app, db
    app = create_app()
    with app.app_context():
        db.create_all()
        from superset.commands.importers.v1.utils import get_contents_from_bundle, load_metadata, validate_metadata_type
        from superset.databases.schemas import ImportV1DatabaseSchema
        from superset.datasets.schemas import ImportV1DatasetSchema
        from superset.charts.schemas import ImportV1ChartSchema
        from superset.dashboards.schemas import ImportV1DashboardSchema
        import yaml
        contents = get_contents_from_bundle(ZipFile(sys.argv[1] if len(sys.argv) > 1 else '/app/dashboards/payments-operations.zip'))
        errors = []
        metadata = load_metadata(contents)
        validate_metadata_type(metadata, 'Dashboard', errors)
        schemas = {'databases/': ImportV1DatabaseSchema(), 'datasets/': ImportV1DatasetSchema(), 'charts/': ImportV1ChartSchema(), 'dashboards/': ImportV1DashboardSchema()}
        counts = dict.fromkeys(schemas, 0)
        for name, text in contents.items():
            for prefix, schema in schemas.items():
                if name.startswith(prefix):
                    counts[prefix] += 1
                    try:
                        schema.load(yaml.safe_load(text))
                    except Exception as exc:
                        errors.append((name, str(exc)))
        print('asset counts:', counts)
        if errors:
            raise AssertionError(errors)
        assert counts == {'databases/': 1, 'datasets/': 1, 'charts/': 6, 'dashboards/': 1}
        print('Pinned importer validates every database, dataset, chart and dashboard asset')
