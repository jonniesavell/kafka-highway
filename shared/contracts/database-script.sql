\set ON_ERROR_STOP on

\set ident_schema     `cat common/identifiers.schema.json`
\set money_schema     `cat common/money.schema.json`
\set variant_schema   `cat catalog/variant-v1.schema.json`
\set canonical_schema `cat catalog/canonical-product-v1.schema.json`

INSERT
  INTO operations.schema_documents(schema_id, json_schema)
VALUES ('urn:indigententerprises:common:identifiers:1', :'ident_schema'::jsonb)
ON CONFLICT (schema_id) DO UPDATE
SET json_schema = EXCLUDED.json_schema,
    enabled     = true,
    updated_at  = now();

INSERT
  INTO operations.schema_documents(schema_id, json_schema)
VALUES ('urn:indigententerprises:common:money:1', :'money_schema'::jsonb)
ON CONFLICT (schema_id) DO UPDATE
SET json_schema = EXCLUDED.json_schema,
    enabled     = true,
    updated_at  = now();

INSERT
  INTO operations.schema_registry(event_type, version, schema_id, payload_class)
VALUES (
  'urn:indigententerprises:common:money',
  1,
  'urn:indigententerprises:common:money:1',
  'com.indigententerprises.applications.shared.contracts.MoneySchema'
)
ON CONFLICT (event_type, version) DO UPDATE
SET schema_id     = EXCLUDED.schema_id,
    payload_class = EXCLUDED.payload_class,
    enabled       = true,
    updated_at    = now();

INSERT
  INTO operations.schema_documents(schema_id, json_schema)
VALUES ('urn:indigententerprises:catalog:variant:1', :'variant_schema'::jsonb)
ON CONFLICT (schema_id) DO UPDATE
SET json_schema = EXCLUDED.json_schema,
    enabled     = true,
    updated_at  = now();

INSERT
  INTO operations.schema_registry(event_type, version, schema_id, payload_class)
VALUES (
  'urn:indigententerprises:catalog:variant',
  1,
  'urn:indigententerprises:catalog:variant:1',
  'com.indigententerprises.applications.shared.contracts.VariantV1Schema'
)
ON CONFLICT (event_type, version) DO UPDATE
SET schema_id     = EXCLUDED.schema_id,
    payload_class = EXCLUDED.payload_class,
    enabled       = true,
    updated_at    = now();

INSERT
  INTO operations.schema_documents(schema_id, json_schema)
VALUES ('urn:indigententerprises:catalog:canonical-product:1', :'canonical_schema'::jsonb)
ON CONFLICT (schema_id) DO UPDATE
SET json_schema = EXCLUDED.json_schema,
    enabled     = true,
    updated_at  = now();

INSERT INTO operations.schema_registry (event_type, version, schema_id, payload_class)
VALUES (
  'urn:indigententerprises:catalog:canonical-product',
  1,
  'urn:indigententerprises:catalog:canonical-product:1',
  'com.indigententerprises.applications.shared.contracts.CanonicalProductV1Schema'
)
ON CONFLICT (event_type, version) DO UPDATE
SET schema_id     = EXCLUDED.schema_id,
    payload_class = EXCLUDED.payload_class,
    enabled       = true,
    updated_at    = now();
