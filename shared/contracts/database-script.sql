\set ON_ERROR_STOP on

\set money_schema     `cat common/money.schema.json`
\set variant_schema   `cat catalog/variant-v1.schema.json`
\set canonical_schema `cat catalog/canonical-product-v1.schema.json`

INSERT INTO operations.schema_registry (event_type, version, payload_class, json_schema)
VALUES (
  'urn:indigententerprises:common:money',
  1,
  'com.indigententerprises.applications.shared.contracts.MoneySchema',
  :'money_schema'::jsonb
)
ON CONFLICT (event_type, version) DO UPDATE
SET payload_class = EXCLUDED.payload_class,
    json_schema   = EXCLUDED.json_schema,
    enabled       = true,
    updated_at    = now();

INSERT INTO operations.schema_registry (event_type, version, payload_class, json_schema)
VALUES (
  'urn:indigententerprises:catalog:variant',
  1,
  'com.indigententerprises.applications.shared.contracts.VariantV1Schema',
  :'variant_schema'::jsonb
)
ON CONFLICT (event_type, version) DO UPDATE
SET payload_class = EXCLUDED.payload_class,
    json_schema   = EXCLUDED.json_schema,
    enabled       = true,
    updated_at    = now();

INSERT INTO operations.schema_registry (event_type, version, payload_class, json_schema)
VALUES (
  'urn:indigententerprises:catalog:canonical-product',
  1,
  'com.indigententerprises.applications.shared.contracts.CanonicalProductV1Schema',
  :'canonical_schema'::jsonb
)
ON CONFLICT (event_type, version) DO UPDATE
SET payload_class = EXCLUDED.payload_class,
    json_schema   = EXCLUDED.json_schema,
    enabled       = true,
    updated_at    = now();

