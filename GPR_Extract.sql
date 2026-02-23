WITH params AS (
  SELECT
    ['404', '415'] AS bu_list,
    TIMESTAMP('2025-09-01 00:00:00') AS from_ts,
    TIMESTAMP('2026-01-31 23:59:59') AS to_ts
)

SELECT
  'v2_sto_ttyp_240' AS table,
  t.publish_time,
  t.businessUnit_Code,
  t.item_Number,
  t.internalTransaction_CreationTimestamp,
  t.internalTransaction_AdjustmentQuantity,
  t.internalTransaction_Reason_Code,
  t.internalTransaction_Reason_SubCode,
  t.internalTransaction_TransactionEvent_EventCode,
  t.internalTransaction_TransactionEvent_IKEACode
FROM `ingka-ilo-ct-prod.dm_core_inventory_transaction.v2_sto_ttyp_240` AS t
CROSS JOIN params p
WHERE t.businessUnit_Code IN UNNEST(p.bu_list)
  AND t.internalTransaction_CreationTimestamp BETWEEN p.from_ts AND p.to_ts

UNION ALL
SELECT
  'v2_sto_ttyp_280' AS table,
  t.publish_time,
  t.businessUnit_Code,
  t.item_Number,
  t.internalTransaction_CreationTimestamp,
  t.internalTransaction_AdjustmentQuantity,
  t.internalTransaction_Reason_Code,
  t.internalTransaction_Reason_SubCode,
  t.internalTransaction_TransactionEvent_EventCode,
  t.internalTransaction_TransactionEvent_IKEACode
FROM `ingka-ilo-ct-prod.dm_core_inventory_transaction.v2_sto_ttyp_280` AS t
CROSS JOIN params p
WHERE t.businessUnit_Code IN UNNEST(p.bu_list)
  AND t.internalTransaction_CreationTimestamp BETWEEN p.from_ts AND p.to_ts

UNION ALL
SELECT
  'v2_sto_ttyp_310' AS table,
  t.publish_time,
  t.businessUnit_Code,
  t.item_Number,
  t.internalTransaction_CreationTimestamp,
  t.internalTransaction_AdjustmentQuantity,
  t.internalTransaction_Reason_Code,
  t.internalTransaction_Reason_SubCode,
  t.internalTransaction_TransactionEvent_EventCode,
  t.internalTransaction_TransactionEvent_IKEACode
FROM `ingka-ilo-ct-prod.dm_core_inventory_transaction.v2_sto_ttyp_310` AS t
CROSS JOIN params p
WHERE t.businessUnit_Code IN UNNEST(p.bu_list)
  AND t.internalTransaction_CreationTimestamp BETWEEN p.from_ts AND p.to_ts

UNION ALL
SELECT
  'v2_sto_ttyp_319' AS table,
  t.publish_time,
  t.businessUnit_Code,
  t.item_Number,
  t.internalTransaction_CreationTimestamp,
  t.internalTransaction_AdjustmentQuantity,
  t.internalTransaction_Reason_Code,
  t.internalTransaction_Reason_SubCode,
  t.internalTransaction_TransactionEvent_EventCode,
  t.internalTransaction_TransactionEvent_IKEACode
FROM `ingka-ilo-ct-prod.dm_core_inventory_transaction.v2_sto_ttyp_319` AS t
CROSS JOIN params p
WHERE t.businessUnit_Code IN UNNEST(p.bu_list)
  AND t.internalTransaction_CreationTimestamp BETWEEN p.from_ts AND p.to_ts

UNION ALL
SELECT
  'v2_sto_ttyp_323' AS table,
  t.publish_time,
  t.businessUnit_Code,
  t.item_Number,
  t.internalTransaction_CreationTimestamp,
  t.internalTransaction_AdjustmentQuantity,
  t.internalTransaction_Reason_Code,
  t.internalTransaction_Reason_SubCode,
  t.internalTransaction_TransactionEvent_EventCode,
  t.internalTransaction_TransactionEvent_IKEACode
FROM `ingka-ilo-ct-prod.dm_core_inventory_transaction.v2_sto_ttyp_323` AS t
CROSS JOIN params p
WHERE t.businessUnit_Code IN UNNEST(p.bu_list)
  AND t.internalTransaction_CreationTimestamp BETWEEN p.from_ts AND p.to_ts

UNION ALL
SELECT
  'v2_sto_ttyp_324' AS table,
  t.publish_time,
  t.businessUnit_Code,
  t.item_Number,
  t.internalTransaction_CreationTimestamp,
  t.internalTransaction_AdjustmentQuantity,
  t.internalTransaction_Reason_Code,
  t.internalTransaction_Reason_SubCode,
  t.internalTransaction_TransactionEvent_EventCode,
  t.internalTransaction_TransactionEvent_IKEACode
FROM `ingka-ilo-ct-prod.dm_core_inventory_transaction.v2_sto_ttyp_324` AS t
CROSS JOIN params p
WHERE t.businessUnit_Code IN UNNEST(p.bu_list)
  AND t.internalTransaction_CreationTimestamp BETWEEN p.from_ts AND p.to_ts

UNION ALL
SELECT
  'v2_sto_ttyp_325' AS table,
  t.publish_time,
  t.businessUnit_Code,
  t.item_Number,
  t.internalTransaction_CreationTimestamp,
  t.internalTransaction_AdjustmentQuantity,
  t.internalTransaction_Reason_Code,
  t.internalTransaction_Reason_SubCode,
  t.internalTransaction_TransactionEvent_EventCode,
  t.internalTransaction_TransactionEvent_IKEACode
FROM `ingka-ilo-ct-prod.dm_core_inventory_transaction.v2_sto_ttyp_325` AS t
CROSS JOIN params p
WHERE t.businessUnit_Code IN UNNEST(p.bu_list)
  AND t.internalTransaction_CreationTimestamp BETWEEN p.from_ts AND p.to_ts

UNION ALL
SELECT
  'v2_sto_ttyp_390' AS table,
  t.publish_time,
  t.businessUnit_Code,
  t.item_Number,
  t.internalTransaction_CreationTimestamp,
  t.internalTransaction_AdjustmentQuantity,
  t.internalTransaction_Reason_Code,
  t.internalTransaction_Reason_SubCode,
  t.internalTransaction_TransactionEvent_EventCode,
  t.internalTransaction_TransactionEvent_IKEACode
FROM `ingka-ilo-ct-prod.dm_core_inventory_transaction.v2_sto_ttyp_390` AS t
CROSS JOIN params p
WHERE t.businessUnit_Code IN UNNEST(p.bu_list)
  AND t.internalTransaction_CreationTimestamp BETWEEN p.from_ts AND p.to_ts

UNION ALL
SELECT
  'v2_sto_ttyp_391' AS table,
  t.publish_time,
  t.businessUnit_Code,
  t.item_Number,
  t.internalTransaction_CreationTimestamp,
  t.internalTransaction_AdjustmentQuantity,
  t.internalTransaction_Reason_Code,
  t.internalTransaction_Reason_SubCode,
  t.internalTransaction_TransactionEvent_EventCode,
  t.internalTransaction_TransactionEvent_IKEACode
FROM `ingka-ilo-ct-prod.dm_core_inventory_transaction.v2_sto_ttyp_391` AS t
CROSS JOIN params p
WHERE t.businessUnit_Code IN UNNEST(p.bu_list)
  AND t.internalTransaction_CreationTimestamp BETWEEN p.from_ts AND p.to_ts

UNION ALL
SELECT
  'v2_sto_ttyp_395' AS table,
  t.publish_time,
  t.businessUnit_Code,
  t.item_Number,
  t.internalTransaction_CreationTimestamp,
  t.internalTransaction_AdjustmentQuantity,
  t.internalTransaction_Reason_Code,
  t.internalTransaction_Reason_SubCode,
  t.internalTransaction_TransactionEvent_EventCode,
  t.internalTransaction_TransactionEvent_IKEACode
FROM `ingka-ilo-ct-prod.dm_core_inventory_transaction.v2_sto_ttyp_395` AS t
CROSS JOIN params p
WHERE t.businessUnit_Code IN UNNEST(p.bu_list)
  AND t.internalTransaction_CreationTimestamp BETWEEN p.from_ts AND p.to_ts

UNION ALL
SELECT
  'v2_sto_ttyp_396' AS table,
  t.publish_time,
  t.businessUnit_Code,
  t.item_Number,
  t.internalTransaction_CreationTimestamp,
  t.internalTransaction_AdjustmentQuantity,
  t.internalTransaction_Reason_Code,
  t.internalTransaction_Reason_SubCode,
  t.internalTransaction_TransactionEvent_EventCode,
  t.internalTransaction_TransactionEvent_IKEACode
FROM `ingka-ilo-ct-prod.dm_core_inventory_transaction.v2_sto_ttyp_396` AS t
CROSS JOIN params p
WHERE t.businessUnit_Code IN UNNEST(p.bu_list)
  AND t.internalTransaction_CreationTimestamp BETWEEN p.from_ts AND p.to_ts

UNION ALL
SELECT
  'v2_sto_ttyp_397' AS table,
  t.publish_time,
  t.businessUnit_Code,
  t.item_Number,
  t.internalTransaction_CreationTimestamp,
  t.internalTransaction_AdjustmentQuantity,
  t.internalTransaction_Reason_Code,
  t.internalTransaction_Reason_SubCode,
  t.internalTransaction_TransactionEvent_EventCode,
  t.internalTransaction_TransactionEvent_IKEACode
FROM `ingka-ilo-ct-prod.dm_core_inventory_transaction.v2_sto_ttyp_397` AS t
CROSS JOIN params p
WHERE t.businessUnit_Code IN UNNEST(p.bu_list)
  AND t.internalTransaction_CreationTimestamp BETWEEN p.from_ts AND p.to_ts

UNION ALL
SELECT
  'v2_sto_ttyp_398' AS table,
  t.publish_time,
  t.businessUnit_Code,
  t.item_Number,
  t.internalTransaction_CreationTimestamp,
  t.internalTransaction_AdjustmentQuantity,
  t.internalTransaction_Reason_Code,
  t.internalTransaction_Reason_SubCode,
  t.internalTransaction_TransactionEvent_EventCode,
  t.internalTransaction_TransactionEvent_IKEACode
FROM `ingka-ilo-ct-prod.dm_core_inventory_transaction.v2_sto_ttyp_398` AS t
CROSS JOIN params p
WHERE t.businessUnit_Code IN UNNEST(p.bu_list)
  AND t.internalTransaction_CreationTimestamp BETWEEN p.from_ts AND p.to_ts

UNION ALL
SELECT
  'v2_sto_ttyp_431' AS table,
  t.publish_time,
  t.businessUnit_Code,
  t.item_Number,
  t.internalTransaction_CreationTimestamp,
  t.internalTransaction_AdjustmentQuantity,
  t.internalTransaction_Reason_Code,
  t.internalTransaction_Reason_SubCode,
  t.internalTransaction_TransactionEvent_EventCode,
  t.internalTransaction_TransactionEvent_IKEACode
FROM `ingka-ilo-ct-prod.dm_core_inventory_transaction.v2_sto_ttyp_431` AS t
CROSS JOIN params p
WHERE t.businessUnit_Code IN UNNEST(p.bu_list)
  AND t.internalTransaction_CreationTimestamp BETWEEN p.from_ts AND p.to_ts

UNION ALL
SELECT
  'v2_sto_ttyp_441' AS table,
  t.publish_time,
  t.businessUnit_Code,
  t.item_Number,
  t.internalTransaction_CreationTimestamp,
  t.internalTransaction_AdjustmentQuantity,
  t.internalTransaction_Reason_Code,
  t.internalTransaction_Reason_SubCode,
  t.internalTransaction_TransactionEvent_EventCode,
  t.internalTransaction_TransactionEvent_IKEACode
FROM `ingka-ilo-ct-prod.dm_core_inventory_transaction.v2_sto_ttyp_441` AS t
CROSS JOIN params p
WHERE t.businessUnit_Code IN UNNEST(p.bu_list)
  AND t.internalTransaction_CreationTimestamp BETWEEN p.from_ts AND p.to_ts

UNION ALL
SELECT
  'v2_sto_ttyp_456' AS table,
  t.publish_time,
  t.businessUnit_Code,
  t.item_Number,
  t.internalTransaction_CreationTimestamp,
  t.internalTransaction_AdjustmentQuantity,
  t.internalTransaction_Reason_Code,
  t.internalTransaction_Reason_SubCode,
  t.internalTransaction_TransactionEvent_EventCode,
  t.internalTransaction_TransactionEvent_IKEACode
FROM `ingka-ilo-ct-prod.dm_core_inventory_transaction.v2_sto_ttyp_456` AS t
CROSS JOIN params p
WHERE t.businessUnit_Code IN UNNEST(p.bu_list)
  AND t.internalTransaction_CreationTimestamp BETWEEN p.from_ts AND p.to_ts

UNION ALL
SELECT
  'v2_sto_ttyp_471' AS table,
  t.publish_time,
  t.businessUnit_Code,
  t.item_Number,
  t.internalTransaction_CreationTimestamp,
  t.internalTransaction_AdjustmentQuantity,
  t.internalTransaction_Reason_Code,
  t.internalTransaction_Reason_SubCode,
  t.internalTransaction_TransactionEvent_EventCode,
  t.internalTransaction_TransactionEvent_IKEACode
FROM `ingka-ilo-ct-prod.dm_core_inventory_transaction.v2_sto_ttyp_471` AS t
CROSS JOIN params p
WHERE t.businessUnit_Code IN UNNEST(p.bu_list)
  AND t.internalTransaction_CreationTimestamp BETWEEN p.from_ts AND p.to_ts

UNION ALL
SELECT
  'v2_sto_ttyp_472' AS table,
  t.publish_time,
  t.businessUnit_Code,
  t.item_Number,
  t.internalTransaction_CreationTimestamp,
  t.internalTransaction_AdjustmentQuantity,
  t.internalTransaction_Reason_Code,
  t.internalTransaction_Reason_SubCode,
  t.internalTransaction_TransactionEvent_EventCode,
  t.internalTransaction_TransactionEvent_IKEACode
FROM `ingka-ilo-ct-prod.dm_core_inventory_transaction.v2_sto_ttyp_472` AS t
CROSS JOIN params p
WHERE t.businessUnit_Code IN UNNEST(p.bu_list)
  AND t.internalTransaction_CreationTimestamp BETWEEN p.from_ts AND p.to_ts

UNION ALL
SELECT
  'v2_sto_ttyp_473' AS table,
  t.publish_time,
  t.businessUnit_Code,
  t.item_Number,
  t.internalTransaction_CreationTimestamp,
  t.internalTransaction_AdjustmentQuantity,
  t.internalTransaction_Reason_Code,
  t.internalTransaction_Reason_SubCode,
  t.internalTransaction_TransactionEvent_EventCode,
  t.internalTransaction_TransactionEvent_IKEACode
FROM `ingka-ilo-ct-prod.dm_core_inventory_transaction.v2_sto_ttyp_473` AS t
CROSS JOIN params p
WHERE t.businessUnit_Code IN UNNEST(p.bu_list)
  AND t.internalTransaction_CreationTimestamp BETWEEN p.from_ts AND p.to_ts

UNION ALL
SELECT
  'v2_sto_ttyp_474' AS table,
  t.publish_time,
  t.businessUnit_Code,
  t.item_Number,
  t.internalTransaction_CreationTimestamp,
  t.internalTransaction_AdjustmentQuantity,
  t.internalTransaction_Reason_Code,
  t.internalTransaction_Reason_SubCode,
  t.internalTransaction_TransactionEvent_EventCode,
  t.internalTransaction_TransactionEvent_IKEACode
FROM `ingka-ilo-ct-prod.dm_core_inventory_transaction.v2_sto_ttyp_474` AS t
CROSS JOIN params p
WHERE t.businessUnit_Code IN UNNEST(p.bu_list)
  AND t.internalTransaction_CreationTimestamp BETWEEN p.from_ts AND p.to_ts

UNION ALL
SELECT
  'v2_sto_ttyp_475' AS table,
  t.publish_time,
  t.businessUnit_Code,
  t.item_Number,
  t.internalTransaction_CreationTimestamp,
  t.internalTransaction_AdjustmentQuantity,
  t.internalTransaction_Reason_Code,
  t.internalTransaction_Reason_SubCode,
  t.internalTransaction_TransactionEvent_EventCode,
  t.internalTransaction_TransactionEvent_IKEACode
FROM `ingka-ilo-ct-prod.dm_core_inventory_transaction.v2_sto_ttyp_475` AS t
CROSS JOIN params p
WHERE t.businessUnit_Code IN UNNEST(p.bu_list)
  AND t.internalTransaction_CreationTimestamp BETWEEN p.from_ts AND p.to_ts

UNION ALL
SELECT
  'v2_sto_ttyp_476' AS table,
  t.publish_time,
  t.businessUnit_Code,
  t.item_Number,
  t.internalTransaction_CreationTimestamp,
  t.internalTransaction_AdjustmentQuantity,
  t.internalTransaction_Reason_Code,
  t.internalTransaction_Reason_SubCode,
  t.internalTransaction_TransactionEvent_EventCode,
  t.internalTransaction_TransactionEvent_IKEACode
FROM `ingka-ilo-ct-prod.dm_core_inventory_transaction.v2_sto_ttyp_476` AS t
CROSS JOIN params p
WHERE t.businessUnit_Code IN UNNEST(p.bu_list)
  AND t.internalTransaction_CreationTimestamp BETWEEN p.from_ts AND p.to_ts;
