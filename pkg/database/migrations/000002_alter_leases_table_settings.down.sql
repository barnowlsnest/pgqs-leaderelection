ALTER TABLE IF EXISTS pgqs.leases RESET (
    fillfactor,
    autovacuum_vacuum_scale_factor,
    autovacuum_vacuum_threshold
);
