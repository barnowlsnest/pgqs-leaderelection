ALTER TABLE IF EXISTS leases RESET (
    fillfactor,
    autovacuum_vacuum_scale_factor,
    autovacuum_vacuum_threshold
);
