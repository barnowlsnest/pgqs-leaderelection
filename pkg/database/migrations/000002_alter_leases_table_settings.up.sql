ALTER TABLE IF EXISTS leases SET (
    fillfactor = 70,
    autovacuum_vacuum_scale_factor = 0,
    autovacuum_vacuum_threshold = 100
);
