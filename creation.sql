CREATE OR REPLACE FUNCTION dump(tablename regclass)
    RETURNS void
    LANGUAGE plpgsql
AS
$$
DECLARE
    original_columns TEXT;
    history_table    TEXT;
    view_name        TEXT;
BEGIN
    history_table = tablename || '_history';
    view_name = history_table || '_view';

    EXECUTE format($t$ CREATE TEMPORARY TABLE templ (LIKE %s) $t$, tablename);
    ALTER TABLE templ
        ADD COLUMN sys_period tstzrange NOT NULL DEFAULT tstzrange(current_timestamp, NULL);

    EXECUTE format($t$ CREATE TABLE %s (LIKE templ) $t$, history_table);
    SELECT array_to_string(
                   array(
                           SELECT column_name
                           FROM information_schema.COLUMNS
                           WHERE table_name = tablename::TEXT
                           ORDER BY ordinal_position), ',')
    INTO original_columns;
    EXECUTE format($t$
        CREATE TRIGGER %s
            BEFORE INSERT OR UPDATE OR DELETE ON %s
            FOR EACH ROW EXECUTE PROCEDURE versioning(
              'sys_period', %s, true
            )
        $t$
        , history_table || '_trigger', history_table, history_table);
    EXECUTE format($t$
        CREATE VIEW %s AS SELECT %s FROM %s
        $t$, view_name, original_columns, history_table);
    DROP TABLE templ;
    RETURN;
END
$$;


CREATE OR REPLACE FUNCTION enable_temporal(tablename regclass)
    RETURNS void
    LANGUAGE plpgsql
AS
$$
DECLARE
    original_columns TEXT;
    history_table    TEXT;
    view_name        TEXT;
    trigger_name     TEXT;
BEGIN
    history_table = '_' || tablename || '_history';
    view_name = tablename || '_view';
    trigger_name = history_table || '_trigger';

    SELECT array_to_string(
                   array(
                           SELECT column_name
                           FROM information_schema.COLUMNS
                           WHERE table_name = tablename::TEXT
                           ORDER BY ordinal_position), ',')
    INTO original_columns;

    EXECUTE format($t$
        ALTER TABLE %s
            ADD COLUMN sys_period tstzrange NOT NULL DEFAULT tstzrange(current_timestamp, NULL);
        $t$, tablename);

    EXECUTE format($t$ CREATE TABLE %s (LIKE %s) $t$, history_table, tablename);

    EXECUTE format($t$
        CREATE TRIGGER %s
            BEFORE INSERT OR UPDATE OR DELETE ON %s
            FOR EACH ROW EXECUTE PROCEDURE versioning(
              'sys_period', %s, true
            )
        $t$
        , trigger_name, tablename, history_table);

    EXECUTE format($t$
        CREATE VIEW %s AS SELECT %s FROM %s
        $t$, view_name, original_columns, tablename);

    RETURN;
END
$$;