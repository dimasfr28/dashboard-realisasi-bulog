-- RPC Functions untuk Target Kanwil dan Target Kancab Comparison

-- ===== TARGET KANWIL RPC =====

-- Function untuk mendapatkan data dari target_kanwil_compare yang tidak ada di target_kanwil
-- NEW: Date IS part of comparison (date + kanwil_id)
CREATE OR REPLACE FUNCTION get_target_kanwil_compare_not_exists_page(
    p_last_id bigint,
    p_limit integer DEFAULT 1000
)
RETURNS TABLE (
    target_kanwil_compare_id bigint,
    row_hash text
)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    SELECT
        tc.id as target_kanwil_compare_id,
        tc.row_hash
    FROM target_kanwil_compare tc
    WHERE tc.id > p_last_id
      AND NOT EXISTS (
          SELECT 1
          FROM target_kanwil t
          WHERE t.date = tc.date
            AND t.kanwil_id = tc.kanwil_id
      )
    ORDER BY tc.id
    LIMIT p_limit;
END;
$$;

-- Function untuk UPSERT data dari target_kanwil_compare ke target_kanwil
-- Jika kombinasi (date, kanwil_id) sudah ada, UPDATE target_setara_beras
-- Jika belum ada, INSERT record baru
CREATE OR REPLACE FUNCTION upsert_target_kanwil_from_compare(
    p_compare_ids bigint[]
)
RETURNS TABLE (
    inserted_count integer,
    updated_count integer
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_inserted_count integer := 0;
    v_updated_count integer := 0;
    v_record RECORD;
BEGIN
    -- Loop through each compare ID
    FOR v_record IN
        SELECT tc.kanwil_id, tc.target_setara_beras, tc.date
        FROM target_kanwil_compare tc
        WHERE tc.id = ANY(p_compare_ids)
    LOOP
        -- Try to update existing record
        UPDATE target_kanwil
        SET target_setara_beras = v_record.target_setara_beras
        WHERE date = v_record.date AND kanwil_id = v_record.kanwil_id;

        -- If no rows updated, insert new record
        IF NOT FOUND THEN
            INSERT INTO target_kanwil (kanwil_id, target_setara_beras, date)
            VALUES (v_record.kanwil_id, v_record.target_setara_beras, v_record.date);
            v_inserted_count := v_inserted_count + 1;
        ELSE
            v_updated_count := v_updated_count + 1;
        END IF;
    END LOOP;

    RETURN QUERY SELECT v_inserted_count, v_updated_count;
END;
$$;

-- ===== TARGET KANCAB RPC =====

-- Function untuk mendapatkan data dari target_kancab_compare yang tidak ada di target_kancab
-- NEW: Date IS part of comparison (date + kancab_id)
CREATE OR REPLACE FUNCTION get_target_kancab_compare_not_exists_page(
    p_last_id bigint,
    p_limit integer DEFAULT 1000
)
RETURNS TABLE (
    target_kancab_compare_id bigint,
    row_hash text
)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    SELECT
        tc.id as target_kancab_compare_id,
        tc.row_hash
    FROM target_kancab_compare tc
    WHERE tc.id > p_last_id
      AND NOT EXISTS (
          SELECT 1
          FROM target_kancab t
          WHERE t.date = tc.date
            AND t.kancab_id = tc.kancab_id
      )
    ORDER BY tc.id
    LIMIT p_limit;
END;
$$;

-- Function untuk UPSERT data dari target_kancab_compare ke target_kancab
-- Jika kombinasi (date, kancab_id) sudah ada, UPDATE target_setara_beras
-- Jika belum ada, INSERT record baru
CREATE OR REPLACE FUNCTION upsert_target_kancab_from_compare(
    p_compare_ids bigint[]
)
RETURNS TABLE (
    inserted_count integer,
    updated_count integer
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_inserted_count integer := 0;
    v_updated_count integer := 0;
    v_record RECORD;
BEGIN
    -- Loop through each compare ID
    FOR v_record IN
        SELECT tc.kancab_id, tc.target_setara_beras, tc.date
        FROM target_kancab_compare tc
        WHERE tc.id = ANY(p_compare_ids)
    LOOP
        -- Try to update existing record
        UPDATE target_kancab
        SET target_setara_beras = v_record.target_setara_beras
        WHERE date = v_record.date AND kancab_id = v_record.kancab_id;

        -- If no rows updated, insert new record
        IF NOT FOUND THEN
            INSERT INTO target_kancab (kancab_id, target_setara_beras, date)
            VALUES (v_record.kancab_id, v_record.target_setara_beras, v_record.date);
            v_inserted_count := v_inserted_count + 1;
        ELSE
            v_updated_count := v_updated_count + 1;
        END IF;
    END LOOP;

    RETURN QUERY SELECT v_inserted_count, v_updated_count;
END;
$$;

-- Grant execute permissions
GRANT EXECUTE ON FUNCTION get_target_kanwil_compare_not_exists_page(bigint, integer) TO authenticated, anon;
GRANT EXECUTE ON FUNCTION get_target_kancab_compare_not_exists_page(bigint, integer) TO authenticated, anon;
GRANT EXECUTE ON FUNCTION upsert_target_kanwil_from_compare(bigint[]) TO authenticated, anon;
GRANT EXECUTE ON FUNCTION upsert_target_kancab_from_compare(bigint[]) TO authenticated, anon;
