-- RPC Functions untuk Target Kanwil dan Target Kancab Comparison

-- ===== TARGET KANWIL RPC =====

-- Function untuk mendapatkan data dari target_kanwil_compare yang tidak ada di target_kanwil
-- Date is NOT part of comparison (only kanwil_id and target_setara_beras)
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
          WHERE t.kanwil_id = tc.kanwil_id
            AND t.target_setara_beras = tc.target_setara_beras
      )
    ORDER BY tc.id
    LIMIT p_limit;
END;
$$;

-- ===== TARGET KANCAB RPC =====

-- Function untuk mendapatkan data dari target_kancab_compare yang tidak ada di target_kancab
-- Date is NOT part of comparison (only kancab_id and target_setara_beras)
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
          WHERE t.kancab_id = tc.kancab_id
            AND t.target_setara_beras = tc.target_setara_beras
      )
    ORDER BY tc.id
    LIMIT p_limit;
END;
$$;

-- Grant execute permissions
GRANT EXECUTE ON FUNCTION get_target_kanwil_compare_not_exists_page(bigint, integer) TO authenticated, anon;
GRANT EXECUTE ON FUNCTION get_target_kancab_compare_not_exists_page(bigint, integer) TO authenticated, anon;
