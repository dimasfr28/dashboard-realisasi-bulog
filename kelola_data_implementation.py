# =================================================================
# IMPLEMENTASI BARU KELOLA DATA - SESUAI PROMPT.TXT
# Flow: Load All DB → Add nama_kanwil/kancab → Upload → Compare → Append/Replace
# =================================================================

# Fungsi untuk load ALL data dari database per 1000 baris
def load_all_realisasi_from_db_with_progress(supabase):
    """
    Load SEMUA data dari tabel realisasi per 1000 baris dengan progress bar
    Menambahkan nama_kanwil dan nama_kancab dari join
    """
    import time
    start_time = time.time()

    st.markdown("---")
    st.markdown('<h4 style="color: #1f497d;">📥 Loading Data dari Database</h4>', unsafe_allow_html=True)

    # Step 1: Get total count
    print("=" * 80)
    print("LOADING ALL DATA FROM DATABASE - START")
    print("=" * 80)

    st.info("🔄 Step 1: Menghitung total data di database...")
    count_result = supabase.table('realisasi').select('*', count='exact').execute()
    total_records = count_result.count
    st.success(f"✅ Total data di database: **{total_records:,}** records")

    print(f"[STEP 1] Total records in database: {total_records:,}")

    if total_records == 0:
        st.warning("⚠️ Tabel realisasi masih kosong")
        print("[WARNING] Table is empty")
        return pd.DataFrame(), {}, {}

    # Step 2: Load kanwil and kancab mappings
    st.info("🔄 Step 2: Loading mapping Kanwil & Kancab...")
    kanwil_map = {}  # kanwil_id -> nama_kanwil
    kancab_map = {}  # kancab_id -> nama_kancab

    kanwil_result = supabase.table('kanwil').select('*').execute()
    for kw in kanwil_result.data:
        kanwil_map[kw['kanwil_id']] = kw['nama_kanwil']

    kancab_result = supabase.table('kancab').select('*').execute()
    for kc in kancab_result.data:
        kancab_map[kc['kancab_id']] = kc['nama_kancab']

    st.success(f"✅ Loaded {len(kanwil_map)} Kanwil & {len(kancab_map)} Kancab mappings")
    print(f"[STEP 2] Loaded {len(kanwil_map)} Kanwil & {len(kancab_map)} Kancab mappings")

    # Step 3: Load data per 1000 rows
    st.info("🔄 Step 3: Loading data dari database per 1000 baris...")
    batch_size = 1000
    total_batches = (total_records + batch_size - 1) // batch_size

    print(f"[STEP 3] Will load {total_batches} batches of {batch_size} records each")
    print("-" * 80)

    all_data = []
    progress_bar = st.progress(0, f"Loading batch 1/{total_batches}...")

    # Create placeholder for real-time updates
    status_placeholder = st.empty()

    for batch_num in range(total_batches):
        batch_start = time.time()
        offset = batch_num * batch_size

        # Fetch batch
        result = supabase.table('realisasi')\
            .select('*')\
            .range(offset, offset + batch_size - 1)\
            .execute()

        # Add to list
        batch_count = len(result.data)
        all_data.extend(result.data)

        batch_elapsed = time.time() - batch_start

        # Update progress
        progress = ((batch_num + 1) / total_batches)
        progress_bar.progress(
            progress,
            f"Loading batch {batch_num + 1}/{total_batches} - {len(all_data):,}/{total_records:,} records"
        )

        # Print to console
        print(f"[BATCH {batch_num + 1}/{total_batches}] Loaded {batch_count:,} records (offset {offset:,}) - "
              f"Total so far: {len(all_data):,}/{total_records:,} ({progress*100:.1f}%) - "
              f"Time: {batch_elapsed:.2f}s")

        # Update status
        status_placeholder.info(f"📊 Loaded {len(all_data):,} / {total_records:,} records ({progress*100:.1f}%)")

    progress_bar.empty()
    status_placeholder.empty()

    print("-" * 80)
    print(f"[STEP 3 COMPLETE] Total records loaded: {len(all_data):,}")

    # Step 4: Convert to DataFrame and add nama columns
    st.info("🔄 Step 4: Converting ke DataFrame dan menambahkan nama_kanwil & nama_kancab...")
    print("[STEP 4] Converting to DataFrame...")

    df = pd.DataFrame(all_data)
    print(f"[STEP 4] DataFrame created with shape: {df.shape}")

    # Add nama_kanwil and nama_kancab columns
    df['nama_kanwil'] = df['kanwil_id'].map(kanwil_map)
    df['nama_kancab'] = df['kancab_id'].map(kancab_map)

    print(f"[STEP 4] Added nama_kanwil and nama_kancab columns")
    print(f"[STEP 4] Final DataFrame columns: {df.columns.tolist()}")

    total_elapsed = time.time() - start_time
    st.success(f"✅ Berhasil load {len(df):,} records dengan nama_kanwil & nama_kancab dalam {total_elapsed:.2f} detik")

    print("=" * 80)
    print(f"LOADING COMPLETE - Total time: {total_elapsed:.2f} seconds ({total_elapsed/60:.2f} minutes)")
    print(f"Average: {len(df)/total_elapsed:.0f} records/second")
    print("=" * 80)

    # Debug: Show sample
    with st.expander("👁️ Preview Data dari Database (5 rows pertama)", expanded=False):
        st.dataframe(df.head(), use_container_width=True)
        st.write(f"Total Columns: {len(df.columns)}")
        st.write(f"Columns: {df.columns.tolist()}")

    return df, kanwil_map, kancab_map


# Fungsi untuk compare dan find unique records
def find_unique_records(df_db, df_new, kanwil_map, kancab_map):
    """
    Compare df_new dengan df_db dan return unique records
    Unique berdasarkan key fields yang penting
    """
    st.markdown("---")
    st.markdown('<h4 style="color: #1f497d;">🔍 Analisis Data Unik</h4>', unsafe_allow_html=True)

    st.info("🔄 Preparing data baru untuk comparison...")

    # Prepare df_new: add kanwil_id and kancab_id
    df_new_prepared = df_new.copy()

    # Create reverse mapping: nama -> id
    kanwil_name_to_id = {v: k for k, v in kanwil_map.items()}
    kancab_name_to_id = {v: k for k, v in kancab_map.items()}

    # Map kanwil and Entitas to IDs
    df_new_prepared['kanwil_id'] = df_new_prepared['kanwil'].map(kanwil_name_to_id)
    df_new_prepared['kancab_id'] = df_new_prepared['Entitas'].map(kancab_name_to_id)

    # Define key fields for uniqueness check (sesuai prompt.txt)
    key_fields = [
        'nomor_po', 'no_in_out', 'tanggal_penerimaan',
        'komoditi', 'spesifikasi', 'kanwil_id', 'kancab_id'
    ]

    # Map Excel columns to DB columns for new data
    df_new_keys = df_new_prepared.copy()
    df_new_keys['nomor_po'] = df_new_prepared['Nomor PO'].apply(clean_value)
    df_new_keys['no_in_out'] = df_new_prepared['Nomor IN / OUT'].apply(clean_value)
    df_new_keys['tanggal_penerimaan'] = df_new_prepared['Tanggal Penerimaan'].apply(convert_to_date)
    df_new_keys['komoditi'] = df_new_prepared['Komoditi'].apply(clean_value)
    df_new_keys['spesifikasi'] = df_new_prepared['spesifikasi'].apply(clean_value)

    st.info("🔄 Creating hash untuk comparison...")

    # Create hash for comparison
    def create_hash(row):
        values = tuple(str(row[field]) for field in key_fields if field in row.index)
        return hash(values)

    # Get existing hashes from DB
    if len(df_db) > 0:
        existing_hashes = set(df_db[key_fields].apply(create_hash, axis=1))
    else:
        existing_hashes = set()

    # Get new hashes
    new_hashes = df_new_keys[key_fields].apply(create_hash, axis=1)

    # Find unique
    unique_mask = ~new_hashes.isin(existing_hashes)
    df_unique = df_new[unique_mask].copy()

    num_unique = len(df_unique)
    num_duplicate = len(df_new) - num_unique

    st.success(f"✅ Analisis selesai!")

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("📊 Total Data Baru", f"{len(df_new):,}")
    with col2:
        st.metric("✅ Data Unik", f"{num_unique:,}")
    with col3:
        st.metric("⚠️ Data Duplikat", f"{num_duplicate:,}")

    return df_unique, num_unique, num_duplicate


# Fungsi untuk TRUNCATE table (reset ID)
def truncate_table_realisasi(supabase):
    """
    TRUNCATE table realisasi dan reset sequence ID ke 0
    """
    try:
        # Execute TRUNCATE via RPC (butuh function di Supabase)
        # Alternatif: Delete all + Reset sequence

        st.warning("🗑️ Menghapus semua data...")
        # Delete all records
        delete_result = supabase.table('realisasi').delete().neq('id', 0).execute()

        st.warning("🔄 Reset ID sequence ke 0...")
        # Reset sequence (via RPC call if available, or SQL)
        try:
            # This requires a custom RPC function in Supabase
            supabase.rpc('reset_realisasi_sequence').execute()
            st.success("✅ Sequence ID direset ke 0")
        except:
            st.info("ℹ️ Sequence reset mungkin perlu dilakukan manual via SQL: ALTER SEQUENCE realisasi_id_seq RESTART WITH 1;")

        return True
    except Exception as e:
        st.error(f"❌ Error truncate: {str(e)}")
        return False
