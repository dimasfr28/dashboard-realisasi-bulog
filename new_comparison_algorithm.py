import pandas as pd
from supabase import create_client, Client
import toml
from datetime import datetime
import os
import hashlib
import json
import time

class RealisasiCompareProcessor:
    def __init__(self, secrets_path="/home/dimas/bulog/dashboard-realisasi/.streamlit/secrets.toml"):
        """Initialize processor with Supabase connection and configuration"""
        # Load secrets
        secrets = toml.load(secrets_path)
        self.url = secrets['supabase']['project_url']
        self.key = secrets['supabase']['api_key']
        self.supabase: Client = create_client(self.url, self.key)

        # Configuration
        self.limit = 1000              # Records per page
        self.max_retries = 3           # Maximum retry attempts
        self.retry_delay = 2           # Base delay in seconds

    def generate_row_hash(self, record):
        """
        Generate SHA256 hash from record data for duplicate detection.
        Excludes auto-generated fields like id and created_at.
        """
        hash_data = {
            'kanwil_id': record.get('kanwil_id'),
            'kancab_id': record.get('kancab_id'),
            'lokasi_persediaan': record.get('lokasi_persediaan'),
            'id_pemasok': record.get('id_pemasok'),
            'nama_pemasok': record.get('nama_pemasok'),
            'tanggal_po': record.get('tanggal_po'),
            'nomor_po': record.get('nomor_po'),
            'produk': record.get('produk'),
            'no_jurnal': record.get('no_jurnal'),
            'no_in_out': record.get('no_in_out'),
            'tanggal_penerimaan': record.get('tanggal_penerimaan'),
            'komoditi': record.get('komoditi'),
            'spesifikasi': record.get('spesifikasi'),
            'tahun_stok': record.get('tahun_stok'),
            'tanggal_kirim_keuangan': record.get('tanggal_kirim_keuangan'),
            'jenis_transaksi': record.get('jenis_transaksi'),
            'akun_analitik': record.get('akun_analitik'),
            'jenis_pengadaan': record.get('jenis_pengadaan'),
            'satuan': record.get('satuan'),
            'uom_po': record.get('uom_po'),
            'kuantum_po_kg': record.get('kuantum_po_kg'),
            'qty_in_out': record.get('qty_in_out'),
            'harga_include_ppn': record.get('harga_include_ppn'),
            'nominal_realisasi_incl_ppn': record.get('nominal_realisasi_incl_ppn'),
            'status': record.get('status'),
        }

        json_string = json.dumps(hash_data, sort_keys=True, default=str)
        return hashlib.sha256(json_string.encode()).hexdigest()

    def reset_table_realisasi(self):
        """
        Reset realisasi table and its sequence using TRUNCATE
        """
        print(f"{'='*60}")
        print("🗑️  Reset tabel realisasi")
        print(f"{'='*60}\n")

        try:
            print("Mereset tabel realisasi dan sequence (TRUNCATE)...")
            self.supabase.rpc(
                "reset_table_sequence",
                {"p_table_name": "realisasi"}
            ).execute()
            print("✅ Tabel realisasi telah di-truncate dan sequence di-reset\n")
        except Exception as e:
            print(f"❌ Error saat reset: {e}")
            print("Mencoba alternatif method (manual delete)...\n")
            try:
                # Manual delete all rows
                self.supabase.table('realisasi').delete().neq('id', 0).execute()
                print("✅ Tabel berhasil dikosongkan menggunakan delete")
                print("⚠️  Perhatian: Sequence ID mungkin perlu direset manual\n")
            except Exception as e2:
                print(f"❌ Error pada alternatif method: {e2}\n")

    def migrate_to_realisasi_compare(self, excel_path):
        """
        Migrate data from Excel Export sheet to realisasi_compare table.
        """
        print("=" * 60)
        print("📥 Starting Migration to realisasi_compare")
        print("=" * 60 + "\n")

        # Read Excel
        print(f"📖 Reading Excel: {excel_path}")
        df = pd.read_excel(excel_path, sheet_name='Export')
        print(f"   ✅ Loaded {len(df)} rows from Export sheet\n")

        # Get mappings
        print("🔍 Fetching kanwil mapping...")
        kanwil_response = self.supabase.table('kanwil').select('*').execute()
        kanwil_mapping = {k['nama_kanwil']: k['kanwil_id'] for k in kanwil_response.data}
        print(f"   ✅ Found {len(kanwil_mapping)} kanwil records\n")

        print("🔍 Fetching kancab mapping...")
        kancab_response = self.supabase.table('kancab').select('*, kanwil!inner(nama_kanwil)').execute()
        kancab_mapping = {}
        for k in kancab_response.data:
            key = (k['kanwil']['nama_kanwil'], k['nama_kancab'])
            kancab_mapping[key] = k['kancab_id']
        print(f"   ✅ Found {len(kancab_mapping)} kancab records\n")

        # Clear table
        print("🗑️  Clearing realisasi_compare table...")
        try:
            self.supabase.table('realisasi_compare').delete().neq('id', 0).execute()
            print("   ✅ Table cleared\n")
        except Exception as e:
            print(f"   ⚠️  Error clearing table: {e}\n")

        # Migrate data
        print("📥 Migrating data to realisasi_compare...")
        realisasi_compare_data = []
        batch_size = 1000
        total_inserted = 0
        skipped_kanwil = 0
        skipped_kancab = 0

        for idx, row in df.iterrows():
            try:
                kanwil_name = str(row['kanwil']) if pd.notna(row['kanwil']) else None
                kancab_name = str(row['Entitas']) if pd.notna(row['Entitas']) else None

                kanwil_id = kanwil_mapping.get(kanwil_name)
                kancab_id = kancab_mapping.get((kanwil_name, kancab_name))

                if not kanwil_id:
                    skipped_kanwil += 1
                if not kancab_id and kancab_name:
                    skipped_kancab += 1

                tanggal_po = pd.to_datetime(row['Tanggal PO']).date() if pd.notna(row['Tanggal PO']) else None
                tanggal_penerimaan = pd.to_datetime(row['Tanggal Penerimaan']).date() if pd.notna(row['Tanggal Penerimaan']) else None
                tanggal_kirim = pd.to_datetime(row['Tanggal Kirim Keuangan']).date() if pd.notna(row['Tanggal Kirim Keuangan']) else None

                record = {
                    'kanwil_id': kanwil_id,
                    'kancab_id': kancab_id,
                    'lokasi_persediaan': str(row['Lokasi Persediaan']) if pd.notna(row['Lokasi Persediaan']) else None,
                    'id_pemasok': int(row['No. ID Pemasok']) if pd.notna(row['No. ID Pemasok']) else None,
                    'nama_pemasok': str(row['Nama Pemasok']) if pd.notna(row['Nama Pemasok']) else None,
                    'tanggal_po': tanggal_po.isoformat() if tanggal_po else None,
                    'nomor_po': str(row['Nomor PO']) if pd.notna(row['Nomor PO']) else None,
                    'produk': str(row['Produk']) if pd.notna(row['Produk']) else None,
                    'no_jurnal': str(row['No Jurnal']) if pd.notna(row['No Jurnal']) else None,
                    'no_in_out': str(row['Nomor IN / OUT']) if pd.notna(row['Nomor IN / OUT']) else None,
                    'tanggal_penerimaan': tanggal_penerimaan.isoformat() if tanggal_penerimaan else None,
                    'komoditi': str(row['Komoditi']) if pd.notna(row['Komoditi']) else None,
                    'spesifikasi': str(row['spesifikasi']) if pd.notna(row['spesifikasi']) else None,
                    'tahun_stok': int(row['Tahun Stok']) if pd.notna(row['Tahun Stok']) else None,
                    'tanggal_kirim_keuangan': tanggal_kirim.isoformat() if tanggal_kirim else None,
                    'jenis_transaksi': str(row['Jenis Transaksi']) if pd.notna(row['Jenis Transaksi']) else None,
                    'akun_analitik': str(row['Akun Analitik']) if pd.notna(row['Akun Analitik']) else None,
                    'jenis_pengadaan': str(row['Jenis Pengadaan']) if pd.notna(row['Jenis Pengadaan']) else None,
                    'satuan': str(row['Satuan']) if pd.notna(row['Satuan']) else None,
                    'uom_po': str(row['uom_po']) if pd.notna(row['uom_po']) else None,
                    'kuantum_po_kg': float(row['Kuantum PO (Kg)']) if pd.notna(row['Kuantum PO (Kg)']) else None,
                    'qty_in_out': float(row['In / Out']) if pd.notna(row['In / Out']) else None,
                    'harga_include_ppn': float(row['Harga Include ppn']) if pd.notna(row['Harga Include ppn']) else None,
                    'nominal_realisasi_incl_ppn': float(row['Nominal Realisasi Incl ppn']) if pd.notna(row['Nominal Realisasi Incl ppn']) else None,
                    'status': str(row['Status']) if pd.notna(row['Status']) else None,
                }

                record['row_hash'] = self.generate_row_hash(record)
                realisasi_compare_data.append(record)

                if len(realisasi_compare_data) >= batch_size:
                    self.supabase.table('realisasi_compare').insert(realisasi_compare_data).execute()
                    total_inserted += len(realisasi_compare_data)
                    print(f"   ✅ Inserted batch: {total_inserted} records")
                    realisasi_compare_data = []

            except Exception as e:
                print(f"   ⚠️  Error at row {idx}: {e}")
                continue

        if realisasi_compare_data:
            self.supabase.table('realisasi_compare').insert(realisasi_compare_data).execute()
            total_inserted += len(realisasi_compare_data)
            print(f"   ✅ Inserted final batch: {total_inserted} records")

        print(f"\n📊 Migration Summary:")
        print(f"   Total records inserted: {total_inserted}")
        print(f"   Skipped (kanwil not found): {skipped_kanwil}")
        print(f"   Skipped (kancab not found): {skipped_kancab}")
        print("\n✅ Migration to realisasi_compare completed\n")

    def migrate_to_realisasi_direct(self, excel_path):
        """
        Migrate data from Excel Export sheet directly to realisasi table.
        Used for REPLACE mode.
        """
        print("=" * 60)
        print("📥 Starting Direct Migration to realisasi (REPLACE MODE)")
        print("=" * 60 + "\n")

        # Read Excel
        print(f"📖 Reading Excel: {excel_path}")
        df = pd.read_excel(excel_path, sheet_name='Export')
        print(f"   ✅ Loaded {len(df)} rows from Export sheet\n")

        # Get mappings
        print("🔍 Fetching kanwil mapping...")
        kanwil_response = self.supabase.table('kanwil').select('*').execute()
        kanwil_mapping = {k['nama_kanwil']: k['kanwil_id'] for k in kanwil_response.data}
        print(f"   ✅ Found {len(kanwil_mapping)} kanwil records\n")

        print("🔍 Fetching kancab mapping...")
        kancab_response = self.supabase.table('kancab').select('*, kanwil!inner(nama_kanwil)').execute()
        kancab_mapping = {}
        for k in kancab_response.data:
            key = (k['kanwil']['nama_kanwil'], k['nama_kancab'])
            kancab_mapping[key] = k['kancab_id']
        print(f"   ✅ Found {len(kancab_mapping)} kancab records\n")

        # Reset realisasi table
        self.reset_table_realisasi()

        # Migrate data
        print("📥 Migrating data to realisasi...")
        realisasi_data = []
        batch_size = 1000
        total_inserted = 0
        skipped_kanwil = 0
        skipped_kancab = 0

        for idx, row in df.iterrows():
            try:
                kanwil_name = str(row['kanwil']) if pd.notna(row['kanwil']) else None
                kancab_name = str(row['Entitas']) if pd.notna(row['Entitas']) else None

                kanwil_id = kanwil_mapping.get(kanwil_name)
                kancab_id = kancab_mapping.get((kanwil_name, kancab_name))

                if not kanwil_id:
                    skipped_kanwil += 1
                if not kancab_id and kancab_name:
                    skipped_kancab += 1

                tanggal_po = pd.to_datetime(row['Tanggal PO']).date() if pd.notna(row['Tanggal PO']) else None
                tanggal_penerimaan = pd.to_datetime(row['Tanggal Penerimaan']).date() if pd.notna(row['Tanggal Penerimaan']) else None
                tanggal_kirim = pd.to_datetime(row['Tanggal Kirim Keuangan']).date() if pd.notna(row['Tanggal Kirim Keuangan']) else None

                record = {
                    'kanwil_id': kanwil_id,
                    'kancab_id': kancab_id,
                    'lokasi_persediaan': str(row['Lokasi Persediaan']) if pd.notna(row['Lokasi Persediaan']) else None,
                    'id_pemasok': int(row['No. ID Pemasok']) if pd.notna(row['No. ID Pemasok']) else None,
                    'nama_pemasok': str(row['Nama Pemasok']) if pd.notna(row['Nama Pemasok']) else None,
                    'tanggal_po': tanggal_po.isoformat() if tanggal_po else None,
                    'nomor_po': str(row['Nomor PO']) if pd.notna(row['Nomor PO']) else None,
                    'produk': str(row['Produk']) if pd.notna(row['Produk']) else None,
                    'no_jurnal': str(row['No Jurnal']) if pd.notna(row['No Jurnal']) else None,
                    'no_in_out': str(row['Nomor IN / OUT']) if pd.notna(row['Nomor IN / OUT']) else None,
                    'tanggal_penerimaan': tanggal_penerimaan.isoformat() if tanggal_penerimaan else None,
                    'komoditi': str(row['Komoditi']) if pd.notna(row['Komoditi']) else None,
                    'spesifikasi': str(row['spesifikasi']) if pd.notna(row['spesifikasi']) else None,
                    'tahun_stok': int(row['Tahun Stok']) if pd.notna(row['Tahun Stok']) else None,
                    'tanggal_kirim_keuangan': tanggal_kirim.isoformat() if tanggal_kirim else None,
                    'jenis_transaksi': str(row['Jenis Transaksi']) if pd.notna(row['Jenis Transaksi']) else None,
                    'akun_analitik': str(row['Akun Analitik']) if pd.notna(row['Akun Analitik']) else None,
                    'jenis_pengadaan': str(row['Jenis Pengadaan']) if pd.notna(row['Jenis Pengadaan']) else None,
                    'satuan': str(row['Satuan']) if pd.notna(row['Satuan']) else None,
                    'uom_po': str(row['uom_po']) if pd.notna(row['uom_po']) else None,
                    'kuantum_po_kg': float(row['Kuantum PO (Kg)']) if pd.notna(row['Kuantum PO (Kg)']) else None,
                    'qty_in_out': float(row['In / Out']) if pd.notna(row['In / Out']) else None,
                    'harga_include_ppn': float(row['Harga Include ppn']) if pd.notna(row['Harga Include ppn']) else None,
                    'nominal_realisasi_incl_ppn': float(row['Nominal Realisasi Incl ppn']) if pd.notna(row['Nominal Realisasi Incl ppn']) else None,
                    'status': str(row['Status']) if pd.notna(row['Status']) else None,
                }

                record['row_hash'] = self.generate_row_hash(record)
                realisasi_data.append(record)

                if len(realisasi_data) >= batch_size:
                    self.supabase.table('realisasi').insert(realisasi_data).execute()
                    total_inserted += len(realisasi_data)
                    print(f"   ✅ Inserted batch: {total_inserted} records")
                    realisasi_data = []

            except Exception as e:
                print(f"   ⚠️  Error at row {idx}: {e}")
                continue

        if realisasi_data:
            self.supabase.table('realisasi').insert(realisasi_data).execute()
            total_inserted += len(realisasi_data)
            print(f"   ✅ Inserted final batch: {total_inserted} records")

        print(f"\n📊 Migration Summary:")
        print(f"   Total records inserted: {total_inserted}")
        print(f"   Skipped (kanwil not found): {skipped_kanwil}")
        print(f"   Skipped (kancab not found): {skipped_kancab}")
        print("\n✅ Direct migration to realisasi completed (REPLACE MODE)\n")

    def get_min_id(self):
        """Get minimum ID from realisasi_compare table"""
        try:
            res = (
                self.supabase
                .table("realisasi_compare")
                .select("id")
                .order("id", desc=False)
                .limit(1)
                .execute()
            )
            if res.data:
                return res.data[0]["id"]
            return 0
        except Exception as e:
            print(f"Error mendapatkan min id: {e}")
            return 0

    def get_total_rows(self):
        """Get total number of rows in realisasi_compare table"""
        try:
            result = self.supabase.table("realisasi_compare").select("id", count="exact").limit(1).execute()
            return result.count
        except Exception as e:
            print(f"Error mendapatkan total rows: {e}")
            return None

    def fetch_with_retry(self, last_id, limit, retries=0):
        """
        Fetch data from RPC with automatic retry on timeout.
        Implements exponential backoff retry strategy.
        """
        try:
            res = self.supabase.rpc(
                "get_realisasi_compare_not_exists_page",
                {
                    "p_last_id": last_id,
                    "p_limit": limit
                }
            ).execute()
            return res.data
        except Exception as e:
            if retries < self.max_retries:
                delay = self.retry_delay * (2 ** retries)  # Exponential backoff
                print(f"   ⚠️  Error pada last_id={last_id}, retry ke-{retries + 1}/{self.max_retries}")
                print(f"   ⏳ Waiting {delay} seconds before retry...")
                time.sleep(delay)
                return self.fetch_with_retry(last_id, limit, retries + 1)
            else:
                print(f"   ❌ Gagal setelah {self.max_retries} retry pada last_id={last_id}: {e}")
                raise e

    def process_all_data(self):
        """
        Process all data from realisasi_compare using cursor-based pagination.
        Returns list of IDs that don't exist in realisasi table.
        """
        total_rows = self.get_total_rows()
        min_id = self.get_min_id()

        if total_rows is None:
            print("Tidak bisa mendapatkan total rows, melanjutkan tanpa batas")
            total_rows = float('inf')
        else:
            print(f"Total baris di realisasi_compare: {total_rows}")
            print(f"Starting from ID: {min_id}\n")

        last_id = min_id
        processed_count = 0
        all_results = []

        while True:
            print(f"Fetching data dengan last_id={last_id}...")

            try:
                data = self.fetch_with_retry(last_id, self.limit)

                if data and len(data) > 0:
                    print(f"   ✅ Mendapat {len(data)} baris data")
                    all_results.extend(data)
                    processed_count += len(data)

                    last_id = max([row['realisasi_compare_id'] for row in data])

                    if total_rows != float('inf'):
                        progress = (processed_count / total_rows) * 100
                        print(f"   📊 Progress: {processed_count}/{total_rows} ({progress:.1f}%)\n")
                    else:
                        print(f"   📊 Total processed: {processed_count}\n")

                else:
                    print(f"   ✅ Hasil kosong pada last_id={last_id}")

                    if total_rows != float('inf') and last_id >= (min_id + total_rows):
                        print("   ✅ Sudah mencapai akhir data")
                        break

                    # Try advancing last_id
                    last_id += self.limit

                    # Safety check to avoid infinite loop
                    if total_rows != float('inf') and last_id > (min_id + total_rows + self.limit * 10):
                        print("   ⚠️  Melewati batas maksimal, menghentikan iterasi")
                        break

            except Exception as e:
                print(f"   ❌ Error fatal yang tidak bisa di-retry: {e}")
                break

            time.sleep(0.2)  # Rate limiting

        print(f"\n{'='*60}")
        print(f"✅ Selesai! Total data yang perlu dimigrasi: {len(all_results)} baris")
        print(f"{'='*60}\n")
        return all_results

    def migrate_data_to_realisasi(self, df_results):
        """
        Migrate data from realisasi_compare to realisasi based on IDs in df_results
        """
        if df_results.empty:
            print("Tidak ada data untuk dimigrasi")
            return 0

        total_ids = len(df_results)
        print(f"\n{'='*60}")
        print(f"📥 Memulai Migrasi {total_ids} data ke tabel realisasi")
        print(f"{'='*60}\n")

        ids_to_migrate = df_results['realisasi_compare_id'].tolist()
        batch_size = 1000
        migrated_count = 0

        for i in range(0, len(ids_to_migrate), batch_size):
            batch_ids = ids_to_migrate[i:i + batch_size]
            batch_num = i // batch_size + 1

            try:
                print(f"Batch {batch_num}: Fetching {len(batch_ids)} records (IDs {batch_ids[0]} - {batch_ids[-1]})")

                response = (
                    self.supabase
                    .table("realisasi_compare")
                    .select("*")
                    .in_("id", batch_ids)
                    .execute()
                )

                data_to_insert = response.data

                if not data_to_insert:
                    print(f"   ⚠️  Tidak ada data untuk batch ini")
                    continue

                # Remove 'id' column (will be auto-generated in realisasi table)
                for row in data_to_insert:
                    if 'id' in row:
                        del row['id']

                print(f"   📤 Inserting {len(data_to_insert)} rows...")
                self.supabase.table("realisasi").insert(data_to_insert).execute()

                migrated_count += len(data_to_insert)
                progress = (migrated_count / total_ids) * 100
                print(f"   ✅ Berhasil! Total: {migrated_count}/{total_ids} ({progress:.1f}%)\n")

                time.sleep(0.5)  # Rate limiting

            except Exception as e:
                print(f"   ❌ Error pada batch {batch_num}: {e}\n")
                continue

        print(f"{'='*60}")
        print(f"✅ Migrasi Selesai: {migrated_count}/{total_ids} data berhasil")
        print(f"{'='*60}\n")
        return migrated_count

    def truncate_and_reset_realisasi_compare(self):
        """
        Truncate realisasi_compare table and reset sequence using TRUNCATE
        """
        print(f"{'='*60}")
        print("🗑️  Truncate & Reset realisasi_compare")
        print(f"{'='*60}\n")

        try:
            print("Menghapus semua data dari realisasi_compare (TRUNCATE)...")
            self.supabase.rpc(
                "reset_table_sequence",
                {"p_table_name": "realisasi_compare"}
            ).execute()
            print("✅ Tabel realisasi_compare telah di-truncate dan sequence di-reset\n")
        except Exception as e:
            print(f"❌ Error saat truncate: {e}")
            print("Mencoba alternatif method (manual delete)...\n")
            try:
                self.supabase.table('realisasi_compare').delete().neq('id', 0).execute()
                print("✅ Tabel berhasil dikosongkan menggunakan delete")
                print("⚠️  Perhatian: Sequence ID mungkin perlu direset manual\n")
            except Exception as e2:
                print(f"❌ Error pada alternatif method: {e2}\n")

    def run_full_process(self, excel_path, mode="append"):
        """
        Run complete process with two modes:
        - append: migrate Excel → compare → migrate to realisasi → cleanup (default)
        - replace: reset realisasi → migrate Excel directly to realisasi

        Args:
            excel_path: Path to Excel file
            mode: "append" or "replace" (default: "append")
        """
        print("\n" + "=" * 60)
        print(f"🚀 NEW COMPARISON ALGORITHM - MODE: {mode.upper()}")
        print("=" * 60 + "\n")

        if mode.lower() == "replace":
            # REPLACE MODE: Direct migration to realisasi with reset
            print("⚠️  WARNING: REPLACE mode akan menghapus semua data di tabel realisasi!")
            print("=" * 60 + "\n")

            # Direct migration to realisasi (includes reset)
            self.migrate_to_realisasi_direct(excel_path)

            print(f"{'='*60}")
            print(f"✅ PROCESS COMPLETED! (REPLACE MODE)")
            print(f"{'='*60}\n")

        else:
            # APPEND MODE: Original flow with comparison
            print("ℹ️  APPEND mode: hanya data baru yang akan ditambahkan")
            print("=" * 60 + "\n")

            # Step 1: Migrate Excel to realisasi_compare
            self.migrate_to_realisasi_compare(excel_path)

            # Step 2: Compare and get missing data
            print("Step 2: Mencari data yang perlu dimigrasi...")
            results = self.process_all_data()

            if results:
                df_results = pd.DataFrame(results)
                print(f"DataFrame shape: {df_results.shape}")
                if len(df_results) > 0:
                    print(f"Sample data:\n{df_results.head()}\n")

                # Step 3: Migrate to realisasi
                migrated = self.migrate_data_to_realisasi(df_results)

                # Step 4: Cleanup if migration successful
                if migrated > 0:
                    self.truncate_and_reset_realisasi_compare()

                print(f"{'='*60}")
                print(f"✅ PROCESS COMPLETED! (APPEND MODE)")
                print(f"{'='*60}")
                print(f"Total data dimigrasi: {migrated}")
                print(f"{'='*60}\n")
            else:
                print(f"{'='*60}")
                print("✅ Tidak ada data yang perlu dimigrasi")
                print("✅ Semua data di realisasi_compare sudah ada di realisasi")
                print(f"{'='*60}\n")


# Main execution
if __name__ == "__main__":
    excel_file = "/home/dimas/bulog/dashboard-realisasi/assets/hasil_gabungan (3).xlsx"

    processor = RealisasiCompareProcessor()

    # Pilih mode: "append" atau "replace"
    # - "append": Hanya menambahkan data baru yang belum ada (default)
    # - "replace": Reset tabel realisasi dan input ulang semua data dari Excel

    mode = input("Pilih mode (append/replace): ").strip().lower()
    # Ubah ke "replace" untuk mode replace

    processor.run_full_process(excel_file, mode=mode)
