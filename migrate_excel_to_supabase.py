import pandas as pd
from supabase import create_client, Client
import toml
from datetime import datetime
import os
import hashlib
import json

class SupabaseDataImporter:
    def __init__(self, secrets_path="/home/dimas/bulog/dashboard-realisasi/.streamlit/secrets.toml"):
        # Load secrets
        secrets = toml.load(secrets_path)
        self.url = secrets['supabase']['project_url']
        self.key = secrets['supabase']['api_key']
        self.supabase: Client = create_client(self.url, self.key)

    def generate_row_hash(self, record):
        """
        Generate SHA256 hash from record data for duplicate detection.
        Excludes auto-generated fields like id and created_at.
        """
        # Create a sorted dictionary to ensure consistent hashing
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

        # Convert to JSON string with sorted keys for consistent hashing
        json_string = json.dumps(hash_data, sort_keys=True, default=str)

        # Generate SHA256 hash
        return hashlib.sha256(json_string.encode()).hexdigest()
        
    def truncate_all_tables(self):
        """Truncate semua tabel dengan urutan yang benar"""
        print("🗑️  Truncating tables...")
        
        tables = ['realisasi', 'target_kancab', 'target_kanwil', 'kancab', 'kanwil']
        
        for table in tables:
            try:
                # Delete semua data
                response = self.supabase.table(table).delete().neq('id', 0).execute()
                print(f"   ✅ Truncated: {table}")
            except Exception as e:
                print(f"   ⚠️  Error truncating {table}: {e}")
        
        print("✅ All tables truncated\n")
    
    def import_kanwil(self, df):
        """Import data kanwil (unique)"""
        print("📥 Importing Kanwil...")
        
        # Extract unique kanwil dari kolom pertama
        kanwil_unique = df.iloc[:, 0].unique()
        
        kanwil_data = []
        for kanwil in kanwil_unique:
            if pd.notna(kanwil):
                kanwil_data.append({'nama_kanwil': str(kanwil)})
        
        if kanwil_data:
            response = self.supabase.table('kanwil').insert(kanwil_data).execute()
            print(f"   ✅ Inserted {len(kanwil_data)} kanwil records\n")
            return response.data
        return []
    
    def import_kancab(self, df):
        """Import data kancab dengan relasi ke kanwil"""
        print("📥 Importing Kancab...")
        
        # Get kanwil mapping
        kanwil_response = self.supabase.table('kanwil').select('*').execute()
        kanwil_mapping = {k['nama_kanwil']: k['kanwil_id'] for k in kanwil_response.data}
        
        # Extract unique kancab per kanwil
        # Asumsikan kolom ke-2 adalah Entitas (Kancab)
        kancab_unique = df.iloc[:, [0, 1]].drop_duplicates()
        
        kancab_data = []
        for _, row in kancab_unique.iterrows():
            kanwil_name = str(row.iloc[0])
            kancab_name = str(row.iloc[1])
            
            if pd.notna(kanwil_name) and pd.notna(kancab_name) and kanwil_name in kanwil_mapping:
                kancab_data.append({
                    'nama_kancab': kancab_name,
                    'kanwil_id': kanwil_mapping[kanwil_name]
                })
        
        if kancab_data:
            response = self.supabase.table('kancab').insert(kancab_data).execute()
            print(f"   ✅ Inserted {len(kancab_data)} kancab records\n")
            return response.data
        return []
    
    def import_target_kanwil(self, df):
        """Import data target kanwil dengan relasi ke kanwil"""
        print("📥 Importing Target Kanwil...")

        # Get kanwil mapping
        kanwil_response = self.supabase.table('kanwil').select('*').execute()
        kanwil_mapping = {k['nama_kanwil']: k['kanwil_id'] for k in kanwil_response.data}

        target_kanwil_data = []
        current_date = datetime.now().date()

        for idx, row in df.iterrows():
            try:
                # Kolom pertama: kanwil, kolom kedua: Target Setara Beras
                kanwil_name = str(row.iloc[0]).strip() if pd.notna(row.iloc[0]) else None
                target_value = str(row.iloc[1]).strip() if pd.notna(row.iloc[1]) else None

                # Skip jika kanwil kosong atau tidak ditemukan
                if not kanwil_name or kanwil_name == '':
                    continue

                kanwil_id = kanwil_mapping.get(kanwil_name)

                if kanwil_id and target_value:
                    record = {
                        'kanwil_id': kanwil_id,
                        'target_setara_beras': target_value,
                        'date': current_date.isoformat()
                    }
                    target_kanwil_data.append(record)
                else:
                    print(f"   ⚠️  Kanwil not found: {kanwil_name}")

            except Exception as e:
                print(f"   ⚠️  Error at row {idx}: {e}")
                continue

        if target_kanwil_data:
            response = self.supabase.table('target_kanwil').insert(target_kanwil_data).execute()
            print(f"   ✅ Inserted {len(target_kanwil_data)} target kanwil records\n")
            return response.data
        else:
            print(f"   ⚠️  No target kanwil data to insert\n")
        return []

    def import_target_kancab(self, df):
        """Import data target kancab dengan relasi ke kancab"""
        print("📥 Importing Target Kancab...")

        # Get kancab mapping
        kancab_response = self.supabase.table('kancab').select('*').execute()
        kancab_mapping = {k['nama_kancab']: k['kancab_id'] for k in kancab_response.data}

        target_kancab_data = []
        current_date = datetime.now().date()

        for idx, row in df.iterrows():
            try:
                # Kolom pertama: kancab, kolom kedua: Target Setara Beras
                kancab_name = str(row.iloc[0]).strip() if pd.notna(row.iloc[0]) else None
                target_value = str(row.iloc[1]).strip() if pd.notna(row.iloc[1]) else None

                # Skip jika kancab kosong
                if not kancab_name or kancab_name == '':
                    continue

                kancab_id = kancab_mapping.get(kancab_name)

                if kancab_id and target_value:
                    record = {
                        'kancab_id': kancab_id,
                        'target_setara_beras': target_value,
                        'date': current_date.isoformat()
                    }
                    target_kancab_data.append(record)
                else:
                    print(f"   ⚠️  Kancab not found: {kancab_name}")

            except Exception as e:
                print(f"   ⚠️  Error at row {idx}: {e}")
                continue

        if target_kancab_data:
            response = self.supabase.table('target_kancab').insert(target_kancab_data).execute()
            print(f"   ✅ Inserted {len(target_kancab_data)} target kancab records\n")
            return response.data
        else:
            print(f"   ⚠️  No target kancab data to insert\n")
        return []

    def import_realisasi(self, df):
        """Import data realisasi dengan relasi ke kanwil dan kancab"""
        print("📥 Importing Realisasi...")

        # Get kanwil mapping
        kanwil_response = self.supabase.table('kanwil').select('*').execute()
        kanwil_mapping = {k['nama_kanwil']: k['kanwil_id'] for k in kanwil_response.data}

        # Get kancab mapping with kanwil
        kancab_response = self.supabase.table('kancab').select('*, kanwil!inner(nama_kanwil)').execute()
        kancab_mapping = {}
        for k in kancab_response.data:
            key = (k['kanwil']['nama_kanwil'], k['nama_kancab'])
            kancab_mapping[key] = k['kancab_id']

        realisasi_data = []
        batch_size = 1000

        for idx, row in df.iterrows():
            try:
                kanwil_name = str(row.iloc[0]) if pd.notna(row.iloc[0]) else None
                kancab_name = str(row.iloc[1]) if pd.notna(row.iloc[1]) else None

                kanwil_id = kanwil_mapping.get(kanwil_name)
                kancab_id = kancab_mapping.get((kanwil_name, kancab_name))

                # Parse tanggal
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

                # Generate and add row hash
                record['row_hash'] = self.generate_row_hash(record)

                realisasi_data.append(record)

                # Insert per batch
                if len(realisasi_data) >= batch_size:
                    self.supabase.table('realisasi').insert(realisasi_data).execute()
                    print(f"   ✅ Inserted batch: {len(realisasi_data)} records")
                    realisasi_data = []

            except Exception as e:
                print(f"   ⚠️  Error at row {idx}: {e}")
                continue

        # Insert remaining data
        if realisasi_data:
            self.supabase.table('realisasi').insert(realisasi_data).execute()
            print(f"   ✅ Inserted final batch: {len(realisasi_data)} records")

        print(f"✅ Realisasi import completed\n")
    
    def run_full_import(self, excel_path):
        """Jalankan full import process"""
        print("=" * 60)
        print("🚀 Starting Full Import Process")
        print("=" * 60 + "\n")

        # Read Excel - semua sheets
        print(f"📖 Reading Excel: {excel_path}")

        # Read sheet realisasi (sheet pertama atau default)
        df_realisasi = pd.read_excel(excel_path)
        print(f"   ✅ Loaded {len(df_realisasi)} rows from Realisasi sheet\n")

        # Read sheet Target Kanwil
        try:
            df_target_kanwil = pd.read_excel(excel_path, sheet_name='Target Kanwil')
            print(f"   ✅ Loaded {len(df_target_kanwil)} rows from Target Kanwil sheet\n")
        except Exception as e:
            print(f"   ⚠️  Could not read Target Kanwil sheet: {e}\n")
            df_target_kanwil = None

        # Read sheet Target Kancab
        try:
            df_target_kancab = pd.read_excel(excel_path, sheet_name='Target Kancab')
            print(f"   ✅ Loaded {len(df_target_kancab)} rows from Target Kancab sheet\n")
        except Exception as e:
            print(f"   ⚠️  Could not read Target Kancab sheet: {e}\n")
            df_target_kancab = None

        # Truncate
        self.truncate_all_tables()

        # Import in order (penting: kanwil dan kancab dulu)
        self.import_kanwil(df_realisasi)
        self.import_kancab(df_realisasi)

        # Import target kanwil jika sheet tersedia
        if df_target_kanwil is not None:
            self.import_target_kanwil(df_target_kanwil)

        # Import target kancab jika sheet tersedia
        if df_target_kancab is not None:
            self.import_target_kancab(df_target_kancab)

        # Import realisasi terakhir
        self.import_realisasi(df_realisasi)

        print("=" * 60)
        print("✅ Import Process Completed!")
        print("=" * 60)


# Usage
if __name__ == "__main__":
    # Path ke file Excel
    excel_file = "/home/dimas/bulog/dashboard-realisasi/assets/hasil_gabungan_ori.xlsx"
    
    # Initialize importer
    importer = SupabaseDataImporter()
    
    # Run import
    importer.run_full_import(excel_file)