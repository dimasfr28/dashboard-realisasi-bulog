# Row Hash Documentation

## Overview

Kolom `row_hash` telah ditambahkan ke tabel `realisasi` dan `realisasi_compare` untuk memudahkan deteksi duplikasi dan komparasi data.

## Implementasi

### Hash Generation

Hash dibuat menggunakan algoritma **SHA256** dari semua field data (kecuali `id`, `created_at`, dan `row_hash` itu sendiri).

**Fungsi:** `generate_row_hash(record)`

**Lokasi:**
- [migrate_excel_to_supabase.py](migrate_excel_to_supabase.py#L17-L55) (line 17-55)
- [new_comparison_algorithm.py](new_comparison_algorithm.py#L17-L55) (line 17-55)

### Fields yang Di-hash

Semua field berikut digunakan untuk membuat hash:
1. kanwil_id
2. kancab_id
3. lokasi_persediaan
4. id_pemasok
5. nama_pemasok
6. tanggal_po
7. nomor_po
8. produk
9. no_jurnal
10. no_in_out
11. tanggal_penerimaan
12. komoditi
13. spesifikasi
14. tahun_stok
15. tanggal_kirim_keuangan
16. jenis_transaksi
17. akun_analitik
18. jenis_pengadaan
19. satuan
20. uom_po
21. kuantum_po_kg
22. qty_in_out
23. harga_include_ppn
24. nominal_realisasi_incl_ppn
25. status

## Karakteristik Hash

- **Algoritma:** SHA256
- **Panjang:** 64 karakter (hexadecimal)
- **Konsistensi:** Hash yang sama selalu dihasilkan untuk data yang identik
- **Deterministik:** Menggunakan `sort_keys=True` pada JSON serialization

## Contoh Hash

```python
# Record
{
    'kanwil_id': 1,
    'kancab_id': 2,
    'lokasi_persediaan': 'GUDANG A',
    'nama_pemasok': 'PT ABC',
    'status': 'done',
    # ... other fields
}

# Menghasilkan hash:
# "ab93facff8d02936948e1231cb2f50d5e8e649c1e23b74c3bfd1132f95de6e75"
```

## Kegunaan

### 1. Deteksi Duplikasi

Dengan index pada kolom `row_hash`, Anda bisa dengan cepat mendeteksi data duplikat:

```sql
-- Cari duplikat berdasarkan hash
SELECT row_hash, COUNT(*) as jumlah
FROM realisasi
GROUP BY row_hash
HAVING COUNT(*) > 1;
```

### 2. Komparasi Data

Membandingkan data antara dua tabel menjadi lebih cepat:

```sql
-- Cari data di realisasi_compare yang tidak ada di realisasi
SELECT rc.*
FROM realisasi_compare rc
LEFT JOIN realisasi r ON rc.row_hash = r.row_hash
WHERE r.row_hash IS NULL;
```

### 3. Data Integrity Check

Verifikasi apakah data berubah setelah migrasi:

```python
# Generate hash sebelum insert
original_hash = generate_row_hash(record)

# Insert data
insert_record(record)

# Query kembali dan compare
db_record = fetch_record(id)
db_hash = generate_row_hash(db_record)

assert original_hash == db_hash, "Data integrity error!"
```

## Database Schema

Pastikan kolom `row_hash` sudah ada di tabel:

```sql
-- Untuk tabel realisasi
ALTER TABLE realisasi ADD COLUMN IF NOT EXISTS row_hash TEXT;

-- Untuk tabel realisasi_compare
ALTER TABLE realisasi_compare ADD COLUMN IF NOT EXISTS row_hash TEXT;

-- Tambahkan index untuk performa
CREATE INDEX IF NOT EXISTS idx_realisasi_row_hash ON realisasi(row_hash);
CREATE INDEX IF NOT EXISTS idx_realisasi_compare_row_hash ON realisasi_compare(row_hash);
```

## Migration Workflow

### File: migrate_excel_to_supabase.py

1. Baca data dari Excel
2. Parse semua field
3. **Generate hash** menggunakan `generate_row_hash()`
4. Insert ke database dengan kolom `row_hash`

### File: new_comparison_algorithm.py

1. Migrate data dari Excel ke `realisasi_compare`
2. **Generate hash** untuk setiap record
3. Compare menggunakan RPC atau hash-based comparison

## Testing

Test konsistensi hash generation:

```bash
python3 -c "
from migrate_excel_to_supabase import SupabaseDataImporter
from new_comparison_algorithm import RealisasiCompareProcessor

importer = SupabaseDataImporter()
processor = RealisasiCompareProcessor()

test_record = {'kanwil_id': 1, 'status': 'done'}
hash1 = importer.generate_row_hash(test_record)
hash2 = processor.generate_row_hash(test_record)

assert hash1 == hash2, 'Hash mismatch!'
print('✅ Hash generation consistent')
"
```

## Performance Considerations

### Pros:
- Query duplikasi sangat cepat dengan index
- Tidak perlu compare semua kolom satu per satu
- Hash fixed-length (64 char) lebih cepat untuk indexing

### Cons:
- Butuh storage tambahan ~64 bytes per row
- Hash generation menambah waktu insert (minimal)

### Recommended Indexes:

```sql
-- Single column index untuk lookup cepat
CREATE INDEX idx_realisasi_row_hash ON realisasi(row_hash);
CREATE INDEX idx_realisasi_compare_row_hash ON realisasi_compare(row_hash);

-- Composite index jika sering query dengan kanwil_id
CREATE INDEX idx_realisasi_kanwil_hash ON realisasi(kanwil_id, row_hash);
```

## Troubleshooting

### Hash Tidak Match Padahal Data Sama

Kemungkinan penyebab:
1. **Type conversion issue**: Pastikan tipe data konsisten (int vs float, str vs None)
2. **Date format**: Pastikan tanggal di-convert ke ISO format string
3. **Whitespace**: Trim whitespace dari string fields
4. **Null handling**: Pastikan None/NULL di-handle konsisten

### Hash Berubah Setelah Update

Ini **normal** karena hash mencerminkan seluruh record. Jika ada field yang berubah, hash akan berubah.

## Best Practices

1. **Generate hash sebelum insert**: Selalu generate hash sebelum insert ke database
2. **Jangan modify hash manually**: Hash harus selalu auto-generated
3. **Re-generate after update**: Jika data di-update, re-generate hash-nya
4. **Use hash for comparison**: Gunakan hash untuk quick comparison, bukan sebagai primary key

## Future Enhancements

Beberapa enhancement yang bisa ditambahkan:
1. Trigger otomatis untuk update hash saat data berubah
2. Hash-based incremental sync
3. Hash validation pada application level
4. Audit trail menggunakan hash history
