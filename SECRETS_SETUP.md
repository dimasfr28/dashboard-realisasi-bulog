# Setup Streamlit Secrets

## Development (Local)

1. Buat file `.streamlit/secrets.toml` di root project:
```toml
# Google Sheets API Configuration
[google_sheets]
api_key = "YOUR_API_KEY_HERE"
spreadsheet_url = "YOUR_SPREADSHEET_URL_HERE"
```

2. **PENTING**: File `secrets.toml` sudah ada di `.gitignore`, jangan commit file ini ke Git!

## Deployment (Streamlit Cloud)

1. Deploy aplikasi Anda ke Streamlit Community Cloud
2. Buka dashboard aplikasi Anda
3. Klik icon **⚙️ Settings** → **Secrets**
4. Paste isi file `secrets.toml` Anda:

```toml
[google_sheets]
api_key = "YOUR_API_KEY_HERE"
spreadsheet_url = "YOUR_SPREADSHEET_URL_HERE"
```

5. Klik **Save**

## Cara Kerja

Aplikasi menggunakan `st.secrets` untuk membaca konfigurasi:
- **Local development**: Baca dari `.streamlit/secrets.toml`
- **Production (Cloud)**: Baca dari Streamlit Cloud secrets management

## Keamanan

✅ File `secrets.toml` tidak akan ter-commit ke repository
✅ Credentials aman di Streamlit Cloud
✅ Tidak perlu hardcode API keys di code

## Troubleshooting

Jika mendapat error "Missing secrets":
1. Pastikan file `.streamlit/secrets.toml` ada (local)
2. Pastikan secrets sudah di-setup di Streamlit Cloud (production)
3. Pastikan format TOML benar (gunakan double quotes)
