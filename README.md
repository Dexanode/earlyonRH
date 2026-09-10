# earlyonRH — Robinhood Onchain Listener

Prototype read-only untuk chain 4663. Menemukan event Pons V1/V2 dan Uniswap V4 langsung dari RPC, menyimpan receipt dan timeline ke SQLite. WebSocket `newHeads` membangunkan collector; `eth_getLogs` mengambil rentang lengkap dan menangani recovery. Ini belum dashboard, trading bot, atau klaim keunggulan latency.

## Jalankan lokal / VPS

Memerlukan Python 3.11+ dan endpoint Robinhood mainnet. Dari folder ini:

```sh
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
cp .env.example .env
# Edit .env dengan endpoint provider. Hapus RPC_WS_URL jika hanya HTTP.
set -a
. ./.env
set +a
.venv/bin/python listener.py once --chunk 1
.venv/bin/python listener.py run
```

Jangan kirim API key melalui chat atau commit `.env`. Program tidak mencetak URL provider. Public RPC dapat menolak request atau mengembalikan rate limit; gunakan provider dengan kuota dan kemampuan `eth_getLogs`, receipts, block reads, serta WebSocket yang diuji. Listener tidak memerlukan private key wallet.

Awal tanpa `--start-block` dimulai dekat head saat pertama dijalankan; bukan backfill genesis. Restart memakai cursor database. Untuk sejarah tertentu, gunakan database baru:

```sh
.venv/bin/python listener.py run --db data/replay.sqlite --start-block 50000000 --chunk 10
```

Angka block di atas hanya contoh input, bukan deployment block resmi. Pilih block sebelum transaksi yang ingin diamati. Riwayat pool/curve yang lahir sebelum start tidak otomatis masuk watchlist. Registry atau decoder berubah → database lama ditolak; replay dengan database baru agar tidak mencampur cakupan.

## Output

```sh
.venv/bin/python listener.py export --output data/timeline.json
.venv/bin/python -m unittest -v
```

Database default `data/listener.sqlite` berisi events, receipts, watches, validations, blocks, serta meta/status/cursor. JSON export menampilkan decoded fields, raw logs, timestamps dan link transaksi explorer. Nilai uint/int menjadi string agar tidak kehilangan presisi di JavaScript. V4 menggunakan PoolId 32 byte; tidak dianggap token address. Export belum melakukan penggabungan relasi V4 pool ke token pada UI.

`observed_at` adalah waktu respons log diterima collector, bukan waktu pertama publikasi onchain. `event_timestamp` berasal dari block. Data backfill tidak membuktikan bahwa collector lebih cepat dari screener.

## Event yang didukung

| Kontrak | Discovery / aktivitas |
|---|---|
| Pons V1 factory | TokenLaunched, lalu Swap pada pool V3 anak |
| Pons V2 factory | TokenLaunched dan LaunchSwept |
| Curve V2 yang ditemukan | CurveBuy, CurveSell, CurveBuyRefunded, CurveCompleted |
| Uniswap V4 PoolManager | Initialize, ModifyLiquidity, Swap |

Launch dan pembelian curve pada transaksi/blok yang sama diambil dalam satu batch. Event lain pada alamat terpantau disimpan Unknown; malformed ABI disimpan DecodeError, kecuali factory decode failure yang menghentikan cursor agar child discovery tidak hilang. Buyer/sender/recipient dipertahankan sebagai label ABI, bukan otomatis pemilik ekonomis atau smart wallet. Repeat-buy conviction, harga USD, quote exit, hook accounting dan paper trading belum dihitung.

V1 tidak mendecode LP mint/burn. V4 liquidityDelta adalah unit likuiditas, bukan USD atau jumlah token yang disetor. Prototype membaca seluruh event PoolManager, sehingga provider berkuota kecil dapat cepat menjadi bottleneck; penyaringan pool aktif adalah optimasi berikutnya. Batas default 500 log per batch menahan commit dan mengecilkan rentang jika terlalu ramai. Satu blok di atas batas akan berhenti/degraded hingga konfigurasi/kode ditinjau, bukan melewatkannya diam-diam.

## Konsistensi dan recovery

- Chain diperiksa saat startup; bytecode harus nonkosong di tiga alamat registry. Hash bytecode disimpan dan perubahan pada restart ditolak. Ini **belum** verifikasi compiled runtime atau audit kontrak; proxy implementation juga tidak divalidasi oleh pemeriksaan ini.
- Receipt sukses harus memuat log yang sama. Parent/block hash diperiksa sebelum commit. Event, watches dan cursor ditulis dalam transaksi SQLite yang sama.
- Primary key transaction hash + log index mencegah duplikasi pada chain tunggal. Reorg menghapus event, receipt dan watches setelah ancestor yang masih cocok. Reorg melewati batas 64 block yang dicari akan berhenti untuk replay eksplisit.
- Dua confirmation block adalah buffer operasional, bukan finality Ethereum. Bisa diubah dengan `--confirmations`; reorg tetap mungkin.
- HTTP retry terbatas, interval request dan chunk dapat disetel. Cursor tidak maju saat pembacaan gagal. Lihat meta.status, last_success dan last_error untuk kesehatan; last_error adalah histori dan bisa tetap ada setelah pulih.
- WebSocket reconnect otomatis dan HTTP polling tetap aktif. Subscription adalah pemicu, bukan satu-satunya sumber data, sehingga putusnya WebSocket tidak membuang rentang block.

## Docker pada VPS

```sh
cp .env.example .env
# Isi endpoint provider di .env
docker compose up --build -d
docker compose logs -f --tail=50 listener
docker compose exec listener python listener.py export
```

SQLite berada di named volume `listener_data`. Backup volume/database dan pantau pertumbuhan disk. Jangan jalankan dua writer untuk database yang sama. Container tidak mengekspos port. Dockerfile/Compose disediakan untuk deployment berikutnya; pengujian utama dilakukan pada Python lokal, bukan Docker/VPS.

## Validasi source

Subset ABI Pons dicocokkan dengan repository publik pada commit `0445a2c39df623169dc4cf3a939308ef4e0f7a60`:

- https://github.com/ponsdotdev/ponsfamily/blob/0445a2c39df623169dc4cf3a939308ef4e0f7a60/contractsV1/src/PonsLaunchFactory.sol
- https://github.com/ponsdotdev/ponsfamily/blob/0445a2c39df623169dc4cf3a939308ef4e0f7a60/contractsV2/src/v2/PonsV2LaunchFactory.sol
- https://github.com/ponsdotdev/ponsfamily/blob/0445a2c39df623169dc4cf3a939308ef4e0f7a60/contractsV2/src/v2/PonsV2BondingCurve.sol
- https://github.com/Uniswap/v4-core/blob/main/src/interfaces/IPoolManager.sol
- https://developers.uniswap.org/docs/protocols/v4/deployments

Hash source Pons dan signature event ada pada `source-manifest.json`. Source ABI bukan bukti kecocokan deployment; keberhasilan decode receipt nyata masih harus dibuktikan pada provider yang stabil. Test fixtures dibuat khusus dan tidak diklaim sebagai trade mainnet. Lihat `VALIDATION.md` untuk hasil tes terbaru.


## Clone dan update di VPS

Pertama kali:

```sh
git clone https://github.com/Dexanode/earlyonRH.git
cd earlyonRH
cp .env.example .env
# Edit .env dengan RPC mainnet milik sendiri.
docker compose up --build -d
```

Update berikutnya, dari folder repository:

```sh
git pull --ff-only
docker compose up --build -d
docker compose logs --tail=50 listener
```

File `.env` tidak dilacak Git dan data SQLite berada di named volume Docker. Jangan gunakan `docker compose down -v` jika ingin mempertahankan data. Jika update mengubah registry/decoder, ikuti instruksi replay dengan database baru; jangan menghapus database lama secara otomatis.
