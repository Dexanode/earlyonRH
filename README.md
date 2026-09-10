# earlyonRH — Robinhood Onchain Listener

Prototype read-only untuk chain 4663, dengan dashboard radar, detail kandidat, dan data health. Listener menemukan event Pons V1/V2 dan Uniswap V4 langsung dari RPC, menyimpan receipt dan timeline ke SQLite. WebSocket `newHeads` membangunkan collector; `eth_getLogs` mengambil rentang lengkap dan menangani recovery. Ini bukan trading bot atau klaim keunggulan latency.

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

Database default `data/listener.sqlite` berisi events, receipts, watches, validations, blocks, serta meta/status/cursor. JSON export menampilkan decoded fields, raw logs, timestamps dan link transaksi explorer. Nilai uint/int menjadi string agar tidak kehilangan presisi di JavaScript. V4 menggunakan PoolId 32 byte; tidak dianggap token address. Radar menampilkan V4 sebagai PoolId; penggabungan pool-token lintas protokol belum tersedia.

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

SQLite berada di named volume `listener_data`. Backup volume/database dan pantau pertumbuhan disk. Jangan jalankan dua writer untuk database yang sama. Dashboard hanya membuka port pada loopback VPS (127.0.0.1:8080). Dockerfile/Compose disediakan untuk deployment berikutnya; pengujian utama dilakukan pada Python lokal, bukan Docker/VPS.

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


## Dashboard: radar, detail kandidat, data health

Setelah `git pull --ff-only` dan `docker compose up --build -d`, service `dashboard` membaca SQLite listener dari volume yang sama dalam mode read-only. Tidak membutuhkan API key dan tidak menulis transaksi. Database kosong tampil sebagai waiting; tidak ada data demo otomatis.

Dari laptop, buka SSH tunnel (ganti USER dan IP_VPS):

```sh
ssh -N -L 8080:127.0.0.1:8080 USER@IP_VPS
```

Buka http://localhost:8080 di browser laptop. Jika port laptop terpakai, ganti bagian pertama menjadi `8081` dan buka http://localhost:8081. Jangan ubah binding menjadi port publik tanpa autentikasi/reverse proxy yang sesuai; dashboard ini dirancang untuk akses pribadi lewat tunnel.

Pemeriksaan di VPS:

```sh
docker compose ps
docker compose logs --tail=50 listener dashboard
curl http://127.0.0.1:8080/api/radar
```

Radar menampilkan maksimum 5.000 event terbaru, dikelompokkan menurut token Pons atau PoolId V4. Search/filter bekerja dalam sampel ini; angka bukan total sepanjang sejarah. Klik alamat untuk membuka detail dan link transaksi. Detail menampilkan 100 event per halaman. Buy/sell yang terhitung adalah event CurveBuy/CurveSell; swap DEX tidak ditebak sebagai pembelian tanpa atribusi.

Data Health menampilkan heartbeat, checkpoint, head terakhir, jarak block, commit terakhir, reorg, dan error historis. Lebih dari 60 detik tanpa heartbeat ditandai stale; jarak lebih dari 100 block ditandai catching-up. Ambang ini indikator operasional, bukan finality atau pengukuran laba. Auto-refresh setiap 10 detik saat tab aktif. Jika API gagal, data terakhir tetap terlihat dengan label koneksi terputus.

Jalankan tanpa Docker:

```sh
.venv/bin/python dashboard.py --db data/listener.sqlite
```

Dashboard tersedia pada http://127.0.0.1:8080. Listener dan dashboard memakai schema yang sama; update ini tidak mengubah fingerprint ABI sehingga database lama tetap dapat digunakan. Index tambahan dibuat otomatis oleh listener untuk query radar.
# Catch-up performance

## Live radar and preserved history

`listener-live` now runs `stream.py`: direct WebSocket `logs` subscriptions for
Pons/curve/V3 signatures plus `newHeads`. Only registered factories and discovered
children are written as candidates. Other matching signatures are discarded.
Logs are held for three observed heads and labelled
`provider-stream-confirmed-3-heads`; they are not independently header/receipt
verified. A streamed header hash is checked when the provider supplies that exact
header. `removed=true` and observed replacements trigger rollback.

Reconnect gaps and late-launch overlaps use a separate, paced HTTP recovery loop
(at most one ten-block range per second; 15-second backoff on failure). The API
reports `recovery_blocks` independently of live head lag. Rate limits in recovery
do not stop live subscriptions. Pending logs survive process restarts. The
historical database and the existing live database are preserved on upgrade.
Subscriptions still consume provider quota; this is request reduction, not an
unlimited-free guarantee.

## Budgeted wallet and contract screening

`enricher` examines only the ten most active recent assets. It caches transaction
sender attribution and refreshes contract screening after six hours. The default
hard budget is 1,000 HTTP calls per UTC day (`ENRICHMENT_DAILY_RPC_BUDGET`). Calls
count against the budget even when the provider fails, preventing retry storms.

Screening records runtime bytecode, standard owner/getOwner and paused responses,
total supply, deployer balance, and EIP-1967 implementation/admin storage slots.
Unsupported methods stay unknown and earn no safety points. This does not prove
that mint, blacklist, tax, upgrade, or transfer restrictions are safe and is not a
source-code audit. Transaction senders are classified as direct or routed relative
to the event actor; this is attribution evidence, not proof of independent control.

Dashboard score v2 exposes activity, safety, unique sender count, routed share,
conviction, and verdict. Conviction is absent until contract screening exists.

Default Compose starts `listener-live` and points the dashboard to `data/live.sqlite`.
On its first run the live database starts near the current head; subsequent restarts
record downtime gaps separately while receiving current events. It scans Pons factories and
their discovered curve/V3 children. Global V4 swaps, pre-start launches, and NFT
marketplaces are not included in this live scope.

The old `data/listener.sqlite` stays intact. Historical collection is opt-in via
the `history` profile and should remain stopped when sharing a constrained RPC.
For migration, run `docker compose stop listener` before
`docker compose up --build -d listener-live dashboard`. Do not remove the volume.
Historical and live records are intentionally separate; neither represents full
chain or wallet history. No scoring should assume complete historical coverage.

The collector batches block headers and transaction receipts, and scans the full
watch list in one log filter (splitting only when rejected). Defaults retain the
provider-compatible ten-block log range and 50 ms request spacing. HTTP 429 causes
backoff without shrinking the range or fanning out into individual reads.

Logs include per-stage timings. Compare cursor and head changes over the same
interval: `healthy` indicates successful collection, not that the data is live.
An increasing lag requires further provider/throughput investigation; waiting alone
does not solve it. Existing databases resume their checkpoint after upgrades.
