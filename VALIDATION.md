# Validation

Tanggal: 10 September 2026.

## Hasil

Suite Python lokal: **15 tests passed**, Python 3.13.5, websockets 15.0.1, pycryptodome 3.23.0.

Cakupan tes:

- Keccak Ethereum dicocokkan dengan topic ERC-20 Transfer yang diketahui; topic Pons V1 dicocokkan dengan dokumentasi.
- Uint besar, signed ticks dan liquidity delta dipertahankan tanpa kehilangan presisi.
- Log malformed ditolak, unknown event tidak ditebak.
- Launch factory dan buy curve pada block/transaction yang sama ditemukan bersama.
- Duplikasi dan restart tidak menggandakan event; start checkpoint lama dipertahankan.
- Receipt hilang/tidak cocok tidak memajukan cursor atau mendaftarkan child watch.
- Reorg menghapus event, receipt dan watch yang orphan; reorg tanpa ancestor dihentikan.
- Chain yang salah ditolak.
- Export memiliki bukti transaksi serta string integer presisi penuh.
- Server WebSocket localhost benar-benar menjalankan handshake, disconnect, reconnect, subscription dan head notification.

Tes socket awal dibatasi sandbox lokal; dijalankan ulang dengan akses localhost dan lulus. Pengujian WebSocket adalah integrasi lokal, bukan provider mainnet.

## Smoke test mainnet

Percobaan `listener.py once --chunk 1 --request-spacing 2` pada `https://rpc-robinhood.hoodmarket.io` berhenti dengan `eth_chainId failed (403)`. Tidak ada event mainnet yang berhasil di-commit oleh prototype dalam percobaan ini. Hasil audit RPC pada sesi sebelumnya tidak dipakai sebagai bukti keberhasilan collector ini.

Berikut masih belum terverifikasi: receipt event mainnet yang didekode end-to-end, subscription provider, throughput berkelanjutan, latency versus screener, deployment Docker/VPS, dan runtime bytecode match terhadap compiled source.

## Batas fungsi

Prototype menyediakan event timeline dan persistence. Belum ada dashboard web, NFT/o1 adapter, classification smart wallet, conviction score, quote exit, USD valuation, atau eksekusi transaksi. File `example-timeline.json` adalah **fixture sintetis** dengan hash transaksi placeholder; bukan market data.

Pons source ABI dicocokkan dengan commit yang dicantumkan dalam README dan source-manifest. Daftar tiga alamat tetap perlu validasi deployment pada RPC yang stabil. Startup hanya memeriksa chain, keberadaan bytecode dan perubahan hash dibanding startup sebelumnya.
