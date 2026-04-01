# ArasiyalAayvu

Tamil Nadu election awareness platform with:
- `backend` Python data pipelines (scrape/transform/upload to Firestore)
- `web` Next.js frontend

## 1) Run Backend (Python pipeline)

From project root:

```bash
cd /Users/jv/Documents/MyProjects/ArasiyalAayvu
source .venv/bin/activate
pip install -r requirements.txt
```

Run a task:

```bash
python main.py --task awareness       # socio + accountability
python main.py --task manifesto       # manifesto seed upload
python main.py --task accountability   # MyNeta accountability pipeline
python main.py --task socio            # socio-economics pipeline
python main.py --task all              # political history full pipeline
```

## 2) Run Frontend (with hot reload)

```bash
cd /Users/jv/Documents/MyProjects/ArasiyalAayvu/web
npm install
npm run dev:hot
```

Notes:
- `npm run dev` also supports Fast Refresh.
- `npm run dev:hot` enables polling-based watching (`CHOKIDAR_USEPOLLING`, `WATCHPACK_POLLING`) for more reliable reloads on some environments.
