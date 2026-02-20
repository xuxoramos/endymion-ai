# Endymion-AI Frontend - Quick Reference

## 🚀 Quick Start

```bash
cd frontend
npm install
npm run dev
```

Visit: **http://localhost:3000**

## 📁 File Structure

```
frontend/
├── src/
│   ├── components/
│   │   ├── CowList.jsx          # Main list page
│   │   ├── CowCard.jsx          # Detail page with analytics
│   │   ├── CreateCowForm.jsx    # Create new cow
│   │   └── SyncStatusBar.jsx    # Sync status indicator
│   ├── services/
│   │   └── api.js               # API client with polling
│   ├── App.jsx                  # Main app + routing
│   ├── main.jsx                 # Entry point
│   └── index.css                # Global styles
├── index.html
├── vite.config.js
├── package.json
├── start.sh                     # Quick start script
└── README.md
```

## 🎯 Pages

| Route | Component | Description |
|-------|-----------|-------------|
| `/` | CowList | Cattle inventory with sync status |
| `/cow/:id` | CowCard | Cow details + analytics + event log |
| `/create` | CreateCowForm | Add new cow form |

## 🔌 API Endpoints Used

```javascript
// Cows
GET    /api/cows              // List all cows
GET    /api/cows/:id          // Get cow details
POST   /api/cows              // Create cow
PUT    /api/cows/:id/breed    // Update breed
DELETE /api/cows/:id          // Deactivate cow

// Analytics (SQL Server projection, <100ms)
GET    /api/v1/analytics/herd-composition  // Herd analytics (21-76ms)
GET    /api/v1/analytics/cow/:id           // Lifecycle analytics

// Sync
GET    /api/sync/status       // Sync lag and status

// Events
GET    /api/events/cow/:id    // Cow event history
GET    /api/events/unpublished // Pending events count
```

## ⚡ Key Features

### Real-time Polling (2-second intervals)
```javascript
// Automatically polls for updates
polling.startCowsPolling(callback);
polling.startSyncPolling(callback);
polling.startCowPolling(cowId, callback);
```

### Sync Status Bar
- 🟢 **Healthy**: <60s lag
- 🟡 **Acceptable**: 60s-5m lag
- 🔴 **Behind**: >5m lag

### Event Actions
1. **Create Cow** → `cow_created` event
2. **Edit Breed** → `cow_updated` event
3. **Mark as Sold** → `cow_deactivated` event

### UI States
- ✅ **Synced**: Green badge, actions enabled
- ⏳ **Pending**: Yellow badge, actions disabled
- 🔄 **Creating**: Loading spinner with event_id

## 🎨 Components

### SyncStatusBar
```jsx
<SyncStatusBar />
```
Shows: sync lag, last sync time, pending events, total synced

### CowList
```jsx
<CowList />
```
Features: Grid layout, auto-refresh, click to view details

### CowCard
```jsx
<CowCard />
```
Features: Two-column layout, edit actions, analytics chart, event log

### CreateCowForm
```jsx
<CreateCowForm />
```
Features: Form validation, breed quick-select, creation feedback

## 🧩 API Service

```javascript
import { cowApi, syncApi, eventsApi, polling } from './services/api';

// Create cow
const cow = await cowApi.createCow({
  breed: 'Holstein',
  birth_date: '2024-01-15',
  sex: 'Female'
});

// Update breed
await cowApi.updateCowBreed(cowId, 'Angus');

// Deactivate
await cowApi.deactivateCow(cowId);

// Get analytics (DuckDB queries Gold Delta, 10-50ms)
const herdComp = await cowApi.getHerdComposition(tenantId, date);
const lifecycle = await cowApi.getCowLifecycle(cowId);

// Get sync status
const status = await syncApi.getSyncStatus();

// Get events
const events = await eventsApi.getCowEvents(cowId);
```

## 🎬 Demo Flow

1. **Start Backend**:
   ```bash
   # Terminal 1
   python backend/api/main.py
   
   # Terminal 2
   python backend/sync/sync_scheduler.py
   ```

2. **Start Frontend**:
   ```bash
   # Terminal 3
   cd frontend
   ./start.sh
   ```

3. **Use the App**:
   - Visit http://localhost:3000
   - Create a cow → watch sync status
   - Click cow → view details
   - Edit breed → see event log update
   - Observe sync lag in real-time

## 📊 Sync Status Explanation

| Metric | Description | Source |
|--------|-------------|--------|
| Sync Lag | Seconds since last sync | `sync.sync_state` table |
| Pending Events | Unpublished events | `events.cow_events` WHERE published=FALSE |
| Total Synced | Rows synced to SQL | `sync.sync_state.total_rows_synced` |
| Sync Interval | Scheduler frequency | 30 seconds (hardcoded) |

## 🔧 Configuration

### Change Polling Interval

```javascript
// src/services/api.js
startPolling(key, callback, interval = 2000) // Change 2000 to desired ms
```

### Change API Base URL

```javascript
// src/services/api.js
const API_BASE = '/api'; // Change for production
```

### Change Port

```javascript
// vite.config.js
server: {
  port: 3000, // Change port here
}
```

## 🐛 Troubleshooting

### Backend Not Running
```bash
# Check if API is accessible
curl http://localhost:8000/api/cows

# Start backend
python backend/api/main.py
```

### CORS Errors
Add CORS middleware to backend:
```python
from fastapi.middleware.cors import CORSMiddleware

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
```

### Polling Not Working
Check browser console (F12):
- Network tab: Should see requests every 2s
- Console: Check for errors

### npm install Fails
```bash
# Clear cache
npm cache clean --force
rm -rf node_modules package-lock.json
npm install
```

## 📦 Dependencies

- **react** 18.2.0 - UI library
- **react-dom** 18.2.0 - React DOM renderer
- **react-router-dom** 6.20.0 - Routing
- **recharts** 2.10.3 - Charts for analytics
- **vite** 5.0.8 - Build tool
- **@vitejs/plugin-react** 4.2.1 - React plugin for Vite

## 🚀 Production Build

```bash
# Build
npm run build

# Preview
npm run preview

# Output: dist/ directory
```

## 💡 Tips

1. **Use browser DevTools** to see polling requests
2. **Watch the sync status bar** to understand lag
3. **Create multiple cows** to see list behavior
4. **Edit and watch** event log update in real-time
5. **Open multiple tabs** to see polling in action

## 🎉 Success Indicators

✅ Frontend loads at http://localhost:3000
✅ Sync status bar shows green/yellow/red indicator
✅ Can create new cow and see it appear
✅ Can edit cow breed and see event in log
✅ Can mark cow as sold
✅ Analytics chart shows weight trend
✅ Event log shows history with sync status
✅ UI updates automatically every 2 seconds

---

**Ready for stakeholder demos!** 🐄✨
