# Endymion-AI Frontend

A minimal React web UI demonstrating the "Cow Card" use case with real-time sync status monitoring.

## 🎯 Features

### Pages

1. **Cow List** (`/`)
   - Shows all cows from SQL projection
   - Indicates sync status (green = synced, yellow = pending)
   - Click cow to view detailed Cow Card
   - Auto-refreshes every 2 seconds

2. **Cow Card** (`/cow/:id`)
   - **Left side**: Cow details from SQL projection
     - Current attributes (breed, birth_date, sex, status)
     - Edit breed functionality
     - Mark as sold action
   - **Right side**: Analytics and event history
     - Herd composition from Gold Delta Lake (queried via DuckDB, 10-50ms)
     - Lifecycle tracking with direct Gold queries
     - Event log showing all cow_events with sync status
   - Actions:
     - Edit breed → emits `cow_updated` event
     - Mark as sold → emits `cow_deactivated` event
   - Disabled during pending sync

3. **Create Cow Form** (`/create`)
   - Input: breed, birth_date, sex
   - Submit → POST `/api/cows`
   - Shows "Creating..." with event ID
   - Auto-redirects when synced (~3s)

### UI Features

- **Sync Status Bar** (all pages)
  - Prominent sync lag display
  - Last synced timestamp (e.g., "15 seconds ago")
  - Pending events count
  - Total rows synced
  - Color-coded status: 🟢 Healthy (<60s) | 🟡 Acceptable (<5m) | 🔴 Behind (>5m)

- **Real-time Updates**
  - Polls every 2 seconds for latest data
  - Shows event_id for each pending change
  - Disables edit buttons if sync pending

- **Event Log**
  - Shows Bronze events for each cow
  - Color-coded: Green (synced) | Yellow (pending)
  - Displays event_id, type, and timestamp

## 🚀 Quick Start

### Prerequisites

- Node.js 18+ and npm
- Backend API running at `http://localhost:8000`

### Installation

```bash
cd frontend

# Install dependencies
npm install

# Start dev server
npm run dev
```

The app will be available at **http://localhost:3000**

### Build for Production

```bash
npm run build
npm run preview
```

## 📁 Project Structure

```
frontend/
├── src/
│   ├── components/
│   │   ├── CowList.jsx          # Main cattle inventory page
│   │   ├── CowCard.jsx          # Detailed cow view with analytics
│   │   ├── CreateCowForm.jsx    # Add new cow form
│   │   └── SyncStatusBar.jsx    # Real-time sync status indicator
│   ├── services/
│   │   └── api.js               # API client with polling support
│   ├── App.jsx                  # Main app with routing
│   ├── main.jsx                 # Entry point
│   └── index.css                # Global styles
├── index.html
├── vite.config.js               # Vite configuration with proxy
└── package.json
```

## 🔌 API Integration

The frontend communicates with the backend API:

### Endpoints Used

- `GET /api/cows` - List all cows (operational.cows SQL projection)
- `GET /api/cows/:id` - Get single cow details
- `POST /api/cows` - Create new cow (emits event)
- `PUT /api/cows/:id/breed` - Update cow breed (emits event)
- `DELETE /api/cows/:id` - Deactivate cow (emits event)
- `GET /api/v1/analytics/herd-composition` - Get herd analytics (DuckDB queries Gold Delta, 10-50ms)
- `GET /api/v1/analytics/cow/:id` - Get cow lifecycle (queries event history directly, <50ms)
- `GET /api/sync/status` - Get sync status and lag
- `GET /api/events/cow/:id` - Get cow event history
- `GET /api/events/unpublished` - Get unpublished events count

**Analytics Architecture:**
```
Gold Delta Lake (canonical) → DuckDB (direct query) → FastAPI → Frontend
                ↓                        ↓                   ↓
         Canonical Truth           No projection        10-50ms response
```

### Polling Strategy

The app uses a polling service that:
- Polls every 2 seconds for real-time updates
- Maintains separate polling intervals per page
- Cleans up intervals on component unmount
- Handles errors gracefully with fallbacks

## 🎨 UI Components

### SyncStatusBar

Shows system-wide sync health:
```jsx
<SyncStatusBar />
```

Features:
- Pulsing dot indicator (color-coded)
- Last sync timestamp
- Sync lag in seconds
- Pending events count
- Total synced rows

### CowList

Grid of cow cards with:
- Breed, sex, birth date
- Active/inactive status
- Sync status badge
- Click to view details

### CowCard

Two-column layout:
- **Details panel**: Editable attributes
- **Analytics panel**: Charts and event log

### CreateCowForm

Form with:
- Breed input (with quick-select buttons)
- Birth date picker
- Sex selector
- Real-time creation feedback
- Behind-the-scenes info

## 🎯 Sync Status Indicators

The UI prominently displays sync lag:

| Status | Color | Lag | Description |
|--------|-------|-----|-------------|
| 🟢 Healthy | Green | <60s | Normal operation |
| 🟡 Acceptable | Yellow | 60s-5m | Slightly behind |
| 🔴 Behind | Red | >5m | Needs attention |

## 🔧 Configuration

### Vite Proxy

The dev server proxies API requests to avoid CORS:

```javascript
// vite.config.js
server: {
  port: 3000,
  proxy: {
    '/api': {
      target: 'http://localhost:8000',
      changeOrigin: true,
    }
  }
}
```

### API Base URL

For production, update the API base URL:

```javascript
// src/services/api.js
const API_BASE = process.env.VITE_API_BASE || '/api';
```

## 🎬 Demo Flow

1. **Start Backend**:
   ```bash
   # Terminal 1: API server
   python backend/api/main.py
   
   # Terminal 2: Sync scheduler
   python backend/sync/sync_scheduler.py
   ```

2. **Start Frontend**:
   ```bash
   # Terminal 3: Frontend dev server
   cd frontend
   npm run dev
   ```

3. **Use the App**:
   - Visit http://localhost:3000
   - Click "Add Cow" to create a new cow
   - Watch the sync status bar update
   - Click a cow to view details
   - Edit breed or mark as sold
   - Observe event log in real-time

## 📊 Event Flow Visualization

The UI helps visualize the data flow:

```
                    Write Path (Operational)
USER ACTION → API → EVENT STORE → BRONZE → SILVER → SQL (operational.cows) → UI UPDATE
                                                                                    ↑
                                                                              (polling)

                    Analytics Path (Read-Only)
           SILVER → GOLD (Delta Lake) → DuckDB → FastAPI → UI (10-50ms)
                         ↓                   ↓
                  Canonical Truth       Direct Query
```

- User clicks "Save" → API creates event
- Frontend shows "Creating..." with event_id
- Polls every 2s until cow appears in SQL projection
- Sync status bar shows lag and pending events
- Event log shows history with sync status

## 🚀 Production Deployment

### Build

```bash
npm run build
```

This creates an optimized production build in `dist/`.

### Serve Static Files

Option 1: Nginx
```nginx
server {
    listen 3000;
    root /path/to/frontend/dist;
    
    location / {
        try_files $uri $uri/ /index.html;
    }
    
    location /api {
        proxy_pass http://backend:8000;
    }
}
```

Option 2: Docker
```dockerfile
FROM node:18 AS builder
WORKDIR /app
COPY package*.json ./
RUN npm ci
COPY . .
RUN npm run build

FROM nginx:alpine
COPY --from=builder /app/dist /usr/share/nginx/html
COPY nginx.conf /etc/nginx/conf.d/default.conf
```

## 🧪 Testing

### Manual Testing Checklist

- [ ] Cow list loads and displays all cows
- [ ] Sync status bar shows correct lag
- [ ] Creating a cow shows "Creating..." state
- [ ] Cow details page shows correct data
- [ ] Editing breed emits event and updates
- [ ] Marking as sold deactivates cow
- [ ] Event log shows correct history
- [ ] Polling updates data automatically
- [ ] UI disables actions during pending sync

### Test with Demo Script

```bash
# Run backend demo first
./demo/run_demo.sh

# Then access frontend
# You should see 3 cows (Bessie, Thunder, Daisy)
```

## 🎨 Styling

The app uses a minimal CSS system:

- **Design System**: CSS variables for colors
- **Layout**: CSS Grid and Flexbox
- **Components**: Card-based design
- **Responsive**: Mobile-friendly breakpoints
- **Colors**: Blue (primary), Green (success), Yellow (warning), Red (danger)

No external CSS framework required!

## 🔮 Future Enhancements

### Implemented
✅ Polling for real-time updates
✅ Sync status monitoring
✅ Event log display
✅ Edit and deactivate actions

### Potential Additions
- [ ] WebSocket for real-time sync notifications
- [ ] Bronze event log in dev tools (browser extension)
- [ ] Advanced filtering and search
- [ ] Bulk operations
- [ ] Export to CSV
- [ ] Mobile app (React Native)

## 📝 Notes

### Why Polling?

- Simple to implement
- No server-side WebSocket infrastructure needed
- Good enough for demo purposes
- 2-second interval balances freshness vs load

### Why No State Management?

- Small app with simple state
- React hooks (useState, useEffect) sufficient
- Polling keeps UI in sync with backend
- No need for Redux/MobX complexity

### Why Recharts?

- Lightweight charting library
- React-native support
- Easy to use and customize
- Perfect for simple line charts

## 🐛 Troubleshooting

### CORS Errors

Make sure the backend has CORS enabled:
```python
# backend/api/main.py
from fastapi.middleware.cors import CORSMiddleware

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
```

### API Not Found (404)

Check Vite proxy configuration:
```bash
# Verify backend is running
curl http://localhost:8000/api/cows

# Check proxy in vite.config.js
```

### Polling Not Working

Open browser console (F12) to check for errors:
- Network tab: Verify API calls every 2s
- Console tab: Check for JavaScript errors

## 📚 Learn More

- [Vite Documentation](https://vitejs.dev/)
- [React Router](https://reactrouter.com/)
- [Recharts](https://recharts.org/)
- [Event Sourcing Pattern](../ARCHITECTURE.md)

## 🎉 Success!

You now have a working web UI that demonstrates:
- Event sourcing with real-time feedback
- Sync status monitoring
- Eventual consistency visualization
- CQRS read/write separation
- Complete audit trail (event log)

Perfect for stakeholder demos! 🚀
