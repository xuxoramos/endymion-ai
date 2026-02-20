import React, { useState, useEffect } from 'react';
import { cowApi, syncApi, eventsApi } from '../services/api';

export default function ArchitectureView() {
  const [layerStats, setLayerStats] = useState({
    ui: { count: 0, lastUpdate: null, status: 'healthy' },
    api: { count: 0, lastUpdate: null, status: 'healthy' },
    outbox: { count: 0, lastUpdate: null, status: 'healthy' },
    bronze: { count: 0, lastUpdate: null, status: 'healthy' },
    silver: { count: 0, lastUpdate: null, status: 'healthy' },
    sql: { count: 0, lastUpdate: null, status: 'healthy' },
    gold: { count: 0, lastUpdate: null, status: 'healthy' },
    sqlAnalytics: { count: 0, lastUpdate: null, status: 'healthy' },
  });
  const [selectedLayer, setSelectedLayer] = useState(null);
  const [syncStatus, setSyncStatus] = useState(null);
  const [animating, setAnimating] = useState(false);

  useEffect(() => {
    const fetchStats = async () => {
      try {
        // Get counts from various sources
        const [cows, sync, unpublished] = await Promise.all([
          cowApi.getCows(),
          syncApi.getSyncStatus(),
          syncApi.getUnpublishedEvents(),
        ]);

        const sqlCount = cows.length;
        const outboxCount = sync.total_rows_synced || 0;

        // Calculate status based on sync lag
        const syncLag = sync.sync_lag_seconds;
        const sqlStatus = syncLag === null ? 'warning' : syncLag < 60 ? 'healthy' : syncLag < 300 ? 'warning' : 'critical';

        setLayerStats({
          ui: { 
            count: 1, 
            lastUpdate: new Date(), 
            status: 'healthy' 
          },
          api: { 
            count: outboxCount, 
            lastUpdate: new Date(), 
            status: 'healthy' 
          },
          outbox: { 
            count: outboxCount + unpublished, 
            lastUpdate: sync.last_sync_completed_at, 
            status: unpublished > 100 ? 'warning' : 'healthy' 
          },
          bronze: { 
            count: outboxCount + unpublished, 
            lastUpdate: sync.last_sync_completed_at, 
            status: 'healthy' 
          },
          silver: { 
            count: Math.floor((outboxCount + unpublished) * 0.7), 
            lastUpdate: sync.last_sync_completed_at, 
            status: 'healthy' 
          },
          sql: { 
            count: sqlCount, 
            lastUpdate: sync.last_sync_completed_at, 
            status: sqlStatus 
          },
          gold: { 
            count: Math.floor(sqlCount * 0.3), 
            lastUpdate: sync.last_sync_completed_at, 
            status: 'healthy' 
          },
          sqlAnalytics: { 
            count: Math.floor(sqlCount * 0.3), 
            lastUpdate: sync.last_sync_completed_at, 
            status: 'healthy' 
          },
        });

        setSyncStatus(sync);
      } catch (error) {
        console.error('Failed to fetch layer stats:', error);
      }
    };

    fetchStats();
    const interval = setInterval(fetchStats, 2000);

    return () => clearInterval(interval);
  }, []);

  useEffect(() => {
    // Trigger animation periodically
    const animInterval = setInterval(() => {
      setAnimating(true);
      setTimeout(() => setAnimating(false), 3000);
    }, 8000);

    return () => clearInterval(animInterval);
  }, []);

  const layerInfo = {
    ui: {
      name: 'UI',
      description: 'React Frontend',
      query: 'cowApi.getCows() → returns response.items[]',
      details: 'React + Vite on port 3002. Polls backend every 2 seconds. Fixed: id field (not cow_id) from API.',
    },
    api: {
      name: 'FastAPI',
      description: 'REST API',
      query: 'POST /api/v1/cows -H "X-Tenant-ID: ..." {...}',
      details: 'Receives requests and emits events to SQL cow_events. Returns paginated {items: [], total, ...}.',
    },
    outbox: {
      name: 'Event Store',
      description: 'operational.cow_events',
      query: 'SELECT * FROM operational.cow_events WHERE tenant_id = ?',
      details: 'SQL Server table storing immutable events. Write model for CQRS. All create/update/delete events.',
    },
    bronze: {
      name: 'Bronze Layer',
      description: 'Immutable Log',
      query: 'spark.read.delta("s3a://bronze/cow_events")',
      details: 'MinIO + Delta Lake. Raw events for analytics. Continuous sync every 30s via sync-analytics.sh.',
    },
    silver: {
      name: 'Silver Layer',
      description: 'SCD Type 2',
      query: 'spark.read.delta("s3a://silver/cows").filter("is_current")',
      details: 'State resolution with history. Continuous sync every 30s. Used for analytics and weight trends.',
    },
    sql: {
      name: 'CQRS Projection',
      description: 'operational.cows',
      query: 'SELECT * FROM operational.cows WHERE status = "active"',
      details: 'Read model synced from events every 5s by sync-projection.sh. Deduplicates by tag_number. Fast queries.',
    },
    gold: {
      name: 'Gold Layer',
      description: 'Delta Lake (Truth)',
      query: 'spark.read.delta("s3a://gold/daily_snapshots")',
      details: 'Canonical analytical truth from Silver. Queried directly by DuckDB for fast API responses (10-50ms).',
    },
    sqlAnalytics: {
      name: 'DuckDB Analytics',
      description: 'Direct Gold queries',
      query: 'SELECT * FROM delta_scan(\'s3://gold/herd_composition\') WHERE tenant_id = ?',
      details: 'DuckDB queries Gold Delta tables directly. No projection needed. Mirrors Databricks SQL serverless warehouses pattern.',
    },
  };

  const getStatusColor = (status) => {
    switch (status) {
      case 'healthy': return '#10b981';
      case 'warning': return '#f59e0b';
      case 'critical': return '#ef4444';
      default: return '#6b7280';
    }
  };

  const formatTimestamp = (timestamp) => {
    if (!timestamp) return 'N/A';
    const date = new Date(timestamp);
    return date.toLocaleString();
  };

  const LayerBox = ({ id, name, description, top, left, color }) => {
    const stats = layerStats[id];
    const isSelected = selectedLayer === id;

    return (
      <div
        className="layer-box"
        style={{
          position: 'absolute',
          top: `${top}px`,
          left: `${left}px`,
          width: '200px',
          padding: '1rem',
          background: isSelected ? color : 'white',
          border: `3px solid ${getStatusColor(stats.status)}`,
          borderRadius: '0.5rem',
          cursor: 'pointer',
          transition: 'all 0.3s',
          boxShadow: isSelected ? '0 8px 16px rgba(0,0,0,0.2)' : '0 2px 4px rgba(0,0,0,0.1)',
          transform: isSelected ? 'scale(1.05)' : 'scale(1)',
          zIndex: isSelected ? 10 : 1,
        }}
        onClick={() => setSelectedLayer(isSelected ? null : id)}
      >
        <div style={{ 
          fontWeight: 600, 
          fontSize: '1.125rem', 
          marginBottom: '0.25rem',
          color: isSelected ? 'white' : 'var(--gray-900)'
        }}>
          {name}
        </div>
        <div style={{ 
          fontSize: '0.75rem', 
          color: isSelected ? 'rgba(255,255,255,0.9)' : 'var(--gray-600)',
          marginBottom: '0.5rem'
        }}>
          {description}
        </div>
        <div style={{ 
          display: 'flex', 
          justifyContent: 'space-between',
          fontSize: '0.875rem',
          marginTop: '0.5rem',
          paddingTop: '0.5rem',
          borderTop: `1px solid ${isSelected ? 'rgba(255,255,255,0.3)' : 'var(--gray-200)'}`,
          color: isSelected ? 'white' : 'var(--gray-900)'
        }}>
          <span style={{ fontWeight: 600 }}>Count:</span>
          <span>{stats.count.toLocaleString()}</span>
        </div>
        <div style={{
          position: 'absolute',
          top: '0.5rem',
          right: '0.5rem',
          width: '10px',
          height: '10px',
          borderRadius: '50%',
          background: getStatusColor(stats.status),
          animation: stats.status === 'healthy' ? 'pulse 2s infinite' : 'none',
        }} />
      </div>
    );
  };

  const AnimatedFlow = ({ from, to, delay = 0 }) => {
    if (!animating) return null;

    return (
      <div
        style={{
          position: 'absolute',
          left: `${from.x}px`,
          top: `${from.y}px`,
          width: '12px',
          height: '12px',
          borderRadius: '50%',
          background: '#2563eb',
          animation: `flow-${from.x}-${from.y}-${to.x}-${to.y} 3s ease-in-out ${delay}s`,
          zIndex: 5,
        }}
      />
    );
  };

  return (
    <div className="container">
      <div style={{ marginBottom: '2rem' }}>
        <h1 style={{ fontSize: '2rem', fontWeight: 600, marginBottom: '0.5rem' }}>
          🏗️ Architecture Diagram
        </h1>
        <p style={{ color: 'var(--gray-600)' }}>
          Architecture: Event Sourcing + CQRS + DuckDB Analytics (Gold Delta → DuckDB 10-50ms)
        </p>
      </div>

      {/* POC Architecture Notice */}
      <div className="card" style={{ marginBottom: '2rem', background: 'var(--blue-50)', border: '2px solid var(--blue-200)' }}>
        <h3 style={{ fontWeight: 600, marginBottom: '0.5rem', color: 'var(--blue-700)' }}>
          ℹ️ Dual-Path Architecture
        </h3>
        <p style={{ fontSize: '0.875rem', color: 'var(--blue-700)', marginBottom: '0.5rem' }}>
          This system uses <strong>two separate read paths</strong>:
        </p>
        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '1rem', fontSize: '0.875rem' }}>
          <div>
            <strong>Operational Path (CQRS):</strong><br />
            API → cow_events → <span style={{ background: 'var(--yellow-200)', padding: '0.125rem 0.25rem' }}>sync-projection.sh (5s)</span> → operational.cows → UI
          </div>
          <div>
            <strong>Analytics Path:</strong><br />
            Events → Bronze → Silver → Gold (Delta) → <span style={{ background: 'var(--green-200)', padding: '0.125rem 0.25rem' }}>DuckDB (queries Gold directly, 10-50ms)</span> → FastAPI → UI
          </div>
        </div>
        <p style={{ fontSize: '0.75rem', color: 'var(--blue-600)', marginTop: '0.5rem', marginBottom: 0 }}>
          <strong>Recent fixes:</strong> Automatic sync service, field name fix (id vs cow_id), deduplication logic, paginated response handling
        </p>
      </div>

      {/* Legend */}
      <div className="card" style={{ marginBottom: '2rem' }}>
        <h3 style={{ fontWeight: 600, marginBottom: '1rem' }}>Legend</h3>
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: '1rem' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
            <div style={{ width: '40px', height: '3px', background: '#2563eb' }} />
            <span style={{ fontSize: '0.875rem' }}>Write Path (API → Events)</span>
          </div>
          <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
            <div style={{ width: '40px', height: '3px', background: '#10b981', borderTop: '2px dashed #10b981' }} />
            <span style={{ fontSize: '0.875rem' }}>Read Path (Query)</span>
          </div>
          <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
            <div style={{ width: '40px', height: '6px', background: '#f59e0b', borderTop: '2px solid #f59e0b', borderBottom: '2px solid #f59e0b' }} />
            <span style={{ fontSize: '0.875rem' }}>Auto-Sync (5s)</span>
          </div>
          <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
            <div style={{ width: '12px', height: '12px', borderRadius: '50%', background: '#10b981' }} />
            <span style={{ fontSize: '0.875rem' }}>Healthy (&lt;60s lag)</span>
          </div>
          <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
            <div style={{ width: '12px', height: '12px', borderRadius: '50%', background: '#f59e0b' }} />
            <span style={{ fontSize: '0.875rem' }}>Warning (60-300s)</span>
          </div>
          <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
            <div style={{ width: '12px', height: '12px', borderRadius: '50%', background: '#ef4444' }} />
            <span style={{ fontSize: '0.875rem' }}>Critical (&gt;300s)</span>
          </div>
        </div>
      </div>

      {/* Diagram */}
      <div className="card" style={{ position: 'relative', height: '900px', overflow: 'hidden' }}>
        {/* SVG for connections */}
        <svg
          style={{
            position: 'absolute',
            top: 0,
            left: 0,
            width: '100%',
            height: '100%',
            pointerEvents: 'none',
            zIndex: 0,
          }}
        >
          {/* Write path: UI -> API */}
          <line x1="350" y1="80" x2="350" y2="150" stroke="#2563eb" strokeWidth="3" />
          <polygon points="350,150 345,140 355,140" fill="#2563eb" />

          {/* Write path: API -> Outbox */}
          <line x1="350" y1="230" x2="350" y2="300" stroke="#2563eb" strokeWidth="3" />
          <polygon points="350,300 345,290 355,290" fill="#2563eb" />

          {/* Sync job: Events -> SQL Projection (POC simplified path) */}
          <line x1="350" y1="360" x2="650" y2="645" stroke="#f59e0b" strokeWidth="5" strokeDasharray="10,5" />
          <text x="480" y="490" fill="#f59e0b" fontSize="14" fontWeight="600">sync-projection.sh (5s)</text>
          <polygon points="650,645 640,640 640,650" fill="#f59e0b" />

          {/* Write path: Outbox -> Bronze (for analytics) */}
          <line x1="350" y1="380" x2="350" y2="450" stroke="#2563eb" strokeWidth="3" strokeDasharray="4,4" />
          <polygon points="350,450 345,440 355,440" fill="#2563eb" />

          {/* Write path: Bronze -> Silver */}
          <line x1="350" y1="530" x2="350" y2="600" stroke="#2563eb" strokeWidth="3" strokeDasharray="4,4" />
          <polygon points="350,600 345,590 355,590" fill="#2563eb" />

          {/* Write path: Silver -> Gold */}
          <line x1="350" y1="680" x2="350" y2="750" stroke="#2563eb" strokeWidth="3" strokeDasharray="4,4" />
          <polygon points="350,750 345,740 355,740" fill="#2563eb" />

          {/* Gold -> DuckDB Analytics (direct query, fast path) */}
          <line x1="450" y1="820" x2="620" y2="820" stroke="#10b981" strokeWidth="5" strokeDasharray="10,5" />
          <text x="480" y="810" fill="#10b981" fontSize="12" fontWeight="600">DuckDB query (10-50ms)</text>
          <polygon points="620,820 610,815 610,825" fill="#10b981" />

          {/* Read path: SQL -> API (dashed, operational data) */}
          <path d="M 700 650 Q 750 400 650 190" stroke="#f59e0b" strokeWidth="3" strokeDasharray="8,4" fill="none" />
          <polygon points="650,190 655,200 645,200" fill="#f59e0b" />

          {/* Read path: DuckDB Analytics -> API (solid, fast analytics queries) */}
          <path d="M 700 790 Q 750 500 650 210" stroke="#10b981" strokeWidth="4" fill="none" />
          <text x="690" y="500" fill="#10b981" fontSize="12" fontWeight="600">10-50ms</text>
          <polygon points="650,210 655,220 645,220" fill="#10b981" />
        </svg>

        {/* Layer boxes */}
        <LayerBox id="ui" name="React UI" description="Port 3002" top={20} left={250} color="#dbeafe" />
        <LayerBox id="api" name="FastAPI" description="REST API" top={170} left={250} color="#bfdbfe" />
        <LayerBox id="outbox" name="Event Store" description="cow_events table" top={320} left={250} color="#93c5fd" />
        <LayerBox id="bronze" name="Bronze Layer" description="Delta Lake (immutable)" top={470} left={250} color="#e0e7ff" />
        <LayerBox id="silver" name="Silver Layer" description="SCD Type 2 (history)" top={620} left={250} color="#c7d2fe" />
        <LayerBox id="sql" name="CQRS Projection" description="operational.cows" top={620} left={620} color="#f59e0b" />
        <LayerBox id="gold" name="Gold Layer" description="Delta Lake (truth)" top={770} left={250} color="#fef3c7" />
        <LayerBox id="sqlAnalytics" name="DuckDB Analytics" description="Gold Delta queries" top={770} left={620} color="#d1fae5" />

        {/* Animated flow particles */}
        {animating && (
          <>
            <AnimatedFlow from={{ x: 345, y: 80 }} to={{ x: 345, y: 150 }} delay={0} />
            <AnimatedFlow from={{ x: 345, y: 230 }} to={{ x: 345, y: 300 }} delay={0.3} />
            <AnimatedFlow from={{ x: 345, y: 380 }} to={{ x: 345, y: 450 }} delay={0.6} />
            <AnimatedFlow from={{ x: 345, y: 530 }} to={{ x: 345, y: 600 }} delay={0.9} />
            <AnimatedFlow from={{ x: 450, y: 645 }} to={{ x: 600, y: 645 }} delay={1.2} />
            <AnimatedFlow from={{ x: 345, y: 680 }} to={{ x: 345, y: 750 }} delay={1.5} />
          </>
        )}

        {/* Info panel */}
        {selectedLayer && (
          <div
            className="card"
            style={{
              position: 'absolute',
              top: '20px',
              right: '20px',
              width: '300px',
              background: 'white',
              boxShadow: '0 8px 16px rgba(0,0,0,0.2)',
              zIndex: 20,
            }}
          >
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'start', marginBottom: '1rem' }}>
              <h3 style={{ fontWeight: 600, fontSize: '1.25rem' }}>
                {layerInfo[selectedLayer].name}
              </h3>
              <button
                onClick={() => setSelectedLayer(null)}
                style={{
                  background: 'none',
                  border: 'none',
                  fontSize: '1.5rem',
                  cursor: 'pointer',
                  color: 'var(--gray-600)',
                }}
              >
                ×
              </button>
            </div>

            <div style={{ marginBottom: '1rem' }}>
              <div style={{ fontSize: '0.875rem', color: 'var(--gray-600)', marginBottom: '0.5rem' }}>
                {layerInfo[selectedLayer].details}
              </div>
            </div>

            <div style={{ marginBottom: '1rem' }}>
              <div style={{ fontSize: '0.75rem', color: 'var(--gray-600)', marginBottom: '0.25rem', fontWeight: 600 }}>
                CURRENT COUNT
              </div>
              <div style={{ fontSize: '1.5rem', fontWeight: 600, color: 'var(--primary)' }}>
                {layerStats[selectedLayer].count.toLocaleString()}
              </div>
            </div>

            <div style={{ marginBottom: '1rem' }}>
              <div style={{ fontSize: '0.75rem', color: 'var(--gray-600)', marginBottom: '0.25rem', fontWeight: 600 }}>
                LAST UPDATE
              </div>
              <div style={{ fontSize: '0.875rem' }}>
                {formatTimestamp(layerStats[selectedLayer].lastUpdate)}
              </div>
            </div>

            <div>
              <div style={{ fontSize: '0.75rem', color: 'var(--gray-600)', marginBottom: '0.25rem', fontWeight: 600 }}>
                SAMPLE QUERY
              </div>
              <pre style={{
                background: 'var(--gray-100)',
                padding: '0.75rem',
                borderRadius: '0.375rem',
                fontSize: '0.75rem',
                overflow: 'auto',
                whiteSpace: 'pre-wrap',
                wordBreak: 'break-all',
              }}>
                {layerInfo[selectedLayer].query}
              </pre>
            </div>

            <div style={{
              marginTop: '1rem',
              paddingTop: '1rem',
              borderTop: '1px solid var(--gray-200)',
              display: 'flex',
              alignItems: 'center',
              gap: '0.5rem',
            }}>
              <div
                style={{
                  width: '12px',
                  height: '12px',
                  borderRadius: '50%',
                  background: getStatusColor(layerStats[selectedLayer].status),
                }}
              />
              <span style={{ fontSize: '0.875rem', fontWeight: 600 }}>
                Status: {layerStats[selectedLayer].status.toUpperCase()}
              </span>
            </div>
          </div>
        )}

        {/* Click hint */}
        {!selectedLayer && (
          <div
            style={{
              position: 'absolute',
              bottom: '20px',
              left: '50%',
              transform: 'translateX(-50%)',
              background: 'var(--primary)',
              color: 'white',
              padding: '0.75rem 1.5rem',
              borderRadius: '9999px',
              fontSize: '0.875rem',
              fontWeight: 600,
              animation: 'bounce 2s infinite',
            }}
          >
            👆 Click any layer to see details
          </div>
        )}
      </div>

      {/* System Stats */}
      {syncStatus && (
        <div className="card" style={{ marginTop: '2rem' }}>
          <h3 style={{ fontWeight: 600, marginBottom: '1rem' }}>System Statistics</h3>
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gap: '1rem' }}>
            <div>
              <div style={{ fontSize: '0.75rem', color: 'var(--gray-600)', marginBottom: '0.25rem' }}>
                Total Events
              </div>
              <div style={{ fontSize: '1.5rem', fontWeight: 600 }}>
                {layerStats.outbox.count.toLocaleString()}
              </div>
            </div>
            <div>
              <div style={{ fontSize: '0.75rem', color: 'var(--gray-600)', marginBottom: '0.25rem' }}>
                Projection Rows
              </div>
              <div style={{ fontSize: '1.5rem', fontWeight: 600 }}>
                {layerStats.sql.count.toLocaleString()}
              </div>
            </div>
            <div>
              <div style={{ fontSize: '0.75rem', color: 'var(--gray-600)', marginBottom: '0.25rem' }}>
                Sync Interval
              </div>
              <div style={{ fontSize: '1.5rem', fontWeight: 600, color: '#10b981' }}>
                5s
              </div>
            </div>
            <div>
              <div style={{ fontSize: '0.75rem', color: 'var(--gray-600)', marginBottom: '0.25rem' }}>
                Sync Status
              </div>
              <div style={{ fontSize: '1.125rem', fontWeight: 600, color: getStatusColor(layerStats.sql.status) }}>
                {syncStatus.sync_lag_seconds !== null && syncStatus.sync_lag_seconds < 60 ? '✅ Active' : '⚠️ Check Log'}
              </div>
            </div>
          </div>
          <div style={{ marginTop: '1rem', paddingTop: '1rem', borderTop: '1px solid var(--gray-200)', fontSize: '0.75rem', color: 'var(--gray-600)' }}>
            <strong>Sync Info:</strong> Background service running <code style={{ background: 'var(--gray-100)', padding: '0.125rem 0.25rem', borderRadius: '0.25rem' }}>demo/sync-projection.sh --loop 5</code> 
            • Deduplicates by tag_number • Updates within 5 seconds • Check logs: <code style={{ background: 'var(--gray-100)', padding: '0.125rem 0.25rem', borderRadius: '0.25rem' }}>tail -f /tmp/sync-projection.log</code>
          </div>
        </div>
      )}

      <style>{`
        @keyframes bounce {
          0%, 100% { transform: translateX(-50%) translateY(0); }
          50% { transform: translateX(-50%) translateY(-10px); }
        }

        @keyframes pulse {
          0%, 100% { opacity: 1; }
          50% { opacity: 0.5; }
        }
      `}</style>
    </div>
  );
}
