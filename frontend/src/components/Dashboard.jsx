import React, { useState, useEffect } from 'react';
import { cowApi } from '../services/api';

export default function Dashboard() {
  const [herdData, setHerdData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [error, setError] = useState(null);

  useEffect(() => {
    const fetchHerdComposition = async () => {
      try {
        // Only show loading spinner on initial load
        if (!herdData) {
          setLoading(true);
        } else {
          setRefreshing(true);
        }
        
        const data = await cowApi.getHerdComposition();
        setHerdData(data);
        setError(null);
      } catch (err) {
        setError(err.message);
      } finally {
        setLoading(false);
        setRefreshing(false);
      }
    };

    fetchHerdComposition();

    // Refresh every 3 seconds for real-time updates
    const interval = setInterval(fetchHerdComposition, 3000);

    return () => clearInterval(interval);
  }, [herdData]);

  // Only show loading spinner on initial load
  if (loading && !herdData) {
    return (
      <div className="container">
        <div className="loading-container">
          <div className="spinner"></div>
          <p style={{ marginTop: '1rem', color: 'var(--gray-600)' }}>
            Loading herd analytics from Gold Delta Lake...
          </p>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="container">
        <div className="alert alert-danger">
          ⚠️ Error loading herd composition: {error}
        </div>
      </div>
    );
  }

  if (!herdData || !herdData.data) {
    return (
      <div className="container">
        <div className="card">
          <div style={{ textAlign: 'center', padding: '3rem' }}>
            <div style={{ fontSize: '3rem', marginBottom: '1rem' }}>📊</div>
            <h3>No Analytics Available</h3>
            <p style={{ color: 'var(--gray-600)', marginTop: '0.5rem' }}>
              Herd composition data is being computed. This appears immediately after the analytics pipeline processes your first cow.
            </p>
          </div>
        </div>
      </div>
    );
  }

  const { data, as_of, cached } = herdData;
  const { snapshot_date, total_cows, by_breed, by_status, by_sex } = data;

  return (
    <div className="container">
      {/* Subtle refresh indicator */}
      {refreshing && (
        <div style={{
          position: 'fixed',
          top: '20px',
          right: '20px',
          background: 'linear-gradient(135deg, #667eea 0%, #764ba2 100%)',
          color: 'white',
          padding: '0.75rem 1.25rem',
          borderRadius: '0.5rem',
          boxShadow: '0 4px 6px rgba(0, 0, 0, 0.1)',
          display: 'flex',
          alignItems: 'center',
          gap: '0.5rem',
          fontSize: '0.875rem',
          fontWeight: 500,
          zIndex: 1000,
          animation: 'fadeIn 0.2s ease-in'
        }}>
          <div className="spinner" style={{ width: '16px', height: '16px', borderWidth: '2px' }}></div>
          Refreshing data...
        </div>
      )}

      <div style={{ marginBottom: '2rem' }}>
        <h2 style={{ fontSize: '1.875rem', fontWeight: 600, marginBottom: '0.5rem' }}>
          📊 Herd Analytics Dashboard
        </h2>
        <div style={{ display: 'flex', gap: '1rem', fontSize: '0.875rem', color: 'var(--gray-600)' }}>
          <span>📅 Snapshot: {snapshot_date}</span>
          <span>🕐 Updated: {new Date(as_of).toLocaleString()}</span>
          {cached && <span className="badge badge-info">Cached</span>}
        </div>
      </div>

      {/* Total Cows Summary */}
      <div className="card" style={{ marginBottom: '2rem', background: 'linear-gradient(135deg, #667eea 0%, #764ba2 100%)', color: 'white' }}>
        <div style={{ textAlign: 'center', padding: '2rem' }}>
          <div style={{ fontSize: '3.5rem', fontWeight: 700, marginBottom: '0.5rem' }}>
            {total_cows}
          </div>
          <div style={{ fontSize: '1.25rem', opacity: 0.9 }}>
            Total Cattle in Herd
          </div>
        </div>
      </div>

      <div className="grid grid-cols-3" style={{ marginBottom: '2rem' }}>
        {/* By Breed */}
        <div className="card">
          <div className="card-header">
            <h3>🐮 By Breed</h3>
          </div>
          <div style={{ padding: '1rem' }}>
            {by_breed && by_breed.length > 0 ? (
              <div style={{ display: 'flex', flexDirection: 'column', gap: '0.75rem' }}>
                {by_breed.map((item, idx) => (
                  <div key={idx} style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                    <div>
                      <div style={{ fontWeight: 600, fontSize: '1rem' }}>{item.dimension}</div>
                      <div style={{ fontSize: '0.75rem', color: 'var(--gray-600)' }}>
                        {item.avg_weight_kg ? `Avg: ${item.avg_weight_kg.toFixed(1)} kg` : ''}
                      </div>
                    </div>
                    <div style={{ textAlign: 'right' }}>
                      <div style={{ fontSize: '1.5rem', fontWeight: 700, color: 'var(--primary)' }}>
                        {item.count}
                      </div>
                      <div style={{ fontSize: '0.75rem', color: 'var(--gray-600)' }}>
                        {item.percentage.toFixed(1)}%
                      </div>
                    </div>
                  </div>
                ))}
              </div>
            ) : (
              <p style={{ textAlign: 'center', color: 'var(--gray-500)', padding: '2rem' }}>No data</p>
            )}
          </div>
        </div>

        {/* By Status */}
        <div className="card">
          <div className="card-header">
            <h3>📍 By Status</h3>
          </div>
          <div style={{ padding: '1rem' }}>
            {by_status && by_status.length > 0 ? (
              <div style={{ display: 'flex', flexDirection: 'column', gap: '0.75rem' }}>
                {by_status.map((item, idx) => (
                  <div key={idx} style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                    <div>
                      <div style={{ fontWeight: 600, fontSize: '1rem' }}>
                        {item.dimension === 'active' ? '✅ Active' : '❌ Inactive'}
                      </div>
                    </div>
                    <div style={{ textAlign: 'right' }}>
                      <div style={{ fontSize: '1.5rem', fontWeight: 700, color: item.dimension === 'active' ? 'var(--success)' : 'var(--gray-400)' }}>
                        {item.count}
                      </div>
                      <div style={{ fontSize: '0.75rem', color: 'var(--gray-600)' }}>
                        {item.percentage.toFixed(1)}%
                      </div>
                    </div>
                  </div>
                ))}
              </div>
            ) : (
              <p style={{ textAlign: 'center', color: 'var(--gray-500)', padding: '2rem' }}>No data</p>
            )}
          </div>
        </div>

        {/* By Sex */}
        <div className="card">
          <div className="card-header">
            <h3>⚥ By Sex</h3>
          </div>
          <div style={{ padding: '1rem' }}>
            {by_sex && by_sex.length > 0 ? (
              <div style={{ display: 'flex', flexDirection: 'column', gap: '0.75rem' }}>
                {by_sex.map((item, idx) => (
                  <div key={idx} style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                    <div>
                      <div style={{ fontWeight: 600, fontSize: '1rem' }}>
                        {item.dimension === 'male' ? '♂️ Male' : item.dimension === 'female' ? '♀️ Female' : '❓ Unknown'}
                      </div>
                    </div>
                    <div style={{ textAlign: 'right' }}>
                      <div style={{ fontSize: '1.5rem', fontWeight: 700, color: 'var(--primary)' }}>
                        {item.count}
                      </div>
                      <div style={{ fontSize: '0.75rem', color: 'var(--gray-600)' }}>
                        {item.percentage.toFixed(1)}%
                      </div>
                    </div>
                  </div>
                ))}
              </div>
            ) : (
              <p style={{ textAlign: 'center', color: 'var(--gray-500)', padding: '2rem' }}>No data</p>
            )}
          </div>
        </div>
      </div>

      {/* Data Source Info */}
      <div className="alert alert-info">
        <strong>📊 Data Source:</strong> Gold Delta Lake (via DuckDB)
        <br />
        <small>Queries canonical Gold Delta tables directly. Gold layer updates every 30 seconds via the analytics pipeline.</small>
      </div>
    </div>
  );
}
