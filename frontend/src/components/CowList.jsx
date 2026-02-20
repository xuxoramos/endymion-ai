import React, { useState, useEffect } from 'react';
import { Link } from 'react-router-dom';
import { cowApi, polling } from '../services/api';
import SyncStatusBar from './SyncStatusBar';

export default function CowList() {
  const [cows, setCows] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    const fetchCows = async () => {
      try {
        const data = await cowApi.getCows();
        setCows(data);
        setLoading(false);
      } catch (err) {
        setError(err.message);
        setLoading(false);
      }
    };

    // Start polling
    polling.startCowsPolling(fetchCows);

    return () => {
      polling.stopCowsPolling();
    };
  }, []);

  const getSyncBadge = (cow) => {
    // In a real implementation, we'd check if cow has pending events
    // For now, assume all are synced
    return <span className="badge badge-success">Synced</span>;
  };

  if (loading) {
    return (
      <div className="container">
        <div className="loading-container">
          <div className="spinner"></div>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="container">
        <div className="alert alert-danger">
          ⚠️ Error loading cows: {error}
        </div>
      </div>
    );
  }

  return (
    <div className="container">
      <SyncStatusBar />

      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '2rem' }}>
        <h2 style={{ fontSize: '1.5rem', fontWeight: 600 }}>
          🐄 Cattle Inventory ({cows.length})
        </h2>
        <Link to="/create" className="btn btn-primary">
          + Add Cow
        </Link>
      </div>

      {cows.length === 0 ? (
        <div className="card">
          <div style={{ textAlign: 'center', padding: '3rem' }}>
            <div style={{ fontSize: '3rem', marginBottom: '1rem' }}>🐄</div>
            <h3>No cows yet</h3>
            <p style={{ color: 'var(--gray-600)', marginTop: '0.5rem' }}>
              Get started by adding your first cow
            </p>
            <Link to="/create" className="btn btn-primary" style={{ marginTop: '1rem' }}>
              Add First Cow
            </Link>
          </div>
        </div>
      ) : (
        <div className="grid grid-cols-3">
          {cows.map((cow) => (
            <Link 
              key={cow.id} 
              to={`/cow/${cow.id}`}
              style={{ textDecoration: 'none', color: 'inherit' }}
            >
              <div className="card">
                <div className="card-header">
                  <div>
                    <div style={{ fontSize: '1.125rem', fontWeight: 600, marginBottom: '0.25rem' }}>
                      🐄 {cow.name || cow.breed}
                    </div>
                    <div style={{ fontSize: '0.875rem', color: 'var(--gray-600)', marginBottom: '0.25rem' }}>
                      {cow.breed} • {cow.tag_number}
                    </div>
                    <div style={{ fontSize: '0.75rem', color: 'var(--gray-500)', fontFamily: 'monospace' }}>
                      ID: {cow.id.substring(0, 8)}...
                    </div>
                  </div>
                  {getSyncBadge(cow)}
                </div>

                <div style={{ marginTop: '1rem' }}>
                  <div className="detail-row">
                    <span className="detail-label">Sex</span>
                    <span className="detail-value" style={{ textTransform: 'capitalize' }}>
                      {cow.sex === 'male' ? '♂ Male' : cow.sex === 'female' ? '♀ Female' : 'Unknown'}
                    </span>
                  </div>
                  <div className="detail-row">
                    <span className="detail-label">Age</span>
                    <span className="detail-value">{cow.age_display || 'Unknown'}</span>
                  </div>
                  <div className="detail-row">
                    <span className="detail-label">Birth Date</span>
                    <span className="detail-value">
                      {cow.birth_date ? new Date(cow.birth_date).toLocaleDateString() : 'Unknown'}
                    </span>
                  </div>
                  <div className="detail-row">
                    <span className="detail-label">Weight</span>
                    <span className="detail-value">
                      {cow.weight_kg ? `${cow.weight_kg} kg` : 'Not recorded'}
                    </span>
                  </div>
                  {cow.current_location && (
                    <div className="detail-row">
                      <span className="detail-label">Location</span>
                      <span className="detail-value">{cow.current_location}</span>
                    </div>
                  )}
                  <div className="detail-row">
                    <span className="detail-label">Status</span>
                    <span className={`badge ${cow.status === 'active' ? 'badge-success' : 'badge-danger'}`}>
                      {cow.status || 'Unknown'}
                    </span>
                  </div>
                </div>

                {/* Sync metadata */}
                <div style={{ 
                  marginTop: '1rem', 
                  paddingTop: '1rem', 
                  borderTop: '1px solid var(--gray-200)',
                  fontSize: '0.75rem',
                  color: 'var(--gray-500)'
                }}>
                  <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: '0.25rem' }}>
                    <span>Last Synced:</span>
                    <span>{cow.last_synced_at ? new Date(cow.last_synced_at).toLocaleTimeString() : 'N/A'}</span>
                  </div>
                  <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                    <span>Sync Version:</span>
                    <span>{cow.sync_version || 0}</span>
                  </div>
                </div>

                <div style={{ 
                  marginTop: '0.75rem', 
                  fontSize: '0.875rem',
                  color: 'var(--primary)',
                  textAlign: 'center',
                  fontWeight: 500
                }}>
                  Click to view full details →
                </div>
              </div>
            </Link>
          ))}
        </div>
      )}
    </div>
  );
}
