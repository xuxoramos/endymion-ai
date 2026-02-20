import React, { useState, useEffect } from 'react';
import { useParams, useNavigate, Link } from 'react-router-dom';
import { cowApi, eventsApi, polling } from '../services/api';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts';
import SyncStatusBar from './SyncStatusBar';

export default function CowCard() {
  const { id } = useParams();
  const navigate = useNavigate();
  
  const [cow, setCow] = useState(null);
  const [analytics, setAnalytics] = useState(null);
  const [events, setEvents] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [editing, setEditing] = useState(false);
  const [newBreed, setNewBreed] = useState('');
  const [actionPending, setActionPending] = useState(false);
  
  // Weight recording state
  const [recordingWeight, setRecordingWeight] = useState(false);
  const [newWeight, setNewWeight] = useState('');
  const [weightNotes, setWeightNotes] = useState('');

  useEffect(() => {
    const fetchCowData = async () => {
      try {
        const [cowData, analyticsData, eventsData] = await Promise.all([
          cowApi.getCow(id),
          cowApi.getCowAnalytics(id),
          eventsApi.getCowEvents(id),
        ]);
        
        setCow(cowData);
        setAnalytics(analyticsData);
        setEvents(eventsData);
        setNewBreed(cowData.breed);
        setLoading(false);
      } catch (err) {
        setError(err.message);
        setLoading(false);
      }
    };

    // Start polling
    polling.startCowPolling(id, fetchCowData);

    return () => {
      polling.stopCowPolling(id);
    };
  }, [id]);

  const handleUpdateBreed = async () => {
    if (!newBreed || newBreed === cow.breed) {
      setEditing(false);
      return;
    }

    setActionPending(true);
    try {
      await cowApi.updateCowBreed(id, newBreed);
      setEditing(false);
      // Cow data will be updated via polling
      setTimeout(() => setActionPending(false), 2000);
    } catch (err) {
      alert('Failed to update breed: ' + err.message);
      setActionPending(false);
    }
  };

  const handleDeactivate = async () => {
    if (!confirm('Are you sure you want to mark this cow as sold?')) {
      return;
    }

    setActionPending(true);
    try {
      await cowApi.deactivateCow(id);
      alert('Cow marked as sold. Redirecting...');
      setTimeout(() => navigate('/'), 2000);
    } catch (err) {
      alert('Failed to deactivate cow: ' + err.message);
      setActionPending(false);
    }
  };

  const handleRecordWeight = async () => {
    const weight = parseFloat(newWeight);
    
    if (!weight || weight <= 0 || weight > 2000) {
      alert('Please enter a valid weight between 1 and 2000 kg');
      return;
    }

    setActionPending(true);
    try {
      await cowApi.recordWeight(id, {
        weight_kg: weight,
        notes: weightNotes || null,
      });
      
      setRecordingWeight(false);
      setNewWeight('');
      setWeightNotes('');
      
      // Show success message
      alert(`✅ Weight recorded: ${weight} kg\n\nThis will flow through:\n→ Events (SQL)\n→ Bronze layer (30s)\n→ Silver layer (30s)\n→ Gold Delta (30s)\n\nWeight chart queries Gold directly via DuckDB.`);
      
      // Data will be updated via polling
      setTimeout(() => setActionPending(false), 2000);
    } catch (err) {
      alert('Failed to record weight: ' + err.message);
      setActionPending(false);
    }
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

  if (error || !cow) {
    return (
      <div className="container">
        <div className="alert alert-danger">
          ⚠️ Error loading cow: {error || 'Cow not found'}
        </div>
        <Link to="/" className="btn btn-secondary">← Back to List</Link>
      </div>
    );
  }

  // Prepare chart data
  const chartData = analytics?.weight_trend || [];
  // Check if cow data is stale (meaning it needs sync)
  const hasPendingEvents = cow?.is_stale || false;

  return (
    <div className="container">
      <SyncStatusBar />

      <div style={{ marginBottom: '1.5rem' }}>
        <Link to="/" className="btn btn-secondary">← Back to List</Link>
      </div>

      {hasPendingEvents && (
        <div className="alert alert-warning">
          ⚠️ This cow has pending events. Changes may not be fully synced yet.
        </div>
      )}

      {actionPending && (
        <div className="alert alert-info">
          ⏳ Action in progress. Waiting for sync...
        </div>
      )}

      <div className="cow-card-layout">
        {/* Left Side: Cow Details */}
        <div className="card">
          <div className="card-header">
            <div>
              <h2 className="card-title">🐄 {cow.name || cow.breed}</h2>
              <div style={{ fontSize: '0.875rem', color: 'var(--gray-600)', marginTop: '0.25rem' }}>
                {cow.breed} • Tag: {cow.tag_number}
              </div>
              <div style={{ fontSize: '0.75rem', color: 'var(--gray-500)', fontFamily: 'monospace', marginTop: '0.25rem' }}>
                ID: {cow.id}
              </div>
            </div>
            <span className={`badge ${cow.status === 'active' ? 'badge-success' : 'badge-danger'}`}>
              {cow.status === 'active' ? 'Active' : 'Inactive'}
            </span>
          </div>

          <div style={{ marginTop: '1.5rem' }}>
            <div className="detail-row">
              <span className="detail-label">Name</span>
              <span className="detail-value">{cow.name || 'Not set'}</span>
            </div>

            <div className="detail-row">
              <span className="detail-label">Tag Number</span>
              <span className="detail-value">{cow.tag_number}</span>
            </div>

            <div className="detail-row">
              <span className="detail-label">Breed</span>
              {editing ? (
                <input
                  type="text"
                  value={newBreed}
                  onChange={(e) => setNewBreed(e.target.value)}
                  className="form-input"
                  style={{ maxWidth: '200px' }}
                  disabled={actionPending}
                />
              ) : (
                <span className="detail-value">{cow.breed}</span>
              )}
            </div>

            <div className="detail-row">
              <span className="detail-label">Sex</span>
              <span className="detail-value" style={{ textTransform: 'capitalize' }}>
                {cow.sex === 'male' ? '♂ Male' : cow.sex === 'female' ? '♀ Female' : 'Unknown'}
              </span>
            </div>

            <div className="detail-row">
              <span className="detail-label">Birth Date</span>
              <span className="detail-value">
                {cow.birth_date ? new Date(cow.birth_date).toLocaleDateString() : 'Unknown'}
              </span>
            </div>

            <div className="detail-row">
              <span className="detail-label">Age</span>
              <span className="detail-value">{cow.age_display || 'Unknown'}</span>
            </div>

            <div className="detail-row">
              <span className="detail-label">Weight</span>
              <span className="detail-value">
                {cow.weight_kg ? `${cow.weight_kg} kg` : 'Not recorded'}
              </span>
            </div>

            {/* Weight Recording Form */}
            {recordingWeight && (
              <div style={{
                marginTop: '1rem',
                padding: '1rem',
                background: 'var(--blue-50)',
                borderRadius: '0.5rem',
                border: '2px solid var(--primary)',
              }}>
                <div style={{ fontWeight: 600, marginBottom: '0.75rem', color: 'var(--primary)' }}>
                  📊 Record New Weight
                </div>
                <div style={{ marginBottom: '0.75rem' }}>
                  <label style={{ display: 'block', fontSize: '0.875rem', marginBottom: '0.25rem' }}>
                    Weight (kg) <span style={{ color: 'var(--red-600)' }}>*</span>
                  </label>
                  <input
                    type="number"
                    min="1"
                    max="2000"
                    step="0.1"
                    value={newWeight}
                    onChange={(e) => setNewWeight(e.target.value)}
                    placeholder="e.g., 675.5"
                    style={{
                      width: '100%',
                      padding: '0.5rem',
                      border: '1px solid var(--gray-300)',
                      borderRadius: '0.375rem',
                    }}
                    disabled={actionPending}
                  />
                </div>
                <div style={{ marginBottom: '0.75rem' }}>
                  <label style={{ display: 'block', fontSize: '0.875rem', marginBottom: '0.25rem' }}>
                    Notes (optional)
                  </label>
                  <textarea
                    value={weightNotes}
                    onChange={(e) => setWeightNotes(e.target.value)}
                    placeholder="e.g., Post-feeding measurement"
                    rows="2"
                    style={{
                      width: '100%',
                      padding: '0.5rem',
                      border: '1px solid var(--gray-300)',
                      borderRadius: '0.375rem',
                      resize: 'vertical',
                    }}
                    disabled={actionPending}
                  />
                </div>
                <div style={{ display: 'flex', gap: '0.5rem' }}>
                  <button
                    className="btn btn-success"
                    onClick={handleRecordWeight}
                    disabled={actionPending}
                    style={{ flex: 1 }}
                  >
                    ✅ Record Weight
                  </button>
                  <button
                    className="btn btn-secondary"
                    onClick={() => {
                      setRecordingWeight(false);
                      setNewWeight('');
                      setWeightNotes('');
                    }}
                    disabled={actionPending}
                    style={{ flex: 1 }}
                  >
                    ✖️ Cancel
                  </button>
                </div>
              </div>
            )}

            {cow.last_weight_date && (
              <div className="detail-row">
                <span className="detail-label">Last Weighed</span>
                <span className="detail-value">
                  {new Date(cow.last_weight_date).toLocaleDateString()}
                </span>
              </div>
            )}

            {cow.current_location && (
              <div className="detail-row">
                <span className="detail-label">Location</span>
                <span className="detail-value">{cow.current_location}</span>
              </div>
            )}

            {cow.dam_id && (
              <div className="detail-row">
                <span className="detail-label">Dam ID</span>
                <span className="detail-value" style={{ fontFamily: 'monospace', fontSize: '0.875rem' }}>
                  {cow.dam_id}
                </span>
              </div>
            )}

            {cow.sire_id && (
              <div className="detail-row">
                <span className="detail-label">Sire ID</span>
                <span className="detail-value" style={{ fontFamily: 'monospace', fontSize: '0.875rem' }}>
                  {cow.sire_id}
                </span>
              </div>
            )}

            {cow.notes && (
              <div className="detail-row">
                <span className="detail-label">Notes</span>
                <span className="detail-value">{cow.notes}</span>
              </div>
            )}

            <div style={{ marginTop: '1rem', paddingTop: '1rem', borderTop: '1px solid var(--gray-200)' }}>
              <div className="detail-row">
                <span className="detail-label">Created At</span>
                <span className="detail-value">
                  {new Date(cow.created_at).toLocaleString()}
                </span>
              </div>

              <div className="detail-row">
                <span className="detail-label">Last Updated</span>
                <span className="detail-value">
                  {new Date(cow.updated_at).toLocaleString()}
                </span>
              </div>

              <div className="detail-row">
                <span className="detail-label">Last Synced</span>
                <span className="detail-value">
                  {cow.last_synced_at ? new Date(cow.last_synced_at).toLocaleString() : 'Never'}
                </span>
              </div>

              <div className="detail-row">
                <span className="detail-label">Sync Version</span>
                <span className="detail-value">{cow.sync_version || 0}</span>
              </div>

              <div className="detail-row">
                <span className="detail-label">Sync Status</span>
                <span className={`badge ${!cow.is_stale ? 'badge-success' : 'badge-warning'}`}>
                  {!cow.is_stale ? 'Synced' : 'Stale'}
                </span>
              </div>
            </div>
          </div>

          {cow.status === 'active' && (
            <div className="action-bar">
              {editing ? (
                <>
                  <button 
                    className="btn btn-success" 
                    onClick={handleUpdateBreed}
                    disabled={actionPending}
                  >
                    💾 Save
                  </button>
                  <button 
                    className="btn btn-secondary" 
                    onClick={() => {
                      setEditing(false);
                      setNewBreed(cow.breed);
                    }}
                    disabled={actionPending}
                  >
                    ✖️ Cancel
                  </button>
                </>
              ) : (
                <>
                  <button 
                    className="btn btn-primary" 
                    onClick={() => setEditing(true)}
                    disabled={actionPending || hasPendingEvents || recordingWeight}
                  >
                    ✏️ Edit Breed
                  </button>
                  <button 
                    className="btn btn-success" 
                    onClick={() => setRecordingWeight(true)}
                    disabled={actionPending || hasPendingEvents || editing || recordingWeight}
                  >
                    ⚖️ Record Weight
                  </button>
                  <button 
                    className="btn btn-danger" 
                    onClick={handleDeactivate}
                    disabled={actionPending || hasPendingEvents || editing || recordingWeight}
                  >
                    🏷️ Mark as Sold
                  </button>
                </>
              )}
            </div>
          )}
        </div>

        {/* Right Side: Analytics */}
        <div>
          <div className="card" style={{ marginBottom: '1.5rem' }}>
            <h3 className="card-title">📊 Weight Trend (Last 30 Days)</h3>
            
            {chartData.length > 0 ? (
              <div style={{ marginTop: '1.5rem', height: '250px' }}>
                <ResponsiveContainer width="100%" height="100%">
                  <LineChart data={chartData}>
                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis 
                      dataKey="date" 
                      tick={{ fontSize: 12 }}
                    />
                    <YAxis 
                      label={{ value: 'Weight (kg)', angle: -90, position: 'insideLeft' }}
                    />
                    <Tooltip />
                    <Line 
                      type="monotone" 
                      dataKey="weight" 
                      stroke="#2563eb" 
                      strokeWidth={2}
                      dot={{ fill: '#2563eb', r: 4 }}
                    />
                  </LineChart>
                </ResponsiveContainer>
              </div>
            ) : (
              <div style={{ textAlign: 'center', padding: '2rem', color: 'var(--gray-600)' }}>
                📈 No weight data available yet
              </div>
            )}
          </div>

          {/* Event Log */}
          <div className="card">
            <h3 className="card-title">📜 Event History</h3>
            
            {events.length > 0 ? (
              <div className="event-log" style={{ marginTop: '1rem' }}>
                {events.map((event) => (
                  <div 
                    key={event.event_id} 
                    className="event-item synced"
                  >
                    <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: '0.25rem' }}>
                      <strong>{event.event_type}</strong>
                      <span className="badge badge-success">
                        Synced
                      </span>
                    </div>
                    <div className="event-time">
                      {new Date(event.event_timestamp || event.created_at).toLocaleString()}
                    </div>
                    <div className="event-id">
                      ID: {event.event_id.substring(0, 16)}...
                    </div>
                  </div>
                ))}
              </div>
            ) : (
              <div style={{ textAlign: 'center', padding: '2rem', color: 'var(--gray-600)' }}>
                No events recorded yet
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
