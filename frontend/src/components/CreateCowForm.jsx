import React, { useState } from 'react';
import { useNavigate, Link } from 'react-router-dom';
import { cowApi } from '../services/api';
import SyncStatusBar from './SyncStatusBar';

export default function CreateCowForm() {
  const navigate = useNavigate();
  
  const [formData, setFormData] = useState({
    tag_number: '',
    name: '',
    breed: '',
    birth_date: '',
    sex: 'female',
  });
  const [creating, setCreating] = useState(false);
  const [error, setError] = useState(null);
  const [createdCowId, setCreatedCowId] = useState(null);

  const handleChange = (e) => {
    const { name, value } = e.target;
    setFormData(prev => ({
      ...prev,
      [name]: value,
    }));
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    
    if (!formData.tag_number || !formData.breed || !formData.birth_date) {
      setError('Please fill in all required fields');
      return;
    }

    setCreating(true);
    setError(null);

    try {
      const response = await cowApi.createCow(formData);
      setCreatedCowId(response.cow_id);
      
      // Wait for sync (CQRS projection runs every 5 seconds)
      setTimeout(() => {
        navigate(`/cow/${response.cow_id}`);
      }, 6000);
      
    } catch (err) {
      setError(err.message);
      setCreating(false);
    }
  };

  if (creating) {
    return (
      <div className="container">
        <SyncStatusBar />
        
        <div className="card" style={{ maxWidth: '600px', margin: '0 auto', textAlign: 'center', padding: '3rem' }}>
          <div className="spinner" style={{ margin: '0 auto 2rem' }}></div>
          
          <h2 style={{ marginBottom: '1rem' }}>Creating Cow...</h2>
          
          <div style={{ color: 'var(--gray-600)', marginBottom: '2rem' }}>
            {createdCowId ? (
              <>
                <p>✅ Cow created successfully!</p>
                <p style={{ marginTop: '0.5rem', fontSize: '0.875rem' }}>
                  Event ID: <code style={{ 
                    background: 'var(--gray-100)', 
                    padding: '0.25rem 0.5rem',
                    borderRadius: '0.25rem',
                    fontFamily: 'monospace'
                  }}>
                    {createdCowId.substring(0, 16)}...
                  </code>
                </p>
                <p style={{ marginTop: '1rem' }}>
                  Waiting for sync to complete...
                </p>
              </>
            ) : (
              <p>Submitting event to API...</p>
            )}
          </div>

          <div className="alert alert-info" style={{ textAlign: 'left' }}>
            <strong>📊 Behind the Scenes:</strong>
            <ol style={{ marginTop: '0.5rem', marginLeft: '1.5rem', fontSize: '0.875rem' }}>
              <li>Event stored in SQL Server</li>
              <li>Published to Bronze layer (Delta Lake)</li>
              <li>Processed in Silver layer (SCD Type 2)</li>
              <li>Synced to SQL projection (~30s)</li>
              <li>Available for querying</li>
            </ol>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="container">
      <SyncStatusBar />

      <div style={{ maxWidth: '600px', margin: '0 auto' }}>
        <div style={{ marginBottom: '1.5rem' }}>
          <Link to="/" className="btn btn-secondary">← Back to List</Link>
        </div>

        <div className="card">
          <h2 className="card-title">🐄 Add New Cow</h2>

          {error && (
            <div className="alert alert-danger" style={{ marginTop: '1rem' }}>
              ⚠️ {error}
            </div>
          )}

          <form onSubmit={handleSubmit} style={{ marginTop: '1.5rem' }}>
            <div className="form-group">
              <label className="form-label">
                Tag Number <span style={{ color: 'var(--danger)' }}>*</span>
              </label>
              <input
                type="text"
                name="tag_number"
                className="form-input"
                value={formData.tag_number}
                onChange={handleChange}
                placeholder="e.g., COW-2026-001"
                required
              />
            </div>

            <div className="form-group">
              <label className="form-label">
                Name (Optional)
              </label>
              <input
                type="text"
                name="name"
                className="form-input"
                value={formData.name}
                onChange={handleChange}
                placeholder="e.g., Bessie"
              />
            </div>

            <div className="form-group">
              <label className="form-label">
                Breed <span style={{ color: 'var(--danger)' }}>*</span>
              </label>
              <input
                type="text"
                name="breed"
                className="form-input"
                value={formData.breed}
                onChange={handleChange}
                placeholder="e.g., Holstein, Angus, Hereford"
                required
              />
            </div>

            <div className="form-group">
              <label className="form-label">
                Birth Date <span style={{ color: 'var(--danger)' }}>*</span>
              </label>
              <input
                type="date"
                name="birth_date"
                className="form-input"
                value={formData.birth_date}
                onChange={handleChange}
                min={new Date(Date.now() - 30 * 365 * 24 * 60 * 60 * 1000).toISOString().split('T')[0]}
                max={new Date().toISOString().split('T')[0]}
                required
              />
              <small style={{ color: 'var(--gray-600)', fontSize: '0.75rem' }}>
                Must be within the last 30 years
              </small>
            </div>

            <div className="form-group">
              <label className="form-label">
                Sex <span style={{ color: 'var(--danger)' }}>*</span>
              </label>
              <select
                name="sex"
                className="form-select"
                value={formData.sex}
                onChange={handleChange}
                required
              >
                <option value="female">Female</option>
                <option value="male">Male</option>
              </select>
            </div>

            <div className="alert alert-info">
              <strong>ℹ️ What happens next:</strong>
              <ul style={{ marginTop: '0.5rem', marginLeft: '1.5rem', fontSize: '0.875rem' }}>
                <li>A <code>cow_created</code> event will be generated</li>
                <li>Event flows through Bronze → Silver → SQL layers</li>
                <li>Sync happens every 30 seconds</li>
                <li>You'll be redirected when the cow is available</li>
              </ul>
            </div>

            <div style={{ display: 'flex', gap: '1rem', marginTop: '1.5rem' }}>
              <button type="submit" className="btn btn-primary" style={{ flex: 1 }}>
                ✅ Create Cow
              </button>
              <Link to="/" className="btn btn-secondary" style={{ flex: 1 }}>
                Cancel
              </Link>
            </div>
          </form>
        </div>

        <div className="card" style={{ marginTop: '1.5rem' }}>
          <h3 style={{ fontSize: '1rem', fontWeight: 600, marginBottom: '0.75rem' }}>
            📚 Example Breeds
          </h3>
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: '0.5rem' }}>
            {['Holstein', 'Angus', 'Hereford', 'Simmental', 'Charolais', 'Jersey'].map(breed => (
              <button
                key={breed}
                type="button"
                className="badge badge-info"
                onClick={() => setFormData(prev => ({ ...prev, breed }))}
                style={{ cursor: 'pointer', fontSize: '0.875rem', padding: '0.5rem 1rem' }}
              >
                {breed}
              </button>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}
